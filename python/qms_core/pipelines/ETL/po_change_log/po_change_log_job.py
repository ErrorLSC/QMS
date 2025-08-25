import pandas as pd
from qms_core.core.common.base_job import BaseJobCore
from qms_core.adaptors.ETL.po_change_log.po_change_log_extractor import POChangeLogExtractor
from qms_core.adaptors.ETL.po_change_log.po_change_log_transformer import POChangeLogTransformer
from qms_core.adaptors.ETL.po_change_log.po_change_log_schema import POChangeLogParams
from qms_core.infrastructure.db.models import PO_SnapshotLog, PO_LAST_SNAPSHOT
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork
from qms_core.core.common.params.loader_params import LoaderParams
from qms_core.core.common.base_loader import BaseLoader

class POChangeLogJob(BaseJobCore):
    """
    对比当前 Open PO 与快照 + 履历，生成变更日志
    - 使用 OpenPOExtractor/Transformer 复用逻辑
    - 双写 PO_SnapshotLog 和 PO_LAST_SNAPSHOT（事务保障）
    """

    def __init__(
        self,
        config,
        dsn: str,
        extract_params: dict | None = None,
        load_params: dict | None = None
    ):
        super().__init__(config=config)
        self.dsn = dsn
        # ✅ 转为 Pydantic params（也可跳过校验）
        self.params = POChangeLogParams.model_validate(extract_params or {})
        self.warehouse_list = self.params.warehouses

        # ✅ extractor 内部仍为手动封装（适合这种多表抓取）
        self.extractor = POChangeLogExtractor(
            config=config,
            dsn=dsn,
            warehouse_list=self.params.warehouses
        )

        self.log_loader = BaseLoader(
            config=config,
            orm_class=PO_SnapshotLog,
            params=LoaderParams()
        )
        self.snapshot_loader = BaseLoader(
            config=config,
            orm_class=PO_LAST_SNAPSHOT,
            params=LoaderParams(
                skip_if_unchanged=True,
                exclude_fields=["POEntryDate", "DueDate", "AcknowledgedDeliveryDate"],
                write_params={"delete_before_insert": True}
            )
        )

    def extract(self) -> dict:
        return self.extractor.fetch_all()

    def transform(self, raw: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        - 调用底层 transformer，返回变更日志和最新快照
        - 保持 Job 层标准化接口格式
        """
        return POChangeLogTransformer(raw).transform() 
    
    @property
    def target_table(self):
        return PO_SnapshotLog

    def run(self, dry_run: bool = False, session=None) -> tuple[pd.DataFrame, pd.DataFrame]:
        raw = self.extract()
        df_log, df_snapshot = self.transform(raw) 

        if not dry_run:
            self.write_result(df_log, df_snapshot, session=session,dry_run=dry_run)

        return df_log,df_snapshot

    def write_result(
        self,
        df_log: pd.DataFrame,
        df_snapshot: pd.DataFrame,
        session=None,
        dry_run=True
    ):
        with UnitOfWork(config=self.config, session=session) as uow:
            print("📝 写入变更日志（PO_SnapshotLog）")
            self.log_loader.write(df_log, session=uow.session, dry_run=dry_run)

            print("🗃️ 写入最新快照（PO_LAST_SNAPSHOT）")
            self.snapshot_loader.write(df_snapshot, session=uow.session, dry_run=dry_run)
