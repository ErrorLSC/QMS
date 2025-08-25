from qms_core.infrastructure.config import MRPConfig
from qms_core.services.warehouse_filter import get_active_warehouses
from qms_core.pipelines.common.pipeline_factory import create_config_driven_pipeline
from qms_core.pipelines.common.registry.loader import load_job_registry_from_yaml
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork
from qms_core.pipelines.ETL.po_change_log.po_change_log_job import POChangeLogJob
from qms_core.adaptors.ETL.postprocess import run_postprocess_tasks

def main(dry_run=False):
    config = MRPConfig()
    dsn="JPNPRDF"

    # 🧩 Step 1: 注册 Job 类
    load_job_registry_from_yaml(r"C:\Users\JPEQZ\OneDrive - Epiroc\QMS\python\qms_core\config\ETL\job_registry.yaml")
    active_warehouses = get_active_warehouses(config=config,WMFAC="JPE")

    # ✅ Step 2: 定义 Job 名组 & 参数生成器
    def make_job_configs(job_names: list[str]) -> dict:
        return {
            name: {"extract_params": {"warehouses": active_warehouses}}
            for name in job_names
        }

    pipeline_configs = [
        {"namespace": "demand", "jobs": ["DemandHistory", "DemandHistoryRaw"]},
        {"namespace": "inventory", "jobs": ["ILI", "IWIAvail"]},
        {"namespace": "metadata", "jobs": ["ILM", "IWI", "IIM", "DPS", "AVM","GCC"]},
        {"namespace": "po_leadtime", "jobs": ["Domestic", "Oversea", "Intransit","FreightCharge"]},
    ]

    # ✅ Step 3: 构建各 Pipeline
    pipelines = {
        p["namespace"]: create_config_driven_pipeline(p["namespace"], p["jobs"])(
            config=config,
            job_configs=make_job_configs(p["jobs"])
        )
        for p in pipeline_configs
    }
    # ✅ Step 4: 单独的 Job
    po_change_log_job = POChangeLogJob(
        config=config,
        dsn=dsn,
        extract_params={"warehouses": active_warehouses}
    )

    # ✅ Step 5: 统一执行
    with UnitOfWork(config) as uow:
        for key in ["demand", "inventory", "metadata"]:
            pipelines[key].run_all(dry_run=dry_run, session=uow.session)

    po_change_log_job.run(dry_run=dry_run)
    pipelines["po_leadtime"].run_all(dry_run=dry_run)

    # ✅ Step 6: 后处理
    run_postprocess_tasks(config, dry_run=dry_run)

if __name__ == "__main__":
    main(dry_run=False)