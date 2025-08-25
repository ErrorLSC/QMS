from qms_core.core.forecast.transaction.virtual_transaction_extractor import VirtualTransactionExtractor
from qms_core.core.forecast.transaction.virtual_transaction_transformer import VirtualTransactionTransformer
from qms_core.core.common.base_job import BaseJobCore
from qms_core.infrastructure.db.models import VirtualStockTransaction
import pandas as pd

class VirtualTransactionJob(BaseJobCore):
    """
    VirtualTransaction Job：
    - 来源：VirtualTransactionExtractor
    - 输出：VirtualStockTransaction
    """
    EXTRACTOR_CLASS = VirtualTransactionExtractor
    TRANSFORMER_CLASS = VirtualTransactionTransformer
    TARGET_TABLE = VirtualStockTransaction

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
    
    def extract(self) -> dict:
        return self.EXTRACTOR_CLASS(self.config).fetch()

    def transform(self, data_dict: dict) -> pd.DataFrame:
        return self.TRANSFORMER_CLASS().transform(data_dict=data_dict)

    def target_table(self):
        return self.TARGET_TABLE

    def run(self, dry_run: bool = True, session=None) -> pd.DataFrame:
        print(f"🚀 运行任务：{self.job_name}")
        data_dict = self.extract()
        df = self.transform(data_dict=data_dict)
        return self.load(df, dry_run=dry_run, session=session)
