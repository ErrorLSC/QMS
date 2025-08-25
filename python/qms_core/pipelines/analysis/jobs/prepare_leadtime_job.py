from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.leadtime.prepare_leadtime_analyzer import PrepareLeadtimeStatsAnalyzer
from qms_core.infrastructure.db.models import PO_DeliveryHistoryRaw,ItemPrepareLTStats
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class PrepareLeadtimeJob(BaseJobCore):
    """
    准备期分析 Job：
    - 来源：PO_DeliveryHistoryRaw
    - 输出：ItemPrepareLTStats
    """

    ANALYZER_CLASS = PrepareLeadtimeStatsAnalyzer
    TARGET_TABLE = ItemPrepareLTStats

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> pd.DataFrame:
        return fetch_orm_data(self.config, PO_DeliveryHistoryRaw)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.ANALYZER_CLASS().analyze(df)
    
    def target_table(self):
        return self.TARGET_TABLE