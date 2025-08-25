from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.delivery.delivery_behavior_analyzer import DeliveryBehaviorAnalyzer
from qms_core.core.analysis.delivery.delivery_preprocessor import DeliveryRecordPreprocessor
from qms_core.infrastructure.db.models import (
    ItemDeliveryBehaviorStats,
    PO_DeliveryHistoryRaw,
    PO_IntransitRaw,
    VendorTransportStats
)
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class DeliveryBehaviorJob(BaseJobCore):
    """
    交付行为分析 Job：
    - 来源：PO_DeliveryHistoryRaw + PO_IntransitRaw（合并后分析）
    - 输出：ItemDeliveryBehaviorStats
    """

    ANALYZER_CLASS = DeliveryBehaviorAnalyzer
    TARGET_TABLE = ItemDeliveryBehaviorStats

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> pd.DataFrame:
        df_hist = fetch_orm_data(self.config, PO_DeliveryHistoryRaw)
        df_git = fetch_orm_data(self.config, PO_IntransitRaw)
        df_stats = fetch_orm_data(self.config, VendorTransportStats)

        merger = DeliveryRecordPreprocessor(stats_df=df_stats, tolerance_days=2)
        return merger.merge(df_hist, df_git)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.ANALYZER_CLASS().analyze(df, closed_only=True)
    
    def target_table(self):
        return self.TARGET_TABLE    