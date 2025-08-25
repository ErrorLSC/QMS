from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.transport.transport_preference_analyzer import TransportPreferenceAnalyzer
from qms_core.infrastructure.db.models import (
    ItemTransportPreference,
    PO_DeliveryHistoryRaw,
    VendorTransportStats
)
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class TransportPreferenceJob(BaseJobCore):
    """
    运输方式偏好分析 Job：
    - 来源：PO_DeliveryHistoryRaw
    - 修正辅助：VendorTransportStats
    - 输出：ItemTransportPreference
    """

    ANALYZER_CLASS = TransportPreferenceAnalyzer
    TARGET_TABLE = ItemTransportPreference

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        df_raw = fetch_orm_data(self.config, PO_DeliveryHistoryRaw)
        df_stats = fetch_orm_data(self.config, VendorTransportStats)
        return df_raw, df_stats

    def transform(self, data: tuple[pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        df_raw, df_stats = data
        return self.ANALYZER_CLASS().analyze(df_raw, stats_df=df_stats)
    
    def target_table(self):
        return self.TARGET_TABLE
