from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.leadtime.transport_leadtime_analyzer import TransportLeadtimeAnalyzer
from qms_core.infrastructure.db.models import VendorTransportStats,PO_DeliveryHistoryRaw 
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class TransportLeadtimeJob(BaseJobCore):
    """
    运输交期分析 Job：
    - 来源：PO_DeliveryHistoryRaw
    - 辅助：VendorTransportStats（可为空）
    - 输出：VendorTransportStats（更新/重写）
    """

    ANALYZER_CLASS = TransportLeadtimeAnalyzer
    TARGET_TABLE = VendorTransportStats

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

        if df_stats.empty:
            print("⚠️ VendorTransportStats 表为空，将使用 fallback 策略进行修正。")

        return df_raw, df_stats

    def transform(self, data: tuple[pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        df_raw, df_stats = data
        df_corrected, df_aggregated = self.ANALYZER_CLASS().analyze(df=df_raw, stats_df=df_stats)
        return df_aggregated
    
    def target_table(self):
        return self.TARGET_TABLE