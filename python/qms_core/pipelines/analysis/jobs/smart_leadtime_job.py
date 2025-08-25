from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.leadtime.smart_leadtime_analyzer import SmartLeadtimeAnalyzer
from qms_core.infrastructure.db.models import (
    ItemTransportPreference,
    ItemPrepareLTStats,
    VendorTransportStats,
    VendorMaster,
    ItemSmartLeadtime
)
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class SmartLeadtimeJob(BaseJobCore):
    """
    智能交期融合 Job：
    - 来源：运输偏好、准备期、运输统计、Vendor 静态交期
    - 输出：ItemSmartLeadtime
    """

    ANALYZER_CLASS = SmartLeadtimeAnalyzer
    TARGET_TABLE = ItemSmartLeadtime

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        df_pref = fetch_orm_data(self.config, ItemTransportPreference)
        df_prep = fetch_orm_data(self.config, ItemPrepareLTStats)
        df_trans = fetch_orm_data(self.config, VendorTransportStats)
        df_vendor = fetch_orm_data(self.config, VendorMaster)
        return df_pref, df_prep, df_trans, df_vendor

    def transform(self, data: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        df_pref, df_prep, df_trans, df_vendor = data
        return self.ANALYZER_CLASS().analyze(df_pref, df_prep, df_trans, df_vendor)
    
    def target_table(self):
        return self.TARGET_TABLE    