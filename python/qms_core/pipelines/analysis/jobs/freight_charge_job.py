from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.transport.freight_charge_analyzer import FreightChargeAnalyzer
from qms_core.infrastructure.db.models import PO_Freight_Charge,VendorMaster,VendorTransportStats,MultiCurrency
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd

class FreightChargeJob(BaseJobCore):
    """
    运输交期分析 Job：
    - 来源：PO_Freight_Charge
    - 辅助：VendorMaster, MultiCurrency
    - 输出：VendorTransportStats（更新）
    """

    ANALYZER_CLASS = FreightChargeAnalyzer
    TARGET_TABLE = VendorTransportStats

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> dict[str, pd.DataFrame]:
        df_raw = fetch_orm_data(self.config,PO_Freight_Charge)
        df_currency = fetch_orm_data(self.config, MultiCurrency)
        df_vendor_map = fetch_orm_data(self.config, VendorMaster)

        return {"raw_data":df_raw,"currency": df_currency,"vendor_map": df_vendor_map}

    def transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        analyzer = self.ANALYZER_CLASS()
        result = analyzer.analyze(df=data["raw_data"],vendor_master_df=data["vendor_map"],currency_df=data["currency"])
        return result
    
    def target_table(self):
        return self.TARGET_TABLE
    