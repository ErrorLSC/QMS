from qms_core.infrastructure.db.models import (
    PO_IntransitRaw, PO_LAST_SNAPSHOT, PO_DeliveryHistoryRaw,
    ItemSmartLeadtime, ItemTransportPreference, ItemBatchProfile,VendorTransportStats,VendorMaster,IWI,ItemDeliveryBehaviorStats
)
from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.core.common.base_extractor import BaseExtractor
from qms_core.core.analysis.delivery.delivery_preprocessor import DeliveryRecordPreprocessor
import pandas as pd

class ETAExtractor(BaseExtractor):
    def __init__(self, config):
        self.config = config
        self.delivery_processor: DeliveryRecordPreprocessor | None = None

    def _build_delivery_processor(self, stats_df: pd.DataFrame) -> DeliveryRecordPreprocessor:
        """
        构造统一的交付处理器（用于尾批 ETA 推理等用途）
        """
        return DeliveryRecordPreprocessor(stats_df)
    
    def fetch_delivery_behavior_stats(self):
        return fetch_orm_data(self.config, ItemDeliveryBehaviorStats)
    
    def fetch_intransit_orders(self):
        return fetch_orm_data(self.config, PO_IntransitRaw)
    
    def fetch_open_po(self):
        return fetch_orm_data(self.config,PO_LAST_SNAPSHOT)

    def fetch_delivery_history(self):
        return fetch_orm_data(self.config, PO_DeliveryHistoryRaw)

    def fetch_smart_leadtime(self):
        return fetch_orm_data(self.config, ItemSmartLeadtime)

    def fetch_transport_preference(self):
        return fetch_orm_data(self.config, ItemTransportPreference)

    def fetch_batch_profile(self):
        return fetch_orm_data(self.config, ItemBatchProfile)
    
    def fetch_vendor_transport_stat(self):
        return fetch_orm_data(self.config, VendorTransportStats)
    
    def fetch_vendor_master(self):
        return fetch_orm_data(self.config, VendorMaster)
    
    def fetch_iwi(self):
        return fetch_orm_data(self.config,IWI)

    def fetch(self) -> dict[str, pd.DataFrame]:
        df_intransit = self.fetch_intransit_orders()
        df_history = self.fetch_delivery_history()
        df_vendor_lt = self.fetch_vendor_transport_stat()

        # 👇 构建 delivery processor
        self.delivery_processor = self._build_delivery_processor(df_vendor_lt)
        df_delivery = self.delivery_processor.merge(df_history, df_intransit)

        return {
            "intransit": df_intransit,
            "history": df_history,
            "open_po": self.fetch_open_po(),
            "delivery": df_delivery,
            "smart_leadtime": self.fetch_smart_leadtime(),
            "transport_preference": self.fetch_transport_preference(),
            "batch_profile": self.fetch_batch_profile(),
            "vendor_transport_stat": df_vendor_lt,
            "vendor_master": self.fetch_vendor_master(),
            "iwi": self.fetch_iwi(),
            "delivery_behavior": self.fetch_delivery_behavior_stats(),
        }