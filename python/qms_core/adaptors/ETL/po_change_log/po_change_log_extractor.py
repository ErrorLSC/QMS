from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.infrastructure.db.models import PO_LAST_SNAPSHOT, PO_DeliveryHistoryRaw
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_extractor import OpenPOExtractor
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_schema import OpenPOParams
from qms_core.adaptors.ETL.BPCS.common.bpcs_extractor import BPCSExtractorMeta

class POChangeLogExtractor():
    def __init__(self, config,dsn, warehouse_list: list[str]):
        self.config = config
        self.dsn = dsn
        self.warehouse_list = warehouse_list

    def _fetch_openpo_snapshot(self):
        filters = []
        if self.warehouse_list:
            filters.append(PO_LAST_SNAPSHOT.Warehouse.in_(self.warehouse_list))
        return fetch_orm_data(self.config, PO_LAST_SNAPSHOT, filters)

    def _fetch_delivery_history(self):
        filters = []
        if self.warehouse_list:
            filters.append(PO_DeliveryHistoryRaw.Warehouse.in_(self.warehouse_list))
        return fetch_orm_data(self.config, PO_DeliveryHistoryRaw, filters)
    
    def _fetch_openpo_now(self):
        meta = BPCSExtractorMeta(dsn=self.dsn)
        params = OpenPOParams(warehouses=self.warehouse_list)
        open_po_extractor = OpenPOExtractor(meta=meta, params=params)
        return open_po_extractor.fetch()
    
    def fetch_all(self) -> dict:
        return {
            "df_prev": self._fetch_openpo_snapshot(),
            "df_delv": self._fetch_delivery_history(),
            "df_now_raw" : self._fetch_openpo_now()
        }