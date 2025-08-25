from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import ItemSmartLeadtime as ORM_ItemSmartLeadtime
from qms_core.infrastructure.db.models import ItemTransportPreference,IIM

class ItemSmartLeadtime(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)
        self.vendor_code = None
        self.total_days = None
        self.total_weeks = None
        self.prep_days = None
        self.transport_mode = None
        self.transport_days = None
        self.source = None


    def load_from_dict(self, record: dict):
        try:
            if not record:
                return
            self.vendor_code = record.get("VendorCode")
            self.total_days = int(record.get("Q60LeadTime"))
            self.total_weeks = int(record.get("Q60LeadTime")) // 7
            self.transport_mode = record.get("TransportMode")
            self.prep_days = record.get("Q60PrepDays")
            self.transport_days = record.get("Q60TransportLeadTime")
            self.source = record.get("Source")
            self._loaded = True
        except Exception as e:
            print(f"[ItemSmartLeadtime] 加载失败 {self.itemnum}-{self.warehouse}: {e}")

    def _extra_fields(self) -> dict:
        return {
            "VendorCode": self.vendor_code,
            "TotalDays": self.total_days,
            "TotalLeadtimeWeeks": self.total_weeks,
            "TransportMode": self.transport_mode,
            "PrepDays": self.prep_days,
            "TransportDays": self.transport_days,
            "Source": self.source,
        }

    def _to_orm(self):
        pass

    def _to_orm_class(self):
        pass
    
    def load(self, session,vendor_code=None):
        try:
            # 先自动去查 ItemMaster 表拿 vendor_code
            master_row = (
                session.query(IIM)
                .filter_by(ITEMNUM=self.itemnum)
                .first()
            )
            if not master_row:
                print(f"[ItemSmartLeadtime] 无法找到ItemMaster: {self.itemnum}-{self.warehouse}")
                return

            vendor_code = master_row.IVEND
            if not vendor_code:
                print(f"[ItemSmartLeadtime] ItemMaster缺失VendorCode: {self.itemnum}-{self.warehouse}")
                return

            selector = SmartLeadtimeSelector(session)
            row = selector.select(self.itemnum, self.warehouse, vendor_code)

            if row:
                self.vendor_code = row.VendorCode
                self.total_days = int(row.Q60LeadTime) if row.Q60LeadTime is not None else None
                self.total_weeks = int(row.Q60LeadTime) // 7 if self.total_days is not None else None
                self.transport_mode = row.TransportMode
                self.prep_days = row.Q60PrepDays
                self.transport_days = row.Q60TransportLeadTime
                self.source = row.Source
                self._loaded = True
            else:
                print(f"[ItemSmartLeadtime] 未找到SmartLeadtime记录: {self.itemnum}-{self.warehouse}")

        except Exception as e:
            print(f"[ItemSmartLeadtime] DB加载失败 {self.itemnum}-{self.warehouse}: {e}")

class SmartLeadtimeSelector:
    """
    负责根据 ItemNum, Warehouse, VendorCode 自动在 TransportPreference 和 SmartLeadtime 中挑选最优交期路径
    """
    def __init__(self, session):
        self.session = session

    def select(self, itemnum, warehouse, vendor_code):
        pref_rows = (
            self.session.query(ItemTransportPreference)
            .filter_by(ITEMNUM=itemnum, Warehouse=warehouse, VendorCode=vendor_code)
            .order_by(ItemTransportPreference.Rank.asc())
            .all()
        )

        for pref in pref_rows:
            row = (
                self.session.query(ORM_ItemSmartLeadtime)
                .filter_by(
                    ITEMNUM=itemnum,
                    Warehouse=warehouse,
                    VendorCode=vendor_code,
                    TransportMode=pref.TransportMode
                )
                .first()
            )
            if row:
                return row  # 直接返回 ORM 行

        return None  # 没找到