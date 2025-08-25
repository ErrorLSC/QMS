from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import IIM, IWI

class ItemMaster(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)

        # 字段初始化
        self.item_type = None
        self.idesc = None
        self.vendor_code = None
        self.vendor_name = None
        self.cost = None
        self.rpflag = None
        self.plc = None
        self.gac = None
        self.pgc = None

        self.moq = None
        self.lead_time = None
        self.safety_stock = None
        self.default_location = None

    def load(self, session, *_, **__):
        if self._loaded:
            return

        iim = session.query(IIM).filter_by(ITEMNUM=self.itemnum).first()
        if iim:
            self.item_type = iim.IITYP
            self.idesc = iim.IDESC
            self.vendor_code = iim.IVEND
            self.vendor_name = iim.VNDNAM
            self.cost = iim.ISCST
            self.plc = iim.CXPPLC
            self.pgc = iim.PGC
            self.gac = iim.GAC
            self.rpflag = iim.RPFLAG

        iwi = session.query(IWI).filter_by(ITEMNUM=self.itemnum, Warehouse=self.warehouse).first()
        if iwi:
            self.moq = iwi.MOQ
            self.lead_time = iwi.WLEAD
            self.safety_stock = iwi.WSAFE
            self.default_location = iwi.WLOC

        self._loaded = True

    def load_from_dict(self, data_dict: dict): 
        # record = data_dict.get((self.itemnum, self.warehouse))
        # print(data_dict)
        # print(record)
        record = data_dict
        if not record:
            return

        self.item_type = record.get("item_type")
        self.idesc = record.get("idesc")
        self.vendor_code = record.get("vendor_code")
        self.vendor_name = record.get("vendor_name")
        self.cost = record.get("cost")
        self.plc = record.get("plc")
        self.pgc = record.get("pgc")
        self.gac = record.get("gac")
        self.rpflag = record.get("rpflag")
        self.lot_size = record.get("lot_size")
        self.moq = record.get("moq")
        self.lead_time = record.get("lead_time")
        self.safety_stock = record.get("safety_stock")
        self.default_location = record.get("default_location")

        self._loaded = True

    def _extra_fields(self) -> dict:
        return {
            "item_type": self.item_type,
            "idesc": self.idesc,
            "vendor_code": self.vendor_code,
            "vendor_name": self.vendor_name,
            "cost": self.cost,
            "rpflag": self.rpflag,
            "plc": self.plc,
            "gac": self.gac,
            "pgc": self.pgc,
            "moq": self.moq,
            "lead_time": self.lead_time,
            "safety_stock": self.safety_stock,
            "default_location": self.default_location,
        }
