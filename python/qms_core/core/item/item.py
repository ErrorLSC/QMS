from qms_core.core.item.item_master import ItemMaster
from qms_core.core.item.item_demand import ItemDemand
from qms_core.core.item.item_demand_type import ItemDemandType
from qms_core.core.item.item_forecast import ItemForecast
from qms_core.core.item.item_safetystock import ItemSafety
from qms_core.core.item.item_inventory import ItemInventory
from qms_core.core.item.item_mrp import ItemMRP
from qms_core.core.item.item_smart_leadtime import ItemSmartLeadtime


class Item:
    def __init__(self, itemnum: str, warehouse: str):
        self.itemnum = itemnum
        self.warehouse = warehouse

        # 子模块懒加载容器
        self._master = None
        self._demand = None
        self._demand_type = None
        self._forecast = None
        self._safetystock = None
        self._inventory = None
        self._mrp = None
        self._smart_leadtime = None

    @property
    def master(self) -> ItemMaster:
        if self._master is None:
            self._master = ItemMaster(self.itemnum, self.warehouse)
        return self._master

    @property
    def demand(self) -> ItemDemand:
        if self._demand is None:
            self._demand = ItemDemand(self.itemnum, self.warehouse)
        return self._demand

    @property
    def demand_type(self) -> ItemDemandType:
        if self._demand_type is None:
            self._demand_type = ItemDemandType(self.itemnum, self.warehouse)
        return self._demand_type

    @property
    def forecast(self) -> ItemForecast:
        if self._forecast is None:
            self._forecast = ItemForecast(self.itemnum, self.warehouse)
        return self._forecast

    @property
    def safetystock(self) -> ItemSafety:
        if self._safetystock is None:
            self._safetystock = ItemSafety(self.itemnum, self.warehouse)
        return self._safetystock

    @property
    def inventory(self) -> ItemInventory:
        if self._inventory is None:
            self._inventory = ItemInventory(self.itemnum, self.warehouse)
        return self._inventory

    @property
    def mrp(self) -> ItemMRP:
        if self._mrp is None:
            self._mrp = ItemMRP(self)
        return self._mrp

    @property
    def smart_leadtime(self) -> ItemSmartLeadtime:
        if self._smart_leadtime is None:
            self._smart_leadtime = ItemSmartLeadtime(self.itemnum,self.warehouse)
        return self._smart_leadtime

    def to_dict(self) -> dict:
        return {
            "itemnum": self.itemnum,
            "warehouse": self.warehouse,
            "master": self.master.to_dict(),
            "demand": self.demand.to_dict(),
            "demand_type": self.demand_type.to_dict(),
            "forecast": self.forecast.to_dict(),
            "safetystock": self.safetystock.to_dict(),
            "inventory": self.inventory.to_dict(),
            "mrp": self.mrp.to_dict(),
            "smart_leadtime": self.smart_leadtime.to_dict()
        }

    def load_all(self, session):
        """统一加载所有模块数据"""
        self.master.load(session)
        self.demand.load(session)
        self.demand_type.load(session)
        self.forecast.load(session)
        self.safetystock.load(session)
        self.inventory.load(session)
        self.mrp.load(session)
        self.smart_leadtime.load(session)

    def show_summary(self):
        print(f"🧩 Item: {self.itemnum} @ {self.warehouse}")
        for comp in [self.master, self.demand, self.demand_type,
                     self.forecast, self.safetystock, self.inventory, self.mrp,self.smart_leadtime]:
            comp.show_summary()
