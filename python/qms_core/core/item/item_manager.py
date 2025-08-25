from qms_core.core.item import Item
import pandas as pd
from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.infrastructure.db.models import DemandHistoryWeekly,IWI

class ItemManager:
    def __init__(self, itemnum_warehouse_list: list[tuple[str, str]]):
        self.item_ids = itemnum_warehouse_list
        self.items: list[Item] = [Item(it, wh) for it, wh in self.item_ids]

    @classmethod
    def from_demand_history(cls, config):
        """
        从 DEMANDHISTORY_WEEKLY 中加载有实际需求记录的 item + warehouse。
        """
        df = fetch_orm_data(config, DemandHistoryWeekly)
        if df.empty:
            return cls([])

        df = df[["ITEMNUM", "Warehouse"]].drop_duplicates()
        item_list = [Item(row["ITEMNUM"], row["Warehouse"]) for _, row in df.iterrows()]
        print(f"📦 加载有需求历史的 {len(item_list)} 个 item 实例")
        return cls([(it.itemnum, it.warehouse) for it in item_list])

    @classmethod
    def from_safety_stock(cls, config):
        """
        从 IWI 中筛选出 WSAFE > 0 的 item + warehouse。
        """
        df = fetch_orm_data(config, IWI)
        df = df[(df["WSAFE"].notna()) & (df["WSAFE"] > 0)]
        df = df[["ITEMNUM", "Warehouse"]].drop_duplicates()
        item_list = [Item(row["ITEMNUM"], row["Warehouse"]) for _, row in df.iterrows()]
        print(f"🛡️ 加载 WSAFE > 0 的 {len(item_list)} 个 item 实例")
        return cls([(it.itemnum, it.warehouse) for it in item_list])

    @classmethod
    def union(cls, *managers):
        item_ids = list(set(it for m in managers for it in m.item_ids))
        return cls(item_ids)
    
    def __repr__(self):
        return f"<ItemManager: {len(self.item_ids)} items>"
