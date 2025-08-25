import pandas as pd
from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import STKOH, STKOHAvail
from collections import defaultdict

class ItemInventory(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)

        self.stock_by_location = {}

        self.total_stock_on_hand = 0
        self.available_stock = 0
        self.in_transit_stock = 0

        self.total_stock_with_replacement = 0
        self.available_stock_with_replacement = 0
        self.in_transit_stock_with_replacement = 0

    def load(self, session, *_, **__):
        if self._loaded:
            return

        # 1. 库位库存（STKOH）
        records = session.query(STKOH).filter_by(
            ITEMNUM=self.itemnum, Warehouse=self.warehouse
        ).all()
        location_qty = defaultdict(float)
        for r in records:
            location_qty[r.LOCATION] += r.QTYOH
        self.stock_by_location = dict(location_qty)
        self.total_stock_on_hand = sum(self.stock_by_location.values())

        # 2. 可用库存与在途库存（STKOHAvail）
        row = session.query(STKOHAvail).filter_by(
            ITEMNUM=self.itemnum, Warehouse=self.warehouse
        ).first()
        if row:
            self.available_stock = row.AVAIL or 0
            self.in_transit_stock = row.IONOD or 0

        self._loaded = True

    def load_from_dict(self, record: dict):
        self.available_stock = record.get("AvailableStock", 0.0)
        self.in_transit_stock = record.get("IntransitStock", 0.0)

        self.available_stock_with_replacement = self.available_stock
        self.in_transit_stock_with_replacement = self.in_transit_stock
        self.total_stock_with_replacement = self.available_stock + self.in_transit_stock

        self.total_stock_on_hand = 0
        self.stock_by_location = {}

        self._loaded = True

    def aggregate_replacements(self, replacing_map: dict, df_stkoh: pd.DataFrame):
        self.total_stock_with_replacement = self.total_stock_on_hand
        self.available_stock_with_replacement = self.available_stock
        self.in_transit_stock_with_replacement = self.in_transit_stock

        if self.itemnum not in replacing_map or df_stkoh.empty:
            return

        usable_parents = [
            rel['parent']
            for rel in replacing_map[self.itemnum]
            if rel.get('using_existing')
        ]
        if not usable_parents:
            return

        matched = df_stkoh[
            (df_stkoh['ITEMNUM'].isin(usable_parents)) &
            (df_stkoh['Warehouse'] == self.warehouse)
        ]
        if matched.empty:
            return

        avail_sum = matched['AVAIL'].fillna(0).sum()
        transit_sum = matched['IONOD'].fillna(0).sum()

        self.available_stock_with_replacement += avail_sum
        self.in_transit_stock_with_replacement += transit_sum
        self.total_stock_with_replacement += avail_sum + transit_sum

    def _extra_fields(self) -> dict:
        return {
            "total_stock_on_hand": self.total_stock_on_hand,
            "available_stock": self.available_stock,
            "in_transit_stock": self.in_transit_stock,
            "total_stock_with_replacement": self.total_stock_with_replacement,
            "available_stock_with_replacement": self.available_stock_with_replacement,
            "in_transit_stock_with_replacement": self.in_transit_stock_with_replacement,
            "stock_by_location": self.stock_by_location,
        }
