from typing import Optional
from datetime import datetime
from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import ItemSafetyRecord


class ItemSafety(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)
        self.recommended_service_level: Optional[float] = None
        self.dynamic_safety_stock: float = 0.0
        self.final_safety_stock: float = 0.0

    def set_values(self, result: dict):
        self.recommended_service_level = result.get("RecommendedServiceLevel")
        self.dynamic_safety_stock = result.get("DynamicSafetyStock", 0.0)
        self.final_safety_stock = result.get("FinalSafetyStock", 0.0)
        self._loaded = True

    def load(self, session, *_, **__):
        row = session.query(ItemSafetyRecord).filter_by(
            ITEMNUM=self.itemnum, Warehouse=self.warehouse
        ).first()
        if row:
            self.recommended_service_level = row.RecommendedServiceLevel
            self.dynamic_safety_stock = row.DynamicSafetyStock
            self.final_safety_stock = row.FinalSafetyStock
        self._loaded = True

    def load_from_dict(self, record: dict):
        self.recommended_service_level = record.get("RecommendedServiceLevel")
        self.dynamic_safety_stock = record.get("DynamicSafetyStock", 0.0)
        self.final_safety_stock = record.get("FinalSafetyStock", 0.0)
        self._loaded = True

    def _extra_fields(self):
        return {
            "RecommendedServiceLevel": self.recommended_service_level,
            "DynamicSafetyStock": self.dynamic_safety_stock,
            "FinalSafetyStock": self.final_safety_stock,
            "SafetyCalcDate": datetime.now(),
        }

    def _to_orm(self):
        return ItemSafetyRecord(
            ITEMNUM=self.itemnum,
            Warehouse=self.warehouse,
            RecommendedServiceLevel=self.recommended_service_level,
            DynamicSafetyStock=self.dynamic_safety_stock,
            FinalSafetyStock=self.final_safety_stock,
            SafetyCalcDate=datetime.now(),
        )
    
    def _to_orm_class(self):
        return ItemSafetyRecord
    
    def get_writer_config(self):
        return {
            "monitor_fields": [
                "RecommendedServiceLevel", "FinalSafetyStock"
            ],
            "enable_logging":False,
            "exclude_fields": ["SafetyCalcDate"],  # 避免因时间字段导致每次都更新
            "write_params": {
            "upsert": True,                  # 默认值，按主键 merge
            "delete_before_insert": False,   # ❌ 不清空整表
            "hot_zone_delete": False,
            }
        }
