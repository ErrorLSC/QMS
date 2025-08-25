from datetime import datetime
from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import DemandType,DemandType_ChangeLog

class ItemDemandType(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)
        self.demand_type = None
        self.activity_level = None
        self.metrics = {}  # 可扩展指标，如 CV、TrendSlope、WeeksWithDemand 等

    def load(self, session, *_, **__):
        row = (
            session.query(DemandType)
            .filter_by(ITEMNUM=self.itemnum, Warehouse=self.warehouse)
            .first()
        )
        if row:
            self.demand_type = row.DemandType
            self.activity_level = row.ActivityLevel
            self.metrics = {
                "WeeksWithDemand": row.WeeksWithDemand,
                "ZeroRatio": row.ZeroRatio,
                "CV": row.CV,
                "TrendSlope": row.TrendSlope,
                "SeasonalStrength": row.SeasonalStrength,
            }
        self._loaded = True

    def load_from_dict(self, row: dict):
        if not row:
            return
        self.demand_type = row.get("DemandType")
        self.activity_level = row.get("ActivityLevel")
        self.metrics = {
            "WeeksWithDemand": row.get("WeeksWithDemand"),
            "ZeroRatio": row.get("ZeroRatio"),
            "CV": row.get("CV"),
            "TrendSlope": row.get("TrendSlope"),
            "SeasonalStrength": row.get("SeasonalStrength"),
        }
        self._loaded = True

    def _extra_fields(self):
        return {
            "DemandType": self.demand_type,
            "ActivityLevel": self.activity_level,
            **self.metrics,
            "ClassifyDate": datetime.now()
        }

    def _to_orm(self):
        return DemandType(
            ITEMNUM=self.itemnum,
            Warehouse=self.warehouse,
            DemandType=self.demand_type,
            ActivityLevel=self.activity_level,
            WeeksWithDemand=int(self.metrics.get("WeeksWithDemand", 0)),
            ZeroRatio=self.metrics.get("ZeroRatio"),
            CV=self.metrics.get("CV"),
            TrendSlope=self.metrics.get("TrendSlope"),
            SeasonalStrength=self.metrics.get("SeasonalStrength"),
            ClassifyDate=datetime.now(),
        )
    
    def _to_orm_class(self):
        return DemandType
    
    def get_writer_config(self):
        return {
            "monitor_fields": [
                "DemandType", "ActivityLevel", "WeeksWithDemand",
                "ZeroRatio", "CV", "TrendSlope", "SeasonalStrength"
            ],
            "enable_logging": True,
            "log_table_model": DemandType_ChangeLog,
            "change_reason": "Classify Update",
            "exclude_fields": ["ClassifyDate"],  # 避免因时间字段导致每次都更新
            "write_params": {
            "upsert": True,                  # 默认值，按主键 merge
            "delete_before_insert": False,   # ❌ 不清空整表
            "hot_zone_delete": False,
            }
        }
