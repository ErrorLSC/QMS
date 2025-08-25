from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import MRPOrder

class ItemMRP(ItemComponentBase):
    def __init__(self, item):
        super().__init__(item.itemnum, item.warehouse)
        self.item = item  # 引用聚合根
        self.recommendation = {}
        self.algorithm = "Static"

    def set_recommendation(self, result: dict):
        self.recommendation = result

    def load(self, session):
        """从数据库加载历史推荐结果"""
        record = session.get(MRPOrder, {"ITEMNUM": self.itemnum, "Warehouse": self.warehouse,"Algorithm":self.algorithm})
        if record:
            self.recommendation = {col.name: getattr(record, col.name) for col in MRPOrder.__table__.columns}
        else:
            self.recommendation = {}

    def _extra_fields(self):
        return self.recommendation or {}

    def _to_orm(self):
        if not self.recommendation:
            return None
        return MRPOrder(**self.recommendation)
    
    def _to_orm_class(self):
        return MRPOrder
    
    def get_writer_config(self):
        return {
            "monitor_fields": [
                "RecommendedServiceLevel", "FinalSafetyStock"
            ],
            "enable_logging":False,
            "exclude_fields": ["CalcDate"],  # 避免因时间字段导致每次都更新
            "write_params": {
            "upsert": True,                  # 默认值，按主键 merge
            "delete_before_insert": False,   # ❌ 不清空整表
            "hot_zone_delete": False,
            }
        }
    
    def show_summary(self):
        print(f"[ItemMRP] ITEMNUM: {self.itemnum}, Warehouse: {self.warehouse}")

        if not self.recommendation:
            print("⚠️ 尚未运行 MRP 计算，或无推荐结果。")
            return

        rec = self.recommendation
        qty = rec.get("RecommendedQty")
        reason = rec.get("OrderReason") or "None"

        if qty is not None:
            print(f"🧾 推荐数量: {qty:.2f} | 原因: {reason}")
        else:
            print("ℹ️ 没有推荐数量字段，可能 MRP 尚未执行。")

        # 可选：打印关键上下文信息
        fields_to_show = [
            "AvailableStock", "IntransitStock", "Forecast_within_LT",
            "FinalSafetyStock", "NetRequirement", "RecommendedServiceLevel","WLEAD","Algorithm"
        ]
        for field in fields_to_show:
            if field in rec:
                print(f"  {field}: {rec[field]}")
