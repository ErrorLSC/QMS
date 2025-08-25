from qms_core.core.forecast.safety_stock.calculator import SafetyStockCalculator
from qms_core.core.forecast.common.forecast_utils import score_service_level

class DynamicSafetyStockCalculator(SafetyStockCalculator):
    """
    使用智能交期估算的安全库存计算器。
    优先使用 item.smart_lead_time.total_days → 向上取整转周数。
    """
    def _calc_lead_time_weeks(self, item) -> int:
        # 优先使用模块字段
        if hasattr(item, "smart_leadtime") and getattr(item.smart_leadtime, "total_weeks", None):
            return max(1, item.smart_leadtime.total_weeks)

        # fallback：从动态字典（假设存在 dynamic_lt_dict）
        if hasattr(self, "dynamic_lt_dict"):
            key = (item.itemnum, item.warehouse)
            lt_weeks = self.dynamic_lt_dict.get(key)
            if lt_weeks is not None:
                return max(1, int(round(lt_weeks)))

        # 最终回退至静态逻辑
        return super()._calc_lead_time_weeks(item)

    def _get_service_level(self, item) -> float:
        svc = getattr(item.master, "service_level", None)
        if svc is not None:
            return svc

        wlead_days = getattr(item.smart_leadtime, "total_days", None)
        if wlead_days is None:
            # raise ValueError(f"[DynamicSafetyStockCalculator] 无法获取交期信息：{item.itemnum}-{item.warehouse}")
            print("[Servicelevel Calculator]:NO VALID LEADTIME DATA")

        return score_service_level(
            iscst=item.master.cost,
            wlead=wlead_days,
            cv=item.demand_type.metrics.get("CV"),
            params=self.params.service_level
        )