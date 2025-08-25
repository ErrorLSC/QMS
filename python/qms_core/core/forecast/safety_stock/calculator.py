import numpy as np
import pandas as pd
from qms_core.core.forecast.common.forecast_utils import preprocess_demand,score_service_level
from qms_core.core.common.params.ParasCenter import ParasCenter
from qms_core.core.common.params.SafetyStockParams import SafetyStockParamsSchema
from qms_core.core.common.params.enums import DemandType, ActivityLevel
from qms_core.core.forecast.safety_stock.registry import SafetyStockMethodRegistry

class SafetyStockCalculator:
    def __init__(self, params: SafetyStockParamsSchema = None):
        self.params = params or ParasCenter().safety_params
        self.registry = SafetyStockMethodRegistry(self.params)

    def preprocess_demand(self, df: pd.DataFrame, max_date=None) -> pd.DataFrame:
        return preprocess_demand(df, max_date, self.params)

    def calculate_for_item(self, item, demand_series=None, max_date=None) -> dict:
        demand_type = item.demand_type.demand_type
        activity_level = item.demand_type.activity_level

        if demand_type in [DemandType.SINGLE, DemandType.NEW] or activity_level in [ActivityLevel.DORMANT, ActivityLevel.INACTIVE]:
            result = {
                "RecommendedServiceLevel": None,
                "DynamicSafetyStock": 0.0,
                "FinalSafetyStock": item.master.safety_stock or 0.0
            }
            item.safetystock.set_values(result)
            return result

        # 自动读取原始需求
        if demand_series is None and hasattr(item, "demand") and hasattr(item.demand, "history"):
            raw_df = item.demand.history
            demand_df = self.preprocess_demand(raw_df, max_date)
            demand_series = demand_df["TotalDemand"].tolist()
        else:
            demand_df = None

        # 服务水平自动计算
        service_level = self._get_service_level(item)

        # 周交期
        lead_time_weeks = self._calc_lead_time_weeks(item)

        # 获取策略类
        strategy = self.registry.get_method(demand_type)

        result = strategy.calculate(
            forecast_series=item.forecast.forecast_series,
            service_level=service_level,
            lead_time_weeks=lead_time_weeks,
            manual_ss=item.master.safety_stock,
            demand_series=demand_series
        )

        item.safetystock.set_values(result)
        return result
    
    def calculate_with_strategy(self, item, strategy_name: str, max_date=None, demand_series=None) -> dict:
        """
        使用指定的策略（忽略 item.demand_type），手动计算安全库存。

        参数:
            item: 含 forecast、demand history 的 item 对象
            strategy_name: 字符串，如 'STEADY', 'SEASONAL', 'BURST' 等
            max_date: 截止日期
            demand_series: 可选外部传入的历史需求数据（列表）

        返回:
            dict: 推荐服务水平、动态安全库存、最终安全库存
        """
        strategy_name = strategy_name.upper()
        strategy = self.registry.get_method(strategy_name)

        if demand_series is None and hasattr(item, "demand") and hasattr(item.demand, "history"):
            raw_df = item.demand.history
            demand_df = preprocess_demand(raw_df, max_date, self.params)
            demand_series = demand_df["TotalDemand"].tolist()

        service_level = self._get_service_level(item)

        lead_time_weeks = self._calc_lead_time_weeks(item)

        result = strategy.calculate(
            forecast_series=item.forecast.forecast_series,
            service_level=service_level,
            lead_time_weeks=lead_time_weeks,
            manual_ss=item.master.safety_stock,
            demand_series=demand_series
        )
        item.safetystock.set_values(result)
        return result
    
    def _calc_lead_time_weeks(self, item) -> int:
        raw_wlead = getattr(item.master, "lead_time", 7) or 7
        return max(1, int(np.ceil(raw_wlead / 7)))
    
    def _get_service_level(self, item) -> float:
        svc = getattr(item.master, "service_level", None)

        if svc is not None:
            return svc
        return score_service_level(
            iscst=item.master.cost,
            wlead=item.master.lead_time,
            cv=item.demand_type.metrics.get("CV"),
            params=self.params.service_level
    )