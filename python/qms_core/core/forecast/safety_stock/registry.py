from qms_core.core.common.strategy_registry import BaseStrategyRegistry
from qms_core.core.common.params.SafetyStockParams import SafetyStockParamsSchema
from qms_core.core.forecast.safety_stock.strategies import (
    SteadySafetyStockStrategy,
    SeasonalSafetyStockStrategy,
    TrendedSafetyStockStrategy,
    IntermittentSafetyStockStrategy,
    BurstSafetyStockStrategy,
    DefaultSafetyStockStrategy,
)

class SafetyStockMethodRegistry(BaseStrategyRegistry):
    def __init__(self, params: SafetyStockParamsSchema):
        super().__init__(
            strategy_classes=[
                SteadySafetyStockStrategy,
                SeasonalSafetyStockStrategy,
                TrendedSafetyStockStrategy,
                IntermittentSafetyStockStrategy,
                BurstSafetyStockStrategy,
            ],
            param_obj=params
        )
        self.default_cls = DefaultSafetyStockStrategy

    def get_method(self, demand_type: str):
        return self.get(demand_type, default_cls=self.default_cls)
