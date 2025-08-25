from qms_core.core.common.strategy_registry import BaseStrategyRegistry  
from qms_core.core.common.params.ForecastParams import ForecastParamsSchema
from qms_core.core.forecast.demand.strategies import (
    SteadyForecaster,
    SeasonalForecaster,
    IntermittentForecaster,
    TrendedForecaster,
    BurstForecaster,
    DefaultForecaster,
)

class ForecastMethodRegistry(BaseStrategyRegistry):
    def __init__(self, forecast_params: ForecastParamsSchema):
        super().__init__(
            strategy_classes=[
                SteadyForecaster,
                SeasonalForecaster,
                IntermittentForecaster,
                TrendedForecaster,
                BurstForecaster,
            ],
            param_obj=forecast_params
        )
        self.default_cls = DefaultForecaster

    def get_method(self, demand_type: str):
        return self.get(demand_type, default_cls=self.default_cls)
    
    def list_methods(self) -> list[str]:
        return list(self.registry.keys())
