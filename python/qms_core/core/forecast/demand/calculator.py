from qms_core.core.common.params.ForecastParams import ForecastParamsSchema
from qms_core.core.common.params.ParasCenter import ParasCenter
from qms_core.core.common.params.enums import DemandType,ActivityLevel
from qms_core.core.forecast.common.forecast_utils import preprocess_demand
import pandas as pd
from qms_core.core.forecast.demand.registry import ForecastMethodRegistry
from qms_core.core.item.item import Item

class DemandForecaster:
    def __init__(self, params: ForecastParamsSchema = None):
        self.params = params or ParasCenter().forecast_params
        self.registry = ForecastMethodRegistry(self.params)

    def forecast_item(self, item:Item, max_date: pd.Timestamp = None):
        df = item.demand.history
        if df is None or df.empty:
            return self._set_forecast(item, [0] * self.params.forecast_horizon_weeks, "empty")

        if max_date is None:
            max_date = df["YearWeek"].max()

        df = df[df["YearWeek"] <= max_date].copy()
        demand_type = item.demand_type.demand_type
        activity = item.demand_type.activity_level

        # 冷启动判断
        if demand_type in [DemandType.SINGLE, DemandType.NEW] or activity in [ActivityLevel.DORMANT, ActivityLevel.INACTIVE]:
            return self._set_forecast(item, [0] * self.params.forecast_horizon_weeks, "cold_start")

        df = self._preprocess(df, max_date)

        forecaster = self.registry.get_method(demand_type)
        forecast_series = forecaster.forecast_series(df, max_weeks=self.params.forecast_horizon_weeks)
        return self._set_forecast(item, forecast_series, demand_type)

    def _preprocess(self, df: pd.DataFrame, max_date: pd.Timestamp) -> pd.DataFrame:
        return preprocess_demand(df, max_date, self.params)

    def _set_forecast(self, item:Item, forecast_series, model_used: str):
        if not isinstance(forecast_series, pd.Series):
            forecast_series = pd.Series(forecast_series)
        item.forecast.set_forecast_values(
            forecast_series=forecast_series,
            model_used=model_used
        )

    def forecast_with_method(self, item:Item, method: str, max_date: pd.Timestamp = None):
        """
        使用用户指定的方法进行预测，忽略 item 的 demand_type。
        method: 字符串，如 'STEADY', 'SEASONAL', 'BURST' 等（应与 DemandType Enum 保持一致）
        """
        df = item.demand.history
        if df is None or df.empty:
            return self._set_forecast(item, [0] * self.params.forecast_horizon_weeks, "empty")

        if max_date is None:
            max_date = df["YearWeek"].max()

        df = df[df["YearWeek"] <= max_date].copy()
        df = self._preprocess(df, max_date)

        method = method.upper()
        forecaster = self.registry.get_method(method)  
        forecast_series = forecaster.forecast_series(df, max_weeks=self.params.forecast_horizon_weeks)
        return self._set_forecast(item, forecast_series, method)