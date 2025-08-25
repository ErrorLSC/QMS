import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from qms_core.core.forecast.common.forecast_utils import weighted_bootstrap_quantile, croston_sba_forecast
from qms_core.core.common.params.enums import DemandType
from abc import ABC, abstractmethod

class BaseForecaster(ABC):
    @abstractmethod
    def forecast(self, df: pd.DataFrame, lead_time_weeks: int) -> pd.Series: ...
    
    @abstractmethod
    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series: ...
    
    @property
    def name(self):
        return self.__class__.__name__.replace("Forecaster", "")


class SteadyForecaster(BaseForecaster):
    demand_type = DemandType.STEADY

    def __init__(self, n_samples=1000, quantile=0.6):
        self.n_samples = n_samples
        self.quantile = quantile

    @classmethod
    def from_params(cls, params):
        return cls(params.steady_n_samples, params.steady_quantile)

    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        demand = df["TotalDemand"]
        weight = df["Weight"]

        if demand.empty or weight.empty or weight.sum() == 0:
            return pd.Series([0] * max_weeks)

        unit = weighted_bootstrap_quantile(demand, weight, self.quantile, self.n_samples)
        return pd.Series([unit] * max_weeks)

    def forecast(self, df: pd.DataFrame, lead_time_weeks=12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)


class SeasonalForecaster(BaseForecaster):
    demand_type = DemandType.SEASONAL

    def __init__(self, baseline_window_weeks=12, fill_profile_default=1.0, debug=False):
        self.baseline_window_weeks = baseline_window_weeks
        self.fill_profile_default = fill_profile_default
        self.debug = debug

    @classmethod
    def from_params(cls, params):
        return cls(params.seasonal_baseline_window_weeks, params.seasonal_fill_profile_default, params.seasonal_debug)

    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        df = df.copy()
        df["WeekNum"] = df["YearWeek"].dt.isocalendar().week

        profile = df.groupby("WeekNum")["TotalDemand"].mean()
        profile /= profile.mean() if profile.mean() != 0 else 1.0
        profile = profile.reindex(range(1, 53), fill_value=self.fill_profile_default)

        if df["YearWeek"].dt.year.nunique() <= 1 and self.debug:
            print("⚠️ SeasonalForecaster: 仅基于一年数据构建 profile，可能不稳定")

        recent_df = df.sort_values("YearWeek").tail(self.baseline_window_weeks)
        weighted_sum = (recent_df["TotalDemand"] * recent_df["Weight"]).sum()
        weight_total = recent_df["Weight"].sum()
        baseline = weighted_sum / weight_total if weight_total > 0 else 1.0

        future_dates = pd.date_range(df["YearWeek"].max() + pd.Timedelta(weeks=1), periods=max_weeks, freq="W-MON")
        future_weeknums = future_dates.isocalendar().week.values
        shape_values = pd.Series([profile.get(w, 1.0) for w in future_weeknums])
        return (shape_values * baseline).reset_index(drop=True)

    def forecast(self, df: pd.DataFrame, lead_time_weeks=12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)


class TrendedForecaster(BaseForecaster):
    demand_type = DemandType.TRENDED

    @classmethod
    def from_params(cls, params):
        return cls()

    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        df = df.copy()
        df["Weeks"] = (df["YearWeek"] - df["YearWeek"].min()).dt.days // 7

        X = df[["Weeks"]]
        y = df["TotalDemand"]

        if len(X) < 2:
            return pd.Series([y.mean() if not y.empty else 0] * max_weeks)

        model = LinearRegression().fit(X, y)
        future_weeks = pd.DataFrame({"Weeks": [X["Weeks"].max() + i for i in range(1, max_weeks + 1)]})
        return pd.Series(model.predict(future_weeks))

    def forecast(self, df: pd.DataFrame, lead_time_weeks=12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)


class IntermittentForecaster(BaseForecaster):
    demand_type = DemandType.INTERMITTENT

    def __init__(self, alpha=0.1):
        self.alpha = alpha

    @classmethod
    def from_params(cls, params):
        return cls(params.intermittent_alpha)

    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        result = croston_sba_forecast(df["TotalDemand"].tolist(), self.alpha)
        return pd.Series([result["forecast"]] * max_weeks)

    def forecast(self, df: pd.DataFrame, lead_time_weeks=12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)


class BurstForecaster(BaseForecaster):
    demand_type = DemandType.BURST

    @classmethod
    def from_params(cls, params):
        return cls()

    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        demand = df["TotalDemand"]
        if len(demand) == 0:
            return pd.Series([0] * max_weeks)

        p90 = np.percentile(demand, 90)
        return pd.Series([p90] * max_weeks)

    def forecast(self, df: pd.DataFrame, lead_time_weeks: int = 12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)


class DefaultForecaster(BaseForecaster):
    def forecast_series(self, df: pd.DataFrame, max_weeks: int = 12) -> pd.Series:
        demand = df["TotalDemand"]
        weight = df["Weight"]

        if demand.empty or weight.empty or weight.sum() == 0:
            return pd.Series([0] * max_weeks)

        mean = (demand * weight).sum() / weight.sum()
        return pd.Series([mean] * max_weeks)

    def forecast(self, df: pd.DataFrame, lead_time_weeks: int = 12):
        return self.forecast_series(df, max_weeks=lead_time_weeks)
