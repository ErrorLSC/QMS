from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from scipy.stats import norm
from sklearn.linear_model import LinearRegression
from qms_core.core.forecast.common.forecast_utils import croston_safety_stock, croston_sba_forecast
from qms_core.core.common.params.enums import DemandType

# === 抽象基类 ===

class BaseSafetyStockStrategy(ABC):
    @abstractmethod
    def calculate(
        self,
        forecast_series: pd.Series,
        service_level: float,
        lead_time_weeks: float,
        manual_ss: float = 0.0,
        demand_series: list | None = None
    ) -> dict:
        """
        返回 dict: {
            "RecommendedServiceLevel": float,
            "DynamicSafetyStock": float,
            "FinalSafetyStock": float
        }
        """
        pass

# === 各种策略类 ===

class SteadySafetyStockStrategy(BaseSafetyStockStrategy):
    demand_type = DemandType.STEADY

    @classmethod
    def from_params(cls, params): return cls()

    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        window = forecast_series[:lead_time_weeks]
        std = window.std(ddof=1) if len(window) > 1 else 0
        if std == 0 and demand_series:
            std = np.std(demand_series, ddof=1)

        Z = norm.ppf(service_level)
        dynamic_ss = round(Z * std, 2)
        return {
            "RecommendedServiceLevel": round(service_level, 4),
            "DynamicSafetyStock": dynamic_ss,
            "FinalSafetyStock": max(dynamic_ss, manual_ss or 0)
        }

class SeasonalSafetyStockStrategy(BaseSafetyStockStrategy):
    demand_type = DemandType.SEASONAL

    @classmethod
    def from_params(cls, params): return cls()

    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        window = forecast_series[:lead_time_weeks * 2]
        std = window.std(ddof=1) if len(window) > 1 else 0
        if std == 0 and demand_series:
            std = np.std(demand_series, ddof=1)

        Z = norm.ppf(service_level)
        dynamic_ss = round(Z * std, 2)
        return {
            "RecommendedServiceLevel": round(service_level, 4),
            "DynamicSafetyStock": dynamic_ss,
            "FinalSafetyStock": max(dynamic_ss, manual_ss or 0)
        }

class TrendedSafetyStockStrategy(BaseSafetyStockStrategy):
    demand_type = DemandType.TRENDED

    @classmethod
    def from_params(cls, params): return cls()

    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        y = forecast_series[:lead_time_weeks]
        X = np.arange(len(y)).reshape(-1, 1)
        if len(y) < 2:
            std = 0
        else:
            model = LinearRegression().fit(X, y)
            resid = y - model.predict(X)
            std = resid.std(ddof=1)
        if std == 0 and demand_series:
            std = np.std(demand_series, ddof=1)

        Z = norm.ppf(service_level)
        dynamic_ss = round(Z * std * np.sqrt(lead_time_weeks), 2)
        return {
            "RecommendedServiceLevel": round(service_level, 4),
            "DynamicSafetyStock": dynamic_ss,
            "FinalSafetyStock": max(dynamic_ss, manual_ss or 0)
        }

class IntermittentSafetyStockStrategy(BaseSafetyStockStrategy):
    demand_type = DemandType.INTERMITTENT

    def __init__(self, alpha=0.1):
        self.alpha = alpha

    @classmethod
    def from_params(cls, params):
        return cls(params.intermittent_alpha)

    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        if not demand_series:
            return {
                "RecommendedServiceLevel": round(service_level, 4),
                "DynamicSafetyStock": 0.0,
                "FinalSafetyStock": manual_ss or 0
            }

        result = croston_sba_forecast(demand_series, self.alpha)
        z_list = result.get("z_list", [])
        p_hat = result.get("p_hat", 1)
        dynamic_ss = croston_safety_stock(z_list, p_hat, lead_time_weeks, service_level)

        if dynamic_ss == 0 and len(demand_series) > 1:
            z_std = np.std(demand_series, ddof=1)
            Z = norm.ppf(service_level)
            dynamic_ss = round(Z * z_std * np.sqrt(lead_time_weeks / max(p_hat, 1)), 2)

        return {
            "RecommendedServiceLevel": round(service_level, 4),
            "DynamicSafetyStock": round(dynamic_ss, 2),
            "FinalSafetyStock": max(dynamic_ss, manual_ss or 0)
        }

class BurstSafetyStockStrategy(BaseSafetyStockStrategy):
    demand_type = DemandType.BURST

    @classmethod
    def from_params(cls, params): return cls()

    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        window = forecast_series[:lead_time_weeks]
        dynamic_ss = round(window.max() * 0.8, 2) if len(window) else 0
        return {
            "RecommendedServiceLevel": round(service_level, 4),
            "DynamicSafetyStock": dynamic_ss,
            "FinalSafetyStock": max(dynamic_ss, manual_ss or 0)
        }

class DefaultSafetyStockStrategy(BaseSafetyStockStrategy):
    def calculate(self, forecast_series, service_level, lead_time_weeks, manual_ss=0.0, demand_series=None):
        return {
            "RecommendedServiceLevel": None,
            "DynamicSafetyStock": 0.0,
            "FinalSafetyStock": manual_ss or 0
        }
