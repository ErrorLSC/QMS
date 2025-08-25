from qms_core.core.common.params.ClassifierParams import ClassifierParamsSchema
from qms_core.core.common.params.ParasCenter import ParasCenter
from typing import Optional
from qms_core.core.common.params.enums import ActivityLevel,DemandType
import pandas as pd
import numpy as np
from qms_core.core.forecast.common.forecast_utils import preprocess_demand
from statsmodels.tsa.seasonal import seasonal_decompose
from qms_core.core.item.item import Item

class DemandClassifier:
    def __init__(self, params: ClassifierParamsSchema = None):
        self.params = params or ParasCenter().classifier_params

    def calculate_for_item(self, item:Item, max_date: pd.Timestamp = None):
        if getattr(item.demand_type, "_loaded", False):
            return

        if item.master.rpflag == "Y":
            item.demand_type.load_from_dict({
                'DemandType': DemandType.REPLACED,
                'ActivityLevel': ActivityLevel.DORMANT,
                'WeeksWithDemand': 0,
                'ZeroRatio': 1.0,
                'CV': np.nan,
                'TrendSlope': np.nan,
                'SeasonalStrength': np.nan,
                'RecommendedServiceLevel': self.params.default_service_level_if_replaced
            })
            return
        
        if item.demand.history is None or item.demand.history.empty:
            if item.master.safety_stock and item.master.safety_stock > 0:
                item.demand_type.load_from_dict({
                    'DemandType': DemandType.STOCKONLY,
                    'ActivityLevel': ActivityLevel.DORMANT,
                    'WeeksWithDemand': 0,
                    'ZeroRatio': 1.0,
                    'CV': np.nan,
                    'TrendSlope': np.nan,
                    'SeasonalStrength': np.nan,
                })
                return
            else:
                raise ValueError(f"No demand history and no safety stock for item {item.itemnum}")

        df = self._prepare_data(item, max_date)
        if df is None:
            return  # 被替代件、无数据、stockonly 等情况

        metrics = self._compute_metrics(df)
        demand_type = self._determine_demand_type(df, metrics)
        activity_level = self._determine_activity_level(df)

        # 写入 item
        item.demand_type.demand_type = demand_type
        item.demand_type.activity_level = activity_level
        item.demand_type.metrics = metrics
        item.demand_type._loaded = True

    def _prepare_data(self, item:Item, max_date) -> Optional[pd.DataFrame]:
        df = item.demand.history.copy()

        if max_date is None:
            max_date = df["YearWeek"].max()
        df = df[df["YearWeek"] <= max_date].copy()
        df = preprocess_demand(df, max_date, self.params)
        return df

    def _compute_metrics(self, df: pd.DataFrame) -> dict:
        p = self.params
        weighted_mean = (df["TotalDemand"] * df["Weight"]).sum() / df["Weight"].sum()
        weighted_std = np.sqrt(((df["Weight"] * (df["TotalDemand"] - weighted_mean) ** 2).sum()) / df["Weight"].sum())
        cv = weighted_std / weighted_mean if weighted_mean > 0 else np.inf
        weeks_with_demand = (df["TotalDemand"] > 0).sum()
        zero_ratio = (df["TotalDemand"] == 0).sum() / len(df)

        try:
            decomposition = seasonal_decompose(df.set_index("YearWeek")["TotalDemand"], model="additive", period=self.params.seasonal_decompose_period)
            trend_slope = decomposition.trend.dropna().diff().mean()
            seasonal_strength = decomposition.seasonal.abs().mean()
        except:
            trend_slope = np.nan
            seasonal_strength = np.nan

        return {
            "WeeksWithDemand": weeks_with_demand,
            "ZeroRatio": zero_ratio,
            "CV": cv,
            "TrendSlope": trend_slope,
            "SeasonalStrength": seasonal_strength,
            "WeightedMean": weighted_mean,
            "WeightedStd": weighted_std
        }

    def _determine_demand_type(self, df: pd.DataFrame, m: dict) -> DemandType:
        p = self.params
        recent_demand = df[df["WeeksAgo"] <= 12]["TotalDemand"].sum()
        is_new = df["WeeksAgo"].max() < p.new_item_weeks

        if is_new:
            return DemandType.NEW if m["WeeksWithDemand"] <= p.new_item_weeks else DemandType.STEADY

        if m["ZeroRatio"] > p.single_zero_ratio and recent_demand <= p.single_recent_demand:
            return DemandType.SINGLE
        elif m["ZeroRatio"] > p.intermittent_zero_ratio:
            return DemandType.INTERMITTENT
        elif m["CV"] < p.steady_cv_threshold and m["WeeksWithDemand"] / len(df) > p.steady_min_weeks_ratio and m["ZeroRatio"] < p.steady_max_zero_ratio:
            return DemandType.STEADY
        else:
            # 细分趋势与季节性
            if abs(m["TrendSlope"]) > max(p.trend_slope_threshold, m["WeightedMean"] * 0.05):
                return DemandType.TRENDED
            elif m["SeasonalStrength"] > max(m["WeightedStd"] * p.seasonal_strength_ratio, p.seasonal_strength_min):
                return DemandType.SEASONAL
            else:
                # 爆发型判断
                tail_n = p.burst_tail_window
                mean_n = p.burst_mean_window
                max_tail = df["TotalDemand"].tail(tail_n).max()
                mean_tail = df["TotalDemand"].tail(mean_n).mean()
                if m["CV"] > p.burst_cv_threshold and m["ZeroRatio"] < p.burst_zero_ratio_max and max_tail > p.burst_recent_max_multiplier * mean_tail:
                    return DemandType.BURST
                return DemandType.INTERMITTENT

    def _determine_activity_level(self, df: pd.DataFrame) -> ActivityLevel:
        p = self.params
        recent_weeks =  df[df["WeeksAgo"] <= (p.recent_weeks_window - 1)]
        recent_weeks_with_demand = (recent_weeks["TotalDemand"] > 0).sum()
        zero_ratio = (df["TotalDemand"] == 0).sum() / len(df)

        if zero_ratio >= p.dormant_zero_ratio and recent_weeks_with_demand == 0:
            return ActivityLevel.DORMANT
        elif zero_ratio >= p.inactive_zero_ratio and recent_weeks_with_demand <= p.inactive_recent_weeks_demand_threshold:
            return ActivityLevel.INACTIVE
        elif zero_ratio >= p.occasional_zero_ratio_threshold:
            return ActivityLevel.OCCASIONAL
        else:
            return ActivityLevel.ACTIVE
