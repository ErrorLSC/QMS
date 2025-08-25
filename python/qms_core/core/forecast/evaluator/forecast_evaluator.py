import pandas as pd
import numpy as np
from datetime import datetime
from pandas._libs.tslibs.nattype import NaTType

from qms_core.core.forecast.demand import DemandClassifier, DemandForecaster
from qms_core.core.forecast.safety_stock import SafetyStockCalculator
from qms_core.infrastructure.db.models import ForecastEvaluation
from qms_core.core.common.params.enums import ForecastType


class ForecastEvaluator:
    def __init__(self, item, classifier: DemandClassifier, forecaster: DemandForecaster,
                 safetystock_calculator: SafetyStockCalculator,
                 analysis_start=None, analysis_end=None, backtest_window_weeks=4,
                 forecast_type=ForecastType.MONTHLY):
        self.item = item
        self.classifier = classifier
        self.forecaster = forecaster
        self.safetystock_calculator = safetystock_calculator
        self.backtest_window_weeks = backtest_window_weeks
        self.forecast_type = forecast_type

        self.demand_history = item.demand.history.copy()
        self.analysis_start = analysis_start or self.demand_history['YearWeek'].min()
        self.analysis_end = analysis_end or self.demand_history['YearWeek'].max()
        self.result = {}

    def evaluate(self):
        df = self.demand_history.copy()

        if pd.api.types.is_string_dtype(df["YearWeek"]) or df["YearWeek"].dtype != "datetime64[ns]":
            try:
                df["YearWeek"] = pd.to_datetime(df["YearWeek"] + "-1", format="%Y-W%W-%w", errors="coerce")
            except Exception:
                df["YearWeek"] = pd.to_datetime(df["YearWeek"], errors="coerce")

        for attr in ["analysis_start", "analysis_end"]:
            val = getattr(self, attr)
            if not isinstance(val, pd.Timestamp):
                try:
                    setattr(self, attr, pd.to_datetime(val, errors="coerce"))
                except Exception:
                    setattr(self, attr, pd.NaT)

        if isinstance(self.analysis_start, NaTType) or isinstance(self.analysis_end, NaTType):
            self.result = self._empty_result()
            return

        self.analysis_end = self.analysis_end - pd.to_timedelta(self.analysis_end.weekday(), unit='D')

        df_analysis = df[(df["YearWeek"] >= self.analysis_start) & (df["YearWeek"] <= self.analysis_end)]
        if df_analysis.empty:
            self.result = self._empty_result()
            return

        self.classifier.calculate_for_item(self.item, max_date=self.analysis_end)
        self.forecaster.forecast_item(self.item, max_date=self.analysis_end)
        self.safetystock_calculator.calculate_for_item(self.item, max_date=self.analysis_end)

        # 预测值
        if self.forecast_type == ForecastType.MONTHLY:
            predicted = self.item.forecast.forecast_monthly or 0
        elif self.forecast_type == ForecastType.QUARTERLY:
            predicted = (self.item.forecast.forecast_monthly) * 3 or 0
        else:
            raise NotImplementedError(f"forecast_type '{self.forecast_type}' 暂未实现")

        predicted = max(predicted, 0)

        # 回测期实际值
        actual_start = self.analysis_end
        actual_end = self.analysis_end + pd.Timedelta(weeks=self.backtest_window_weeks)
        df_backtest = df[(df["YearWeek"] >= actual_start) & (df["YearWeek"] < actual_end)]
        actual = df_backtest["TotalDemand"].sum()

        # 上期、去年同期
        prev_start = self.analysis_end - pd.Timedelta(weeks=self.backtest_window_weeks)
        prev_end = self.analysis_end
        last_period = df[(df["YearWeek"] >= prev_start) & (df["YearWeek"] < prev_end)]["TotalDemand"].sum()

        last_year_start = actual_start - pd.DateOffset(years=1)
        last_year_end = actual_end - pd.DateOffset(years=1)
        last_year = df[(df["YearWeek"] >= last_year_start) & (df["YearWeek"] < last_year_end)]["TotalDemand"].sum()

        # 误差和趋势
        error = predicted - actual
        absolute_error = abs(error)
        ape = absolute_error / actual if actual > 0 else np.nan
        mom = (actual - last_period) / last_period if last_period > 0 else np.nan
        yoy = (actual - last_year) / last_year if last_year > 0 else np.nan

        dyn_ss = self.item.safetystock.dynamic_safety_stock or 0
        final_ss = self.item.safetystock.final_safety_stock or dyn_ss
        recommended_service_level = self.item.safetystock.recommended_service_level or 0.85

        coverage = predicted + dyn_ss
        is_covered = actual <= coverage
        coverage_gap = coverage - actual

        ape_score = 1 / (1 + ape) if not pd.isna(ape) else 0
        mom = 0 if pd.isna(mom) else mom
        yoy = 0 if pd.isna(yoy) else yoy
        trend_score = 1 / (1 + abs(mom) + abs(yoy))
        coverage_score = 1.0 if is_covered else max(0.5, 1 - abs(coverage_gap) / (actual + 1e-6))
        over_penalty = max(0, (predicted - actual) / (actual + 1e-6))
        over_penalty_score = max(0.5, 1 - over_penalty)

        score = round(
            ape_score * 0.4 +
            trend_score * 0.2 +
            coverage_score * 0.2 +
            over_penalty_score * 0.2,
            3
        )

        self.result = {
            "ITEMNUM": self.item.itemnum,
            "Warehouse": self.item.warehouse,
            "PredictedDemand": predicted,
            "ActualDemand": actual,
            "SamePeriodLastYearDemand": last_year,
            "LastPeriodDemand": last_period,
            "YoY_Growth": yoy,
            "MoM_Growth": mom,
            "Error": error,
            "AbsoluteError": absolute_error,
            "APE": ape,
            "ForecastScore": score,
            "DemandType": self.item.demand_type.demand_type,
            "ActivityLevel": self.item.demand_type.activity_level,
            "ISCST": self.item.master.cost,
            "CXPPLC": self.item.master.plc,
            "IDESC": self.item.master.idesc,
            "EvalStart": self.analysis_start,
            "EvalEnd": self.analysis_end,
            "BacktestWindow": self.backtest_window_weeks,
            "DynamicSafetyStock": dyn_ss,
            "FinalSafetyStock": final_ss,
            "ForecastPlusSafety": coverage,
            "CoverageGap": coverage_gap,
            "Covered": is_covered,
            "RecommendedServiceLevel": recommended_service_level
        }
        return self.result

    def _empty_result(self):
        return {
            "ITEMNUM": self.item.itemnum,
            "Warehouse": self.item.warehouse,
            "ForecastScore": None,
            "MAE": None,
            "MAPE": None,
            "RMSE": None,
            "ForecastBias": None,
            "LastUpdated": datetime.now().strftime("%Y-%m-%d")
        }

    def to_dict(self):
        return self.result if self.result and self.result.get("ForecastScore") is not None else None

    def is_valid_evaluation(self):
        return self.result is not None and self.result.get("ForecastScore") is not None

    def write_to_db(self, session):
        if not self.is_valid_evaluation():
            return

        r = self.result
        record = ForecastEvaluation(
            ITEMNUM=r["ITEMNUM"],
            Warehouse=r["Warehouse"],
            EvalStart=r["EvalStart"].date(),
            EvalEnd=r["EvalEnd"].date(),
            BacktestWindow=r["BacktestWindow"],
            PredictedDemand=r["PredictedDemand"],
            DynamicSafetyStock=r["DynamicSafetyStock"],
            ActualDemand=r["ActualDemand"],
            APE=r["APE"],
            MoM_Growth=r["MoM_Growth"],
            YoY_Growth=r["YoY_Growth"],
            ForecastScore=r["ForecastScore"],
            Covered="Y" if r["Covered"] else "N",
            CoverageGap=r["CoverageGap"],
            DemandType=r.get("DemandType"),
            ActivityLevel=r.get("ActivityLevel"),
            LastUpdated=datetime.now().strftime("%Y-%m-%d")
        )
        session.merge(record)
        session.commit()
