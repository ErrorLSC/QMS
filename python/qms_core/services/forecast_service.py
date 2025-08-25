from qms_core.core.forecast.demand import DemandClassifier, DemandForecaster
from qms_core.core.forecast.safety_stock import SafetyStockCalculator
from qms_core.core.forecast.evaluator.forecast_evaluator import ForecastEvaluator
from qms_core.core.common.params.enums import ForecastType


class ItemForecastService:
    def __init__(
        self,
        classifier: DemandClassifier,
        forecaster: DemandForecaster,
        safetystock_calculator: SafetyStockCalculator
    ):
        self.classifier = classifier
        self.forecaster = forecaster
        self.safetystock_calculator = safetystock_calculator

    def classify(self, item, max_date=None):
        self.classifier.calculate_for_item(item, max_date=max_date)

    def forecast(self, item, max_date=None):
        self.forecaster.forecast_item(item, max_date=max_date)

    def calculate_safety(self, item, max_date=None):
        return self.safetystock_calculator.calculate_for_item(item, max_date=max_date)

    def evaluate(
        self,
        item,
        analysis_start=None,
        analysis_end=None,
        backtest_window_weeks=4,
        forecast_type=ForecastType.MONTHLY
    ):
        evaluator = ForecastEvaluator(
            item=item,
            classifier=self.classifier,
            forecaster=self.forecaster,
            safetystock_calculator=self.safetystock_calculator,
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            backtest_window_weeks=backtest_window_weeks,
            forecast_type=forecast_type
        )
        evaluator.evaluate()
        return evaluator
