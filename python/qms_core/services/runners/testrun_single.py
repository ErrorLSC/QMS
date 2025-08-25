# scripts/testrun_single.py

from qms_core.core.item import Item
from qms_core.infrastructure.config import MRPConfig

from qms_core.core.forecast.demand import DemandClassifier, DemandForecaster
from qms_core.core.forecast.safety_stock import SafetyStockCalculator
from qms_core.services.forecast_service import ItemForecastService
from qms_core.core.common.params.ParasCenter import ParasCenter
from qms_core.core.common.params.enums import ForecastType
from qms_core.core.forecast.MRP.MRP_calculator import MRPCalculator

# 初始化数据库连接
config = MRPConfig()
session = config.get_session()

# 设置目标物料和仓库
itemnum = "5697006433"
warehouse = "6"

# 初始化 Item 实例
item = Item(itemnum=itemnum, warehouse=warehouse)

# 可选：调用 show_summary() 打印所有模块摘要信息
item.load_all(session)
params = ParasCenter()
classifier = DemandClassifier(params.classifier_params)
forecaster = DemandForecaster(params.forecast_params)
safety_calc = SafetyStockCalculator(params.safety_params)

service = ItemForecastService(classifier, forecaster, safety_calc)
mrp_runner = MRPCalculator(item=item,params=params.mrp_params)
# 执行完整流程
print("🔍 分类中...")
service.classify(item)

print("🔮 预测中...")
service.forecast(item)

print("📈 安全库存计算中...")
service.calculate_safety(item)

print("📊 回测评估中...")
evaluator = service.evaluate(
    item,
    analysis_start="2024-01-01",
    analysis_end="2025-04-01",
    backtest_window_weeks=4,
    forecast_type=ForecastType.MONTHLY
)
print("📊 MRP计算中...")
item.mrp.set_recommendation(mrp_runner.run())

item.show_summary()
# item.demand_type.write_to_db(config=config)

# 打印回测评估结果（你可以根据 evaluator 对象内容扩展）
print("\n📉 Forecast Evaluation Summary:")
print("MAPE:", evaluator.result.get("ForecastScore"))
print("Actual Demand:", evaluator.result.get("ActualDemand"))
print("Predicted Demand:", evaluator.result.get("PredictedDemand"))

session.close()
