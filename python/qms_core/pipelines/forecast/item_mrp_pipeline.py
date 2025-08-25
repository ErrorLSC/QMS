from qms_core.infrastructure.uow.unit_of_work import UnitOfWork
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.pipelines.common.base_pipeline import BasePipeline
from qms_core.pipelines.forecast.demand.demand_classification_job import DemandClassificationJob
from qms_core.pipelines.forecast.demand.demand_forecast_job import ForecastGenerationJob
from qms_core.pipelines.forecast.safetystock.safety_stock_job import SafetyStockGenerationJob
from qms_core.pipelines.forecast.MRP.MRP_job import MRPJob
from qms_core.core.item.item_manager import ItemManager
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.core.forecast.MRP.dynamic_MRP_calculator import  DynamicMRPCalculator
from qms_core.core.item.item import Item
from typing import Optional,List
from qms_core.core.common.params.loader_params import LoaderParams


class SharedMemoryPipeline(BasePipeline):
    """
    在 BasePipeline 基础上扩展：支持 Job 间共享中间结果作为 input_data 注入下游。
    典型应用场景：ITEM → Forecast → Safety → MRP 连续流水线计算。
    """

    DEFAULT_SHARED_KEYS = ["items", "forecast", "safety_stock", "demand_type"]

    def __init__(self, config=None, items: Optional[List[Item]] = None, item_ids: Optional[list[tuple[str, str]]] = None):
        super().__init__(config)
        self.items = items or []
        self.item_ids = item_ids or []
        self.data_container = None 

    def prepare_global_items(self) -> List[Item]:
        if not self.items:
            print("📦 Ready to prepare item list...")
            manager = ItemManager(self.item_ids) if self.item_ids else ItemManager.from_demand_history(self.config)
            self.items = manager.items
        return self.items

    def load_global_data(self) -> dict :
        print("📦 开始加载全局主数据...")

        preloader = ItemDataPreloader(self.config, self.items)

        self.data_container = MRPDataContainer(
            items=self.items,
            demand_df=preloader.load_demand_history(),
            forecast_dict={},  # 空
            inventory_dict=preloader.load_inventory_info(),
            master_dict=preloader.load_item_master_info(),
            demand_type_dict={},
            safety_stock_dict={},
            smart_lead_time_dict=preloader.load_smart_lead_time(),
        )

        print(f"✅ 主数据加载完成：{len(self.items)} 项物料")

    def run_all(self, dry_run: bool = False, session=None) -> dict[str, object]:
        print("📦 Step 0: 加载主数据")
        self.items = self.prepare_global_items()
        self.load_global_data()

        results = {}
        with UnitOfWork(self.config) as uow:
            print("\n🔮 Step 1: Demand Type Classification")
            classify_job = DemandClassificationJob(config=self.config,data_container=self.data_container)
            classify_result = classify_job.run(dry_run=dry_run, session=uow.session, return_output=True)
            results["demand_type"] = classify_result # {"df": demand type classification result dataframe, "core": result dict, "items": proceed items}
            demand_type_dict = classify_result.get("core",{})
            self.data_container.demand_type_dict = demand_type_dict      
            print(f"✅ 分类完成：{len(results['demand_type']['items'])} 项")

            # Step 2: Forecast
            print("\n🔮 Step 2: Forecast 预测")
            forecast_job = ForecastGenerationJob(config=self.config,data_container=self.data_container)
            forecast_result = forecast_job.run(dry_run=dry_run, session=uow.session, return_output=True)
            results["forecast"] = forecast_result
            forecast_dict = forecast_result.get("core", {})
            self.data_container.forecast_dict = forecast_dict
            print(f"✅ 预测完成：{len(results['forecast']['items'])} 项")
   
            # Step 3: Safety Stock
            print("\n🛡️ Step 3: 安全库存计算")
            safety_job = SafetyStockGenerationJob(config=self.config,data_container=self.data_container)
            safety_result = safety_job.run(dry_run=dry_run, session=uow.session, return_output=True)
            results["safety_stock"] = safety_result
            safety_dict = safety_result.get("core", {})
            self.data_container.safety_stock_dict = safety_dict
            print(f"✅ 安全库存计算完成：{len(results['safety_stock']['items'])} 项")
 
            # Step 4: MRP
            print("\n📦 Step 4: 静态补货计算 MRP")
            mrp_job = MRPJob(config=self.config,use_vectorized=True,data_container=self.data_container)
            mrp_result = mrp_job.run(dry_run=dry_run, session=uow.session, return_output=True)
            results["mrp"] = mrp_result
            print(f"✅ 静态补货建议生成完成：{len(results['mrp']['items'])} 项")

            # Step 5: Dynamic MRP
            print("\n📦 Step 5: 动态补货计算 MRP")
            loader_param = LoaderParams(exclude_fields=["CalcDate"],write_params={"delete_before_insert":False,"upsert":False})
            dynamic_mrp_calculator = DynamicMRPCalculator(data_container=self.data_container,use_vectorized=True)
            dynamic_mrp_job =MRPJob(config=self.config,use_vectorized=True,calculator=dynamic_mrp_calculator,load_params=loader_param)
            dynamic_mrp_results = dynamic_mrp_job.run(dry_run=dry_run,session=uow.session,return_output=True)
            results["dynamic_mrp"] = dynamic_mrp_results
            print(f"✅ 动态补货建议生成完成：{len(results['dynamic_mrp']['items'])} 项")

        return results
    
