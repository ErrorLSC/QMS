from qms_core.infrastructure.uow.unit_of_work import UnitOfWork
from qms_core.pipelines.forecast.demand.demand_classification_job import DemandClassificationJob
from qms_core.pipelines.forecast.demand.demand_forecast_job import ForecastGenerationJob
from qms_core.pipelines.forecast.safetystock.safety_stock_job import SafetyStockGenerationJob
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.pipelines.forecast.MRP.MRP_job import MRPJob
from qms_core.infrastructure.config import MRPConfig 
from qms_core.pipelines.forecast.item_mrp_pipeline import SharedMemoryPipeline 
from qms_core.core.forecast.safety_stock.dynamic_calculator import DynamicSafetyStockCalculator
from qms_core.core.forecast.MRP.dynamic_MRP_calculator import DynamicMRPCalculator
from qms_core.core.item.item_manager import ItemManager

def main(dry_run=True):
    classifyjob = DemandClassificationJob(config=config)
    forecastjob = ForecastGenerationJob(config=config)
    safetyjob = SafetyStockGenerationJob(config=config)
    mrpjob = MRPJob(config=config,use_vectorized=True)
    with UnitOfWork(config) as uow:
        classifyjob.run(dry_run=dry_run,session=uow.session)
        forecastjob.run(dry_run=dry_run,session=uow.session)
        safetyjob.run(dry_run=dry_run,session=uow.session)
        mrpjob.run(dry_run=dry_run,session=uow.session)

def classify_batch(dry_run=True) -> None:
    classify_job = DemandClassificationJob(config=config)
    classify_job.run(dry_run=dry_run)

def forecast_batch(dry_run=True) -> None:
    forecast_job = ForecastGenerationJob(config=config)
    forecast_job.run(dry_run=dry_run)

def safety_stock_batch(dry_run=True):
    safety_stock_calculator = DynamicSafetyStockCalculator()
    safety_stock_job = SafetyStockGenerationJob(config=config,calculator=safety_stock_calculator)
    safety_stock_job.run(dry_run=dry_run)

def mrp_job_batch(dry_run=True):
    manager = (ItemManager.from_demand_history(config))
    items = manager.items

    data_preloader = ItemDataPreloader(config=config,items=items)
    data_container = MRPDataContainer.from_preloader(preloader=data_preloader,items=items)

    # print(data_container.smart_lead_time_dict)
    mrp_calculator = DynamicMRPCalculator(data_container=data_container)
    mrp_job = MRPJob(config=config,use_vectorized=True,data_container=data_container,calculator=mrp_calculator)
    mrp_job.run(dry_run=dry_run)

if __name__ == "__main__":
    config = MRPConfig()
    # main(dry_run=False)
    # MRP_main(dry_run=False)
    pipeline=SharedMemoryPipeline(config=config)
    pipeline.run_all(dry_run=False)
    # classifyjob = DemandClassificationJob(config=config)
    # safetyjob = SafetyStockGenerationJob(config=config)
    # safetyjob.run(dry_run=True,item_ids=[("2653236311","5")])
    # safety_stock_batch(dry_run=True)
    # mrp_job_batch(dry_run=True)
    # classify_batch(dry_run=True)
    # forecast_batch(dry_run=True)