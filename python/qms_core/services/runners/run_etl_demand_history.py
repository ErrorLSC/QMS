from qms_core.infrastructure.config import MRPConfig
from qms_core.services.warehouse_filter import get_active_warehouses
from qms_core.pipelines.common.pipeline_factory import create_config_driven_pipeline
from qms_core.pipelines.common.registry.loader import load_job_registry_from_yaml
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork

def main(dry_run=False):
    config = MRPConfig()

    # 🧩 Step 1: 注册 Job 类
    load_job_registry_from_yaml(r"C:\Users\JPEQZ\OneDrive - Epiroc\QMS\python\qms_core\config\ETL\job_registry.yaml")
    active_warehouses = get_active_warehouses(config=config,WMFAC="JPE")

    job_configs = {
    name: {"extract_params": {"warehouses": active_warehouses}}
    for name in ["DemandHistoryRaw", "DemandHistory"]
    }

    # 🧩 Step 3: 用工厂生成 pipeline
    DemandPipeline = create_config_driven_pipeline("demand", run_order=["DemandHistory", "DemandHistoryRaw"])
    pipeline = DemandPipeline(config=config,job_configs=job_configs)
    with UnitOfWork(config) as uow:
        pipeline.run_all(dry_run,session=uow.session)


if __name__ == "__main__":
    main(dry_run=False)