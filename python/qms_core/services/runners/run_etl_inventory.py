from qms_core.pipelines.common.registry.loader import load_job_registry_from_yaml
from qms_core.infrastructure.config import MRPConfig
from qms_core.services.warehouse_filter import get_active_warehouses
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork
from qms_core.pipelines.common.pipeline_factory import create_config_driven_pipeline

def main(dry_run):
    config = MRPConfig()
    # Step 1: 动态参数
    active_warehouses = get_active_warehouses(config, WMFAC='JPE')
    load_job_registry_from_yaml(r"C:\Users\JPEQZ\OneDrive - Epiroc\QMS\python\qms_core\config\ETL\job_registry.yaml")

    job_configs ={
    name: {"extract_params": {"warehouses": active_warehouses}}
    for name in ["ILI", "IWIAvail"]
    }

    InventoryPipeline = create_config_driven_pipeline("inventory", ["ILI", "IWIAvail"])
    pipeline = InventoryPipeline(config, job_configs=job_configs)
    with UnitOfWork(config) as uow:
        pipeline.run_all(dry_run,session=uow.session)

if __name__ == "__main__":
    main(dry_run=False)
