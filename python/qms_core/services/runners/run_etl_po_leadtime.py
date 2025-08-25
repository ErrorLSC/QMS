from qms_core.infrastructure.config import MRPConfig
from qms_core.services.warehouse_filter import get_active_warehouses
from qms_core.pipelines.common.pipeline_factory import create_config_driven_pipeline
from qms_core.pipelines.common.registry.loader import load_job_registry_from_yaml
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork

def main(dry_run=False):
    config = MRPConfig()

    # Step 1: 注册 Job 类
    load_job_registry_from_yaml(r"C:\Users\JPEQZ\OneDrive - Epiroc\QMS\python\qms_core\config\ETL\job_registry.yaml")

    active_warehouses = get_active_warehouses(config, WMFAC="JPE")
    job_configs ={
    name: {"extract_params": {"warehouses": active_warehouses}}
    for name in ["Domestic", "Oversea", "Intransit","FreightCharge"]
    }

    POLeadTimePipeline = create_config_driven_pipeline(
        namespace="po_leadtime",
    )
    pipeline = POLeadTimePipeline(config=config, job_configs=job_configs)

    print("🧱 [Step 1] 执行 Metadata 主数据导入...")

    with UnitOfWork(config) as uow:
        pipeline.run_all(dry_run=dry_run, session=uow.session)

    print("✅ PO Leadtime 全部处理完成。")

if __name__ == "__main__":
    main(dry_run=False)