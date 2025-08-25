from qms_core.infrastructure.config import MRPConfig
from qms_core.pipelines.analysis.leadtime_pipeline import LeadtimeAnalysisPipeline
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork

def main(dry_run=False):
    config = MRPConfig()

    print("🧱 [Step 1] 执行交期分析 LeadtimeAnalysisPipeline...")

    # 👇 显式展示每个 Job 的配置项（目前都为空，保留结构便于扩展）
    job_configs = {
        "transport_leadtime": {
            # "extract_params": {},
            # "load_params": {}
        },
        "prepare_leadtime": {},
        "transport_preference": {},
        "delivery_behavior": {},
        "batch_profile": {},
        "smart_leadtime": {},
    }

    pipeline = LeadtimeAnalysisPipeline(
        config=config,
        job_configs=job_configs
    )
    with UnitOfWork(config) as uow:
        pipeline.run_all(dry_run=dry_run,session=uow.session)

    print("\n✅ Leadtime 分析全流程完成。")

if __name__ == "__main__":
    main(dry_run=False)