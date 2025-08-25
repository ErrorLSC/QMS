from qms_core.pipelines.common.base_pipeline import BasePipeline
from qms_core.pipelines.common.registry.job_registry import get_all_jobs

def create_config_driven_pipeline(namespace: str, run_order: list[str] = None):
    class ConfigDrivenPipeline(BasePipeline):
        @property
        def JOB_CLASSES(self):
           
            return get_all_jobs(namespace)

        DEFAULT_RUN_ORDER = run_order or []

    ConfigDrivenPipeline.__name__ = f"{namespace.capitalize()}Pipeline"
    return ConfigDrivenPipeline

