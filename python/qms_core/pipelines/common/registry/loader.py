import yaml
from qms_core.pipelines.common.registry.job_registry import register_job

import importlib

def resolve_class_from_string(class_path: str):
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def load_job_registry_from_yaml(yaml_paths: str | list[str]):
    if isinstance(yaml_paths, str):
        yaml_paths = [yaml_paths]

    for path in yaml_paths:
        with open(path, encoding="utf-8") as f:
            registry = yaml.safe_load(f)
            for namespace, jobs in registry.items():
                for name, class_path in jobs.items():
                    cls = resolve_class_from_string(class_path)
                    register_job(name=name, cls=cls, namespace=namespace)
    
