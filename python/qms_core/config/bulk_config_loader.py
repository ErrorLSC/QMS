import yaml

def load_all_job_configs_with_context(config_paths: list[str], context: dict) -> dict:
    from qms_core.config.injector import inject_runtime_values
    merged = {}
    for path in config_paths:
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
            cfg = inject_runtime_values(cfg, context)
            merged.update(cfg)
    return merged