# qms_core/pipelines/common/job_registry.py

from collections import defaultdict

_job_registry = defaultdict(dict)  # namespace → job_name → class

def register_job(name: str, cls, namespace: str = "default"):
    if name in _job_registry[namespace]:
        print(f"⚠️ Job '{name}' 已在 namespace '{namespace}' 中注册，将被覆盖")
    _job_registry[namespace][name] = cls

def get_job(name: str, namespace: str = "default"):
    return _job_registry[namespace].get(name)

def get_all_jobs(namespace: str = "default") -> dict:
    return dict(_job_registry[namespace])

def clear_registry(namespace: str = "default"):
    _job_registry[namespace].clear()

def job(name: str, namespace: str = "default"):
    """
    装饰器：用于自动注册 Job 类。
    用法：@job("ILI", namespace="inventory")
    """
    def decorator(cls):
        register_job(name, cls, namespace)
        return cls
    return decorator

def list_namespaces() -> list[str]:
    """列出当前已注册的所有 namespace"""
    return list(_job_registry.keys())


def list_jobs(namespace: str) -> list[str]:
    """列出某个 namespace 下注册的 job 名称"""
    return list(_job_registry[namespace].keys())


def print_registry(verbose: bool = False):
    """打印所有 namespace 和注册 job，支持 verbose 查看类名"""
    print("📚 当前注册表概览：")
    for ns in list_namespaces():
        jobs = _job_registry[ns]
        print(f"🗂️ Namespace: '{ns}'  ({len(jobs)} jobs)")
        for name, cls in jobs.items():
            if verbose:
                print(f"  └── {name} → {cls.__module__}.{cls.__name__}")
            else:
                print(f"  └── {name}")