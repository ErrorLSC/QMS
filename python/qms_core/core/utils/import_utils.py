import importlib

def resolve_class_from_string(path: str):
    try:
        module_path, class_name = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except Exception as e:
        raise ImportError(f"⚠️ 无法导入类 {path}: {e}")