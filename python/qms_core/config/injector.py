import re

def inject_runtime_values(config_dict: dict, context: dict, placeholder_pattern=r"__([A-Z_]+)__"):
    def replace_value(value):
        if isinstance(value, str):
            match = re.fullmatch(placeholder_pattern, value)
            if match:
                key = match.group(1)
                return context.get(key, value)
        return value

    def recurse(obj):
        if isinstance(obj, dict):
            return {k: recurse(replace_value(v)) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [recurse(replace_value(i)) for i in obj]
        else:
            return replace_value(obj)

    return recurse(config_dict)