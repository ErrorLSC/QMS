from jinja2 import Template, StrictUndefined
from pathlib import Path

def render_sql_template(path: Path, context: dict | None = None, strict: bool = False) -> str:
    with open(path, "r", encoding="utf-8") as f:
        template_text = f.read()

    if context is None:
        context = {}

    if strict:
        template = Template(template_text, undefined=StrictUndefined)
    else:
        template = Template(template_text)

    return template.render(**context)