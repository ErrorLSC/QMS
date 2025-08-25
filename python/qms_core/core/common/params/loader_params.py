from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class LoaderParams(BaseModel):
    use_smart_writer: bool = True
    key_fields: Optional[List[str]] = None
    monitor_fields: Optional[List[str]] = None
    log_table_model: Optional[str] = None  # 💡 你可以后续映射成 ORM 类
    change_reason: Optional[str] = None
    exclude_fields: List[str] = []
    only_update_delta: bool = True
    skip_if_unchanged: bool = True
    write_params: Dict[str, Any] = {}