from pydantic import BaseModel, Field
from typing import List

class DemandHistoryParams(BaseModel):
    min_order_entry_date: int = 20240101
    only_recent_orders: bool = True
    exclude_deleted_orders: bool = True
    exclude_redistribution_orders: bool = True
    exclude_internal_orders: bool = False
    allowed_item_types: List[str] = Field(default_factory=lambda: ["S", "E", "K", "A"])
    allowed_work_types: List[str] = Field(default_factory=lambda: ['ASX', 'ASY', 'GW ', 'KIT', 'REP', 'SPW'])
    warehouses: List[str] = Field(default_factory=lambda: ["5", "6"])