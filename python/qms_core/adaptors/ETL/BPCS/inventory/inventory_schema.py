from pydantic import BaseModel, Field
from typing import List

class InventoryParams(BaseModel):
    warehouses: List[str] = Field(default_factory=lambda: ["5", "6"])