from pydantic import BaseModel, Field
from typing import List

class IIMParams(BaseModel):
    item_types: List[str] = Field(default_factory=lambda: ["S", "E", "K", "A"])
    plcs: List[str] = Field(default_factory=lambda: ["MRS", "SMT", "RDT", "REX", "RGU", "EDB", "HAT"])

class IWIParams(BaseModel):
    warehouses: List[str] = Field(default_factory=lambda: ["5", "6"])
    location_exclude_rules: List[dict] = Field(default_factory=lambda: [{"warehouse": "5", "location": "BARAKI"}])

class ILMParams(BaseModel):
    warehouses: List[str] = Field(default_factory=list)

class DPSParams(BaseModel):
    pass

class AVMParams(BaseModel):
    order_type_filter: str = "UR"

class GCCParams(BaseModel):
    pass