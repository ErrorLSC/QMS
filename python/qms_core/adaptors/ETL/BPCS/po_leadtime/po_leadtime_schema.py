from pydantic import BaseModel
from typing import List

class DomesticPOParams(BaseModel):
    warehouses: List[str]
    po_entry_date_min: int = 20241001
    stockin_date_min: int = 20241001
    enable_hotzone: bool = False

class POIntransitParams(BaseModel):
    warehouses: List[str]
    po_entry_date_min: int = 20241001
    invoice_date_min: int = 20241001

class OverseaPOParams(BaseModel):
    warehouses: List[str]
    invoice_date_min: int = 20241001
    stockin_date_min: int = 20241001
    po_entry_date_min: int = 20241001

class OpenPOParams(BaseModel):
    warehouses: List[str]

class FreightChargeParams(BaseModel):
    warehouses: List[str]
    date_min: int = 20241231
