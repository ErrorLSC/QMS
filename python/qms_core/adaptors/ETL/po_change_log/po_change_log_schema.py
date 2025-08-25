from pydantic import BaseModel
from typing import List

class POChangeLogParams(BaseModel):
    warehouses: List[str]

