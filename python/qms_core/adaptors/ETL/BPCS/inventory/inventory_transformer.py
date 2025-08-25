import pandas as pd
from qms_core.core.common.base_transformer import BaseTransformer

class ILITransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "WAREHOUSE": "Warehouse"
        })
   
class IWIAvailTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "WAREHOUSE": "Warehouse"
        })