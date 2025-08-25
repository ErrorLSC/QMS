import pandas as pd
from qms_core.core.common.base_transformer import BaseTransformer

class DemandHistoryTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.enforce_column_types(df)
        df =  df.rename(columns={
            "WAREHOUSE": "Warehouse"
        })

        return df
