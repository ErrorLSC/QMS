from qms_core.core.common.base_transformer import BaseTransformer
import pandas as pd

class StockSimulatorTransformer(BaseTransformer):

    def __init__(self):
        super().__init__()

    def transform(self, data_dict:dict[str,pd.DataFrame]) -> pd.DataFrame:
        stock_df = data_dict["stock"]
        transaction_df = data_dict["transaction"]

        initial_stock = stock_df.set_index(["ITEMNUM", "Warehouse"])["AVAIL"].to_dict()

        result_rows = []

        for (item, wh), group in transaction_df.groupby(["ITEMNUM", "Warehouse"]):
            stock = initial_stock.get((item, wh), 0.0)
            for _, row in group.iterrows():
                yearweek = row["YearWeek"]
                stock += row["QtyChange"]
                result_rows.append({
                    "ITEMNUM": item,
                    "Warehouse": wh,
                    "YearWeek": yearweek,
                    "ProjectedStock": stock
                })

        result_df = pd.DataFrame(result_rows)
        return result_df