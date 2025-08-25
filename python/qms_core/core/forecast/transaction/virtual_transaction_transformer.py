from qms_core.core.common.base_transformer import BaseTransformer
import pandas as pd

class VirtualTransactionTransformer(BaseTransformer):
    """
    将 Forecast 与 ETA 数据融合为统一的虚拟库存流水格式
    """

    COLUMN_TYPE_MAP = {
        "ITEMNUM": str,
        "Warehouse": str,
        "YearWeek": str,
        "StockChangeType": str,
        "QtyChange": float,
    }

    def __init__(self):
        super().__init__()


    @staticmethod
    def _yearweek_int_to_str(yw: int | float) -> str:
        """
        将 YearWeek 整数（如 202524）转为字符串 '2025-24W'
        """
        if pd.isnull(yw):
            return None
        yw = int(yw)
        year = yw // 100
        week = yw % 100
        return f"{year}-{week:02d}W"

    def transform(self, data_dict:dict) -> pd.DataFrame:
        eta_df = data_dict["eta"]
        eta_df["StockChangeType"] = "ETAInbound"
        eta_df = eta_df.rename(columns={"InTransitQty": "QtyChange"})
        eta_df = eta_df[["ITEMNUM", "Warehouse", "YearWeek", "StockChangeType", "QtyChange"]]

        forecast_dict = data_dict["forecast"]
        forecast_rows = []
        for (itemnum, wh), series in forecast_dict.items():
            if (series == 0).all():
                continue
            for yw, qty in series.items():
                forecast_rows.append({
                    "ITEMNUM": itemnum,
                    "Warehouse": wh,
                    "YearWeek": yw,
                    "StockChangeType": "ForecastDemand",
                    "QtyChange": -qty  # 消耗为负
                })

        forecast_df = pd.DataFrame(forecast_rows)

        all_df = pd.concat([eta_df, forecast_df], ignore_index=True)

        all_df = (
            all_df.groupby(["ITEMNUM", "Warehouse", "YearWeek", "StockChangeType"], as_index=False)
            .agg({"QtyChange": "sum"})
        )
        
        all_df = all_df.sort_values(by=["ITEMNUM", "Warehouse", "YearWeek"]).reset_index(drop=True)
        all_df["YearWeek"] = all_df["YearWeek"].apply(self._yearweek_int_to_str)
        all_df = self.enforce_column_types(all_df)
        return all_df