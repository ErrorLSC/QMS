from qms_core.infrastructure.db.models import (
PO_ETA_Recommendation,ItemForecastRecord
)
from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.core.common.base_extractor import BaseExtractor
import pandas as pd
import json
from qms_core.core.forecast.common.forecast_utils import get_next_n_yearweeks,to_yearweek_int
import numpy as np

class VirtualTransactionExtractor(BaseExtractor):
    def __init__(self, config):
        self.config = config

    @staticmethod
    def _convert_eta_week_to_int(df: pd.DataFrame) -> pd.DataFrame:
        """
        将 ETA_Week (如 '2025-24W') 转换为整数 YearWeek (如 202524)，用于后续计算。
        """
        def parse_yearweek_str(yw_str: str):
            try:
                year, week = yw_str.replace("W", "").split("-")
                return int(year) * 100 + int(week)
            except Exception:
                return np.nan

        df = df.copy()
        df["YearWeek"] = df["ETA_Week"].apply(parse_yearweek_str).astype("Int64")
        return df
    
    def fetch_ETA(self,drop_overdue: bool = True)-> pd.DataFrame:
        df = fetch_orm_data(self.config, PO_ETA_Recommendation)
        df = self._convert_eta_week_to_int(df)

        if drop_overdue:
            current_yw = to_yearweek_int(pd.Timestamp.today())
            df = df[df["YearWeek"] >= current_yw].copy()
        return df

    def fetch_forecast(self) -> dict[tuple[str, str], pd.Series]:
        df = fetch_orm_data(self.config, ItemForecastRecord)

        forecast_dict = {}
        for _, row in df.iterrows():
            key = (row["ITEMNUM"], row["Warehouse"])
            try:
                values = json.loads(row["ForecastSeriesJSON"])
                if not isinstance(values, list) or not values:
                    continue

                # 自动生成 YearWeek 序列
                today = pd.Timestamp.today()
                start_date = today - pd.Timedelta(days=today.weekday())
                yearweeks = get_next_n_yearweeks(start_date, len(values))  # 例：返回 [202526, 202527, ...]
                series = pd.Series(data=values, index=yearweeks, dtype=float)
                forecast_dict[key] = series

            except Exception as e:
                print(f"⚠️ Forecast JSON 解析失败: {key}, error={e}")
                continue

        return forecast_dict

    def fetch(self,drop_overdue:bool=True) -> dict:
        """
        返回格式:
        {
            "eta": ETA_df,  # 含 YearWeek: int
            "forecast": {(ITEMNUM, Warehouse): pd.Series of forecast}
        }
        """
        ETA_df = self.fetch_ETA(drop_overdue=drop_overdue)
        forecast_dict = self.fetch_forecast()
        return {
            "eta": ETA_df,
            "forecast": forecast_dict
        }