import pandas as pd
from typing import Tuple
import datetime

from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer
from qms_core.core.analysis.common.shipmode_assigner import (
    FallbackLeadTimeCache,
    LeadTimeService,
    TransportModePredictor
)
import numpy as np
from qms_core.core.utils.MRP_utils import calculate_ema 

class TransportLeadtimeAnalyzer(BaseAnalyzer):
    REQUIRED_COLUMNS = ["PONUM", "POLINE", "Warehouse", "VendorCode", "TransportTime", "TransportMode", "InvoiceDate", "ActualDeliveryDate"]

    def __init__(self, quantile_threshold: float = 0.9, tolerance_days: int = 0, shipment_granularity: bool = True):
        self.quantile_threshold = quantile_threshold
        self.tolerance_days = tolerance_days
        self.shipment_granularity = shipment_granularity

    def analyze(
        self,
        df: pd.DataFrame,
        stats_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        执行运输交期分析：
        - 修正运输方式（预测器）
        - 计算交期统计（Q60/Q90/mean 等）
        """
        self._validate_input_columns(df)
        df = self._apply_granularity(df) if self.shipment_granularity else df

        fallback_cache = FallbackLeadTimeCache.build(df)
        leadtime_svc = LeadTimeService(stats_df=stats_df, fb_cache=fallback_cache)
        predictor = TransportModePredictor(leadtime_svc=leadtime_svc, tolerance_days=self.tolerance_days)
        df_corrected = predictor.correct(df, overwrite=True, map_courier_to_air=False)

        df_stats = self._aggregate_stats(df_corrected)
        return df_corrected, df_stats

    def _apply_granularity(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["PO_KEY"] = df["PONUM"].astype(str) + "-" + df["POLINE"].astype(str)
        return (
            df.groupby(["Warehouse", "VendorCode", "InvoiceDate", "ActualDeliveryDate", "TransportMode"])
            .agg(
                TransportTime=("TransportTime", "mean"),
                ShipmentSize=("PO_KEY", "nunique")
            )
            .reset_index()
        )

    def _aggregate_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        stats = (
            df.groupby(["Warehouse", "VendorCode", "TransportMode"])["TransportTime"]
            .agg(
                MeanTransportLeadTime="mean",
                TransportLeadTimeStd="std",
                Q60TransportLeadTime=lambda x: x.quantile(0.6),
                Q90TransportLeadTime=lambda x: x.quantile(self.quantile_threshold),
                ModeTransportLeadTime=lambda x: x.mode().iloc[0] if not x.mode().empty else None,
                SmoothedTransportLeadTime=lambda x: calculate_ema(x, alpha=0.2),
                SampleCount="count"
            )
            .reset_index()
        )

        stats["CostPerKg"] = np.nan
        stats["BaseCharge"] = np.nan
        stats["LastUpdated"] = datetime.datetime.now()
        return stats