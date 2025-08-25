from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer
from qms_core.core.analysis.common.shipmode_assigner import (
    FallbackLeadTimeCache, LeadTimeService, TransportModePredictor
)
from qms_core.core.common.params.enums import TransportMode
import pandas as pd
import datetime


class TransportPreferenceAnalyzer(BaseAnalyzer):
    """
    基于历史交期修正后的模式，统计每个物料的运输方式偏好。
    """

    REQUIRED_COLUMNS = [
        "ITEMNUM", "Warehouse", "VendorCode",
        "TransportTime", "TransportMode", "ActualDeliveryDate"
    ]

    def __init__(self, tolerance_days: int = 0):
        self.tolerance_days = tolerance_days

    def analyze(self, df: pd.DataFrame, stats_df: pd.DataFrame) -> pd.DataFrame:
        self._validate_input_columns(df)

        df = df[df["TransportTime"].notna() & df["TransportMode"].notna()].copy()

        # 修正模式
        fb_cache = FallbackLeadTimeCache.build(df)
        leadtime_svc = LeadTimeService(stats_df=stats_df, fb_cache=fb_cache)
        predictor = TransportModePredictor(leadtime_svc, tolerance_days=self.tolerance_days)

        df = predictor.correct(df, overwrite=True, map_courier_to_air=True)
        df["TransportMode"] = df["TransportMode"].replace(
            TransportMode.COURIER.value, TransportMode.AIR.value
        )

        # 聚合
        agg = df.groupby(
            ["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
            dropna=False
        ).agg(
            Count=("TransportMode", "size"),
            LastUsedDate=("ActualDeliveryDate", "max")
        ).reset_index()

        agg["Total"] = agg.groupby(["ITEMNUM", "Warehouse", "VendorCode"])["Count"].transform("sum")
        agg["Confidence"] = agg["Count"] / agg["Total"]

        agg = agg.sort_values(
            ["ITEMNUM", "Warehouse", "VendorCode", "Count"],
            ascending=[True, True, True, False]
        ).reset_index(drop=True)

        agg["Rank"] = agg.groupby(["ITEMNUM", "Warehouse", "VendorCode"]).cumcount() + 1
        agg["LastUsedDate"] = pd.to_datetime(agg["LastUsedDate"], errors="coerce").dt.date
        agg["LastUsedDate"] = agg["LastUsedDate"].apply(lambda x: x if pd.notnull(x) else None)
        agg["LastUpdated"] = datetime.datetime.today()

        return agg[[
            "ITEMNUM", "Warehouse", "VendorCode", "TransportMode",
            "Rank", "Count", "Confidence", "LastUsedDate", "LastUpdated"
        ]]
