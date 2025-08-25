import pandas as pd
import numpy as np
import datetime
from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer

class DeliveryBehaviorAnalyzer(BaseAnalyzer):
    """
    对合并后的 PO 交付记录做行为聚合：
    - 分批率、尾批占比、间隔时间、Q90 等
    """

    REQUIRED_COLUMNS = [
        "PONUM", "POLINE", "UnifiedPOLINE", "VendorCode", "TransportMode",
        "Warehouse", "ITEMNUM", "ShippedQty", "InvoiceDate", "IsClosed"
    ]

    def analyze(self, df: pd.DataFrame, closed_only=True) -> pd.DataFrame:
        self._validate_input_columns(df)

        if closed_only:
            df = df[df["IsClosed"] == "Y"].copy()

        df["BasePOLINE"] = df["POLINE"].astype(str).str.strip().str.split("-").str[0]
        df["POKEY"] = df["PONUM"].astype(str) + "-" + df["BasePOLINE"]
        df["SortDate"] = df["InvoiceDate"]

        batch_df = df.groupby(["POKEY", "InvoiceDate"]).agg(
            BatchQty=("ShippedQty", "sum")
        ).reset_index()

        def summarize_po(po_df):
            po_df = po_df.sort_values("InvoiceDate")
            n = len(po_df)
            total = po_df["BatchQty"].sum()
            spread = (po_df["InvoiceDate"].max() - po_df["InvoiceDate"].min()).days if n > 1 else 0
            interval = spread / (n - 1) if n > 1 else 0
            tail_ratio = po_df["BatchQty"].iloc[-1] / total if total > 0 else 0
            return pd.Series({
                "BatchCount": n,
                "TotalQty": total,
                "MaxBatchQty": po_df["BatchQty"].max(),
                "AvgBatchQty": po_df["BatchQty"].mean(),
                "TailQtyRate": tail_ratio,
                "SpreadDays": spread,
                "IntervalDays": interval,
                "IsSplit": int(n > 1)
            })

        batch_summary = batch_df.groupby("POKEY").apply(summarize_po).reset_index()

        meta_keys = df.drop_duplicates("POKEY")[["POKEY", "VendorCode", "TransportMode", "Warehouse", "ITEMNUM"]]
        merged = pd.merge(batch_summary, meta_keys, on="POKEY", how="left")

        result = merged.groupby(["VendorCode", "TransportMode", "Warehouse", "ITEMNUM"]).agg(
            TotalPOs=("POKEY", "nunique"),
            SplitPO_Rate=("IsSplit", "mean"),
            AvgBatchCount_SplitPOs=("BatchCount", lambda x: x[x > 1].mean() if (x > 1).any() else 1),
            TypicalSingleBatchQty=("AvgBatchQty", "mean"),
            MaxSingleBatchQty=("MaxBatchQty", lambda x: np.percentile(x, 90)),
            AvgSpreadDays_SplitPOs=("SpreadDays", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
            AvgIntervalBetweenBatches=("IntervalDays", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
            AvgTailQtyRate=("TailQtyRate", "mean")
        ).reset_index()

        result["LastUpdated"] = datetime.datetime.today().date()
        return result
    
class DeliveryBatchProfileAnalyzer(BaseAnalyzer):
    """
    将交付行为画像转为可用于 ETA 模拟的发货节奏模板。
    """

    REQUIRED_COLUMNS = [
        "VendorCode", "TransportMode", "Warehouse", "ITEMNUM",
        "SplitPO_Rate", "AvgBatchCount_SplitPOs", "AvgSpreadDays_SplitPOs",
        "TypicalSingleBatchQty", "MaxSingleBatchQty",
        "AvgIntervalBetweenBatches", "AvgTailQtyRate"
    ]

    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_input_columns(df)
        df = df.copy()

        df["IsBatchProne"] = (
            (df["SplitPO_Rate"] > 0.5) &
            (df["AvgBatchCount_SplitPOs"] >= 2) &
            (df["AvgSpreadDays_SplitPOs"] >= 7)
        ).map({True: "Y", False: "N"})

        df["PredictedBatchCount"] = df["AvgBatchCount_SplitPOs"].apply(np.ceil).astype(int)
        df["PredictedBatchQty"] = df["TypicalSingleBatchQty"]
        df["PredictedBatchIntervalDays"] = df["AvgIntervalBetweenBatches"]
        df["PredictedTailQtyRate"] = df["AvgTailQtyRate"]
        df["BatchTriggerQty"] = df["MaxSingleBatchQty"]
        df["LastUpdated"] = datetime.datetime.today().date()

        return df[[
            "VendorCode", "TransportMode", "Warehouse", "ITEMNUM",
            "IsBatchProne", "BatchTriggerQty", "PredictedBatchCount",
            "PredictedBatchQty", "PredictedBatchIntervalDays", "PredictedTailQtyRate","LastUpdated"
        ]]