from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer
import pandas as pd
import numpy as np
from scipy.stats import mode
from qms_core.core.utils.MRP_utils import calculate_ema

class PrepareLeadtimeStatsAnalyzer(BaseAnalyzer):
    """
    准备期分析器：
    - 可配置是否仅分析尾批
    - 支持指数平滑和多种统计指标
    """

    REQUIRED_COLUMNS = [
        "PONUM", "POLINE", "ITEMNUM", "Warehouse", "VendorCode", "InvoiceDate", "PrepareTime"
    ]

    def __init__(self, alpha: float = 0.3, tail_only: bool = True):
        self.alpha = alpha
        self.tail_only = tail_only

    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_input_columns(df)
        df = df.copy()

        if self.tail_only:
            df = self._filter_last_batches(df)

        df = df.dropna(subset=["PrepareTime"])
        grouped = df.groupby(["ITEMNUM", "Warehouse", "VendorCode"])

        agg = grouped["PrepareTime"].agg([
            ("MeanPrepDays", "mean"),
            ("PrepStd", "std"),
            ("ModePrepDays", lambda x: mode(x, keepdims=False).mode if len(x) > 0 else np.nan),
            ("Q60PrepDays", lambda x: np.percentile(x, 60)),
            ("Q90PrepDays", lambda x: np.percentile(x, 90)),
            ("SampleCount", "count"),
        ]).reset_index()

        smooth = grouped["PrepareTime"].apply(
            lambda s: calculate_ema(s, alpha=self.alpha)
        ).reset_index(name="ExpSmoothPrepDays")

        final = agg.merge(smooth, on=["ITEMNUM", "Warehouse", "VendorCode"])
        final["LastUpdated"] = pd.Timestamp.today()
        return final

    def _filter_last_batches(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["POLINE"] = df["POLINE"].astype(str).str.strip()
        df["BasePOLINE"] = df["POLINE"].str.split("-").str[0]
        df_sorted = df.sort_values("InvoiceDate")
        return df_sorted.groupby(["PONUM", "BasePOLINE"], as_index=False).tail(1)