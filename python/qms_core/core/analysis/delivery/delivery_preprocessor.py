import pandas as pd
from qms_core.core.analysis.common.shipmode_assigner import (
    FallbackLeadTimeCache,
    LeadTimeService,
    TransportModePredictor
)
from qms_core.core.common.params.enums import TransportMode
from qms_core.core.utils.po_utils import generate_virtual_po_sublines

class DeliveryRecordPreprocessor:
    """将历史与在手采购记录合并为统一交付记录，含修正 TransportMode、生成 UnifiedPOLINE 等字段"""

    def __init__(self, stats_df: pd.DataFrame, tolerance_days: int = 2):
        self.stats_df = stats_df
        self.tolerance_days = tolerance_days

    def merge(self, df_hist: pd.DataFrame, df_git: pd.DataFrame) -> pd.Series:
        df_hist = df_hist.copy()
        df_git = df_git.copy()
        df_git = df_git[df_git["InvoiceDate"].notna()]
        df_hist["SourceType"] = "HIST"
        df_git["SourceType"] = "GIT"

        fallback_cache = FallbackLeadTimeCache.build(df_hist)
        predictor = TransportModePredictor(
            leadtime_svc=LeadTimeService(self.stats_df, fallback_cache),
            tolerance_days=self.tolerance_days
        )
        df_hist = predictor.correct(df_hist, overwrite=True, map_courier_to_air=True)
        df_hist["TransportMode"] = df_hist["TransportMode"].replace(TransportMode.COURIER.value, TransportMode.AIR.value)

        df_hist["ShippedQty"] = df_hist["ReceivedQty"]
        df_git["ShippedQty"] = df_git["InTransitQty"]
        df_git["ActualDeliveryDate"] = pd.NaT
        df_git["IsClosed"] = "N"

        cols = [
            'PONUM', 'POLINE', 'ITEMNUM', 'VendorCode', 'Warehouse',
            'OrderedQty', 'ShippedQty', 'POEntryDate', 'InvoiceDate', 'ActualDeliveryDate',
            'TransportMode', 'SourceType', 'IsClosed'
        ]
        df_all = pd.concat([df_hist[cols], df_git[cols]], ignore_index=True)

        df_all = generate_virtual_po_sublines(df_all, po_col="PONUM", line_col="POLINE", new_col="UnifiedPOLINE")

        df_all["BasePOLINE"] = df_all["POLINE"].astype(str).str.split("-").str[0]
        df_all["IsSplitDelivery"] = df_all.duplicated(subset=["PONUM", "BasePOLINE"], keep=False)

        return df_all

