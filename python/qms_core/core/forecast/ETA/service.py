from qms_core.core.analysis.transport.transport_preference_analyzer import (
    FallbackLeadTimeCache,
    LeadTimeService,
    TransportModePredictor
)
import pandas as pd

def build_fallback_cache(df_history: pd.DataFrame) -> FallbackLeadTimeCache:
    return FallbackLeadTimeCache.build(df_history)

def build_leadtime_service(
    df_vendor_stats: pd.DataFrame,
    df_history: pd.DataFrame
) -> LeadTimeService:
    fb_cache = build_fallback_cache(df_history)
    return LeadTimeService(stats_df=df_vendor_stats, fb_cache=fb_cache)

def build_mode_predictor(
    df_vendor_stats: pd.DataFrame,
    df_history: pd.DataFrame
) -> TransportModePredictor:
    svc = build_leadtime_service(df_vendor_stats, df_history)
    return TransportModePredictor(leadtime_svc=svc)

