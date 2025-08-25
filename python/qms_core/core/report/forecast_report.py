import pandas as pd
import numpy as np
from qms_core.core.common.params.enums import DemandType,ActivityLevel

def evaluate_system_forecast(df: pd.DataFrame, ignore_inactive=True) -> dict:
    """
    评估整个系统的预测表现（含 Bias），默认排除 Inactive / Dormant 物料。
    """
    df = df.copy()
    df["AbsError"] = (df["ActualDemand"] - df["PredictedDemand"]).abs()
    df["APE"] = df["APE"].fillna(0)

    # ✅ 过滤条件：默认忽略 Dormant / Inactive和New Item
    if ignore_inactive:
        df = df[~df["ActivityLevel"].isin([ActivityLevel.INACTIVE, ActivityLevel.DORMANT])]
        df = df[~df["DemandType"].isin([DemandType.NEW,DemandType.REPLACED,DemandType.STOCKONLY])]

    total_pred = df["PredictedDemand"].sum()
    total_actual = df["ActualDemand"].replace(0, np.nan).sum()
    bias = (total_pred - total_actual) / total_actual if total_actual > 0 else np.nan

    result = {}

    # 🔹 Overall summary
    result["Overall"] = {
        "MAPE": round(df["APE"].mean(), 4),
        "WAPE": round(df["AbsError"].sum() / total_actual, 4) if total_actual else None,
        "RMSE": round(np.sqrt(((df["ActualDemand"] - df["PredictedDemand"]) ** 2).mean()), 4),
        "ForecastScore": round(df["ForecastScore"].mean(), 4),
        "CoverageRate": round((df["Covered"] == "Y").mean(), 4),
        "Bias": round(bias, 4)
    }

    # 🔹 By Demand Type
    result["ByDemandType"] = df.groupby("DemandType").apply(lambda g: {
        "MAPE": round(g["APE"].mean(), 4),
        "WAPE": round(g["AbsError"].sum() / g["ActualDemand"].replace(0, np.nan).sum(), 4),
        "ForecastScore": round(g["ForecastScore"].mean(), 4),
        "CoverageRate": round((g["Covered"] == "Y").mean(), 4),
        "Bias": round(
            (g["PredictedDemand"].sum() - g["ActualDemand"].sum()) / g["ActualDemand"].replace(0, np.nan).sum(), 4
        ) if g["ActualDemand"].sum() > 0 else None
    }).to_dict()

    # 🔹 By Activity Level
    result["ByActivityLevel"] = df.groupby("ActivityLevel").apply(lambda g: {
        "MAPE": round(g["APE"].mean(), 4),
        "WAPE": round(g["AbsError"].sum() / g["ActualDemand"].replace(0, np.nan).sum(), 4),
        "ForecastScore": round(g["ForecastScore"].mean(), 4),
        "CoverageRate": round((g["Covered"] == "Y").mean(), 4),
        "Bias": round(
            (g["PredictedDemand"].sum() - g["ActualDemand"].sum()) / g["ActualDemand"].replace(0, np.nan).sum(), 4
        ) if g["ActualDemand"].sum() > 0 else None
    }).to_dict()

    top_ape_items = df.copy()
    top_ape_items["APE"] = top_ape_items["APE"].fillna(0)
    top_ape_items = top_ape_items.sort_values("APE", ascending=False).head(20)

    result["TopErrorItems"] = top_ape_items[[
        "ITEMNUM", "Warehouse", "DemandType", "ActivityLevel",
        "ActualDemand", "PredictedDemand", "APE",
        "ForecastScore", "CoverageGap", "Covered"
    ]].to_dict(orient="records")

    return result


