import numpy as np
import pandas as pd
from scipy.stats import norm
from qms_core.core.common.params.ParasCenter import ParasCenter
from qms_core.core.common.params.SafetyStockParams import ServiceLevelParamsSchema

def croston_sba_forecast(demand_series, alpha=0.1):
    demand_series = np.asarray(demand_series)
    if len(demand_series) == 0:
        return {"forecast": 0, "z_hat": 0, "p_hat": 1}

    z, p = [], []
    intervals = 0
    for val in demand_series:
        if val > 0:
            z.append(val)
            p.append(intervals + 1)
            intervals = 0
        else:
            intervals += 1

    if not z:
        return {"forecast": 0, "z_hat": 0, "p_hat": 1}

    z_hat, p_hat = z[0], p[0]
    for i in range(1, len(z)):
        z_hat = alpha * z[i] + (1 - alpha) * z_hat
        p_hat = alpha * p[i] + (1 - alpha) * p_hat

    forecast = (1 - alpha / 2) * (z_hat / p_hat)
    return {"forecast": forecast, "z_hat": z_hat, "p_hat": p_hat,"z_list": z}

def croston_safety_stock(z_list, p_hat, lead_time_weeks, service_level):
    if len(z_list) <= 1:
        return 0
    z_std = np.std(z_list, ddof=1)
    Z = norm.ppf(service_level)

    return Z * z_std * np.sqrt(lead_time_weeks / p_hat)

def weighted_bootstrap_quantile(data, weights=None, quantile=0.9, n_samples=1000):
    if len(data) == 0:
        return 0
    if weights is None:
        weights = np.ones_like(data)
    norm_weights = weights / np.sum(weights)
    samples = np.random.choice(data, size=n_samples, replace=True, p=norm_weights)
    return np.quantile(samples, quantile)

def winsorize_series(s, lower=0.01, upper=0.99):
    q_low = s.quantile(lower)
    q_high = s.quantile(upper)
    return s.clip(lower=q_low, upper=q_high)

def singleside_winsorize_series(s, upper=0.95):
    q_high = s.quantile(upper)
    return s.clip(upper=q_high)

def preprocess_demand(df: pd.DataFrame, max_date=None, params=None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["TotalDemand", "WeeksAgo", "Weight"])

    df = df.copy()
    if max_date is None:
        max_date = df["YearWeek"].max()

    df["WeeksAgo"] = (max_date - df["YearWeek"]).dt.days // 7

    if params is None:
        decay = 0.9
        upper = 0.95
    else:
        decay = getattr(params, "decay_factor", 0.9)
        upper = getattr(params, "winsor_upper", 0.95)

    df["Weight"] = decay ** df["WeeksAgo"]
    df["TotalDemand"] = singleside_winsorize_series(df["TotalDemand"], upper=upper)

    return df[["TotalDemand", "WeeksAgo", "Weight"]]

def score_service_level(iscst, wlead, cv,params: ServiceLevelParamsSchema = None) -> float:
    if params is None:
        params = ParasCenter().service_level_params
    # print(wlead,cv,iscst)
    max_leadtime = params.max_leadtime
    min_servicelevel = params.min_servicelevel
    max_servicelevel = params.max_servicelevel

    # 健壮性判断：确保 min < max 且 max 不超过 0.99
    if max_servicelevel <= min_servicelevel or max_servicelevel > 0.99:
        max_servicelevel = 0.99
        min_servicelevel = 0.85

    if pd.isna(iscst) or pd.isna(wlead) or pd.isna(cv) or not np.isfinite(cv):
        return min_servicelevel

    lead_risk_score = np.log1p(wlead) / np.log1p(max_leadtime)
    value_penalty = np.log1p(iscst)

    raw_score = (cv * lead_risk_score) / value_penalty

    if not np.isfinite(raw_score):
        return min_servicelevel
    
    # 使用 Sigmoid 映射到 min_servicelevel ~ max_servicelevel 区间
    sigmoid = 1 / (1 + np.exp(-raw_score))
    mapped_score = min_servicelevel + (max_servicelevel - min_servicelevel) * sigmoid
    return round(mapped_score, 4)

def to_yearweek(date) -> str:
    """
    将 datetime 日期转换为 'YYYY-XXW' 格式（系统标准周格式）

    示例:
        2025-06-11 → '2025-24W'
    """
    if pd.isnull(date):
        return None
    if isinstance(date, str):
        date = pd.to_datetime(date)

    year, week, _ = date.isocalendar()  # week: ISO周数，1-53
    return f"{year}-{week:02d}W"

def to_yearweek_int(date) -> int:
    """
    将日期转为整数形式 YearWeek，如 2025-06-11 → 202524
    """
    if pd.isnull(date):
        return None
    if isinstance(date, str):
        date = pd.to_datetime(date)

    year, week, _ = date.isocalendar()
    return year * 100 + week  # 如 2025 * 100 + 24 = 202524

def convert_column_to_yearweek(df: pd.DataFrame, date_col: str, target_col: str = "YearWeek") -> pd.DataFrame:
    df[target_col] = df[date_col].apply(to_yearweek)
    return df

def get_next_n_yearweeks(start_date: pd.Timestamp, n: int) -> list[int]:
    yearweeks = []
    date = start_date
    for _ in range(n):
        yw = to_yearweek_int(date)
        yearweeks.append(yw)
        date += pd.Timedelta(weeks=1)
    return yearweeks