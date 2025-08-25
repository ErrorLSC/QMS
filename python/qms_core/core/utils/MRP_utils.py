import pandas as pd
import numpy as np
from qms_core.infrastructure.db.models import STKOHAvail,DPS,DemandHistoryWeekly,IIM,IWI,DemandType,ItemForecastRecord,ItemSafetyRecord

def build_stkoh_avail_snapshot(session) -> pd.DataFrame:
    """
    读取 STKOH_AVAIL 全表，返回包含 ITEMNUM、Warehouse、AVAIL、IONOD 的 DataFrame。
    """
    results = session.query(
        STKOHAvail.ITEMNUM,
        STKOHAvail.Warehouse,
        STKOHAvail.AVAIL,
        STKOHAvail.IONOD
    ).all()

    data = [
        {
            "ITEMNUM": row.ITEMNUM,
            "Warehouse": row.Warehouse,
            "AVAIL": row.AVAIL or 0,
            "IONOD": row.IONOD or 0,
        }
        for row in results
    ]

    return pd.DataFrame(data)

def build_replacing_map(session, type_filter='1') -> dict:
    """
    从 DPS 表构建替代关系映射：{子件: [ {'parent': ..., 'using_existing': ...}, ... ]}
    """
    rows = (
        session.query(DPS)
        .filter(DPS.TYPE == type_filter)
        .all()
    )

    if not rows:
        return {}

    replacing_map = {}
    for row in rows:
        child = row.ITEMNUM_CHILD
        parent = row.ITEMNUM_PARENT
        using_existing = (row.USING_EXISTING or 'N') == 'Y'
        replacing_map.setdefault(child, []).append({
            'parent': parent,
            'using_existing': using_existing
        })

    return replacing_map

def preload_demand_history_for_items(session, items: list, replacing_map: dict, max_date=None) -> pd.DataFrame:
    """
    一次性加载所有 item（含其父项）的需求历史，返回完整 DataFrame。
    """
    if max_date is None:
        max_date = pd.Timestamp.today()

    # 收集所有主件和其 parent（按需合并）
    all_keys = set((item.itemnum, item.warehouse) for item in items)

    # 加入替代件对应的 parent
    for item in items:
        replacements = replacing_map.get(item.itemnum, [])
        for rel in replacements:
            if rel.get("using_existing"):
                all_keys.add((rel["parent"], item.warehouse))

    # 查询 DEMANDHISTORY_WEEKLY
    itemnums = list(set(k[0] for k in all_keys))
    lwhs_list = list(set(k[1] for k in all_keys))

    rows = (
        session.query(DemandHistoryWeekly)
        .filter(DemandHistoryWeekly.ITEMNUM.in_(itemnums))
        .filter(DemandHistoryWeekly.Warehouse.in_(lwhs_list))
        .all()
    )

    df = pd.DataFrame([{
        "ITEMNUM": r.ITEMNUM,
        "Warehouse": r.Warehouse,
        "YearWeek": r.YearWeek,
        "TotalDemand": r.TotalDemand or 0
    } for r in rows])

    if df.empty:
        return pd.DataFrame(columns=["ITEMNUM", "Warehouse", "YearWeek", "TotalDemand"])

    df["YearWeek"] = pd.to_datetime(df["YearWeek"] + "-1", format="%G-W%V-%u")  # 转成日期
    df = df[df["YearWeek"] <= max_date]  # 按需过滤未来数据

    return df

def preload_item_master_info(session, items: list) -> dict:
    """
    一次性读取所有 item 的主数据（来自 IIM + IWI），返回 {(ITEMNUM, Warehouse): {字段}} 的字典。
    """
    keys = set((item.itemnum, item.warehouse) for item in items)
    itemnums = list(set(k[0] for k in keys))
    warehouses = list(set(k[1] for k in keys))

    # 主数据（IIM）
    iim_rows = (
        session.query(IIM)
        .filter(IIM.ITEMNUM.in_(itemnums))
        .all()
    )
    iim_dict = {
        row.ITEMNUM: row for row in iim_rows
    }

    # 仓库数据（IWI）
    iwi_rows = (
        session.query(IWI)
        .filter(IWI.ITEMNUM.in_(itemnums))
        .filter(IWI.Warehouse.in_(warehouses))
        .all()
    )
    iwi_dict = {
        (row.ITEMNUM, row.Warehouse): row for row in iwi_rows
    }

    # 合并成最终字典
    result = {}
    for itemnum, warehouse in keys:
        key = (itemnum, warehouse)
        iim = iim_dict.get(itemnum)
        iwi = iwi_dict.get(key)
        result[key] = {
            "item_type": iim.IITYP if iim else None,
            "idesc": iim.IDESC if iim else None,
            "vendor_code": iim.IVEND if iim else None,
            "vendor_name": iim.VNDNAM if iim else None,
            "cost": iim.ISCST if iim else None,
            "plc": iim.CXPPLC if iim else None,
            "pgc": iim.PGC if iim else None,
            "gac": iim.GAC if iim else None,
            "rpflag": iim.RPFLAG if iim else None,
            "lot_size": iwi.WLOTS if iwi else None,
            "moq": iwi.MOQ if iwi else None,
            "lead_time": iwi.WLEAD if iwi else None,
            "safety_stock": iwi.WSAFE if iwi else None,
            "default_location": iwi.WLOC if iwi else None,
        }

    return result

def preload_demand_type_for_items(session, items: list) -> dict:
    """
    一次性预加载所有 item 的分类结果，返回 {(ITEMNUM, Warehouse): {...}} 的字典。
    """
    keys = set((item.itemnum, item.warehouse) for item in items)

    itemnums = [k[0] for k in keys]
    lwhs_list = [k[1] for k in keys]

    rows = (
        session.query(DemandType)
        .filter(DemandType.ITEMNUM.in_(itemnums))
        .filter(DemandType.Warehouse.in_(lwhs_list))
        .all()
    )

    result = {}
    for row in rows:
        key = (row.ITEMNUM, row.LWHS)
        result[key] = {
            "DemandType": row.DemandType,
            "ActivityLevel": row.ActivityLevel,
            "WeeksWithDemand": row.WeeksWithDemand,
            "ZeroRatio": row.ZeroRatio,
            "CV": row.CV,
            "TrendSlope": row.TrendSlope,
            "SeasonalStrength": row.SeasonalStrength,
        }

    return result

def preload_forecast_series_for_items(session, items: list) -> dict:
    """
    一次性预加载所有 item 的预测序列，返回 {(ITEMNUM, Warehouse): {...}} 字典，
    包含 ForecastSeries、ForecastSeriesJSON、Forecast_monthly、ForecastModel 等字段。
    """
    import json

    keys = set((item.itemnum, item.warehouse) for item in items)
    itemnums = [k[0] for k in keys]
    warehouses = [k[1] for k in keys]

    rows = (
        session.query(ItemForecastRecord)
        .filter(ItemForecastRecord.ITEMNUM.in_(itemnums))
        .filter(ItemForecastRecord.Warehouse.in_(warehouses))
        .all()
    )

    result = {}
    for row in rows:
        key = (row.ITEMNUM, row.Warehouse)

        # 安全解析序列
        try:
            series = pd.Series(json.loads(row.ForecastSeriesJSON or "[]"))
        except Exception:
            series = pd.Series(dtype=float)

        result[key] = {
            "ForecastSeries": series,
            "ForecastSeriesJSON": row.ForecastSeriesJSON or "",
            "Forecast_monthly": row.Forecast_monthly or float(series[:4].sum()),
            "ForecastModel": row.ForecastModel or ""
        }

    return result

def preload_item_inventory_info(session, items: list, replacing_map: dict = None) -> dict:
    """
    一次性加载所有 item（含其替代件）的可用库存数据，返回 { (ITEMNUM, Warehouse): {"AVAIL": x, "IONOD": y} } 格式字典。
    替代件库存会被自动汇总到主件上。
    """
    if replacing_map is None:
        replacing_map = {}

    # 1️⃣ 收集所有主件和可能需合并替代件的 ITEMNUM + Warehouse
    all_keys = set((item.itemnum, item.warehouse) for item in items)

    # 加入可用替代件（using_existing=True）的父件记录
    for item in items:
        replacements = replacing_map.get(item.itemnum, [])
        for rel in replacements:
            if rel.get("using_existing"):
                all_keys.add((rel["parent"], item.warehouse))

    itemnums = list(set(k[0] for k in all_keys))
    whs = list(set(k[1] for k in all_keys))

    # 2️⃣ 查询 STKOH_IWI_AVAIL 表
    rows = (
        session.query(STKOHAvail)
        .filter(STKOHAvail.ITEMNUM.in_(itemnums))
        .filter(STKOHAvail.Warehouse.in_(whs))
        .all()
    )

    df = pd.DataFrame([{
        "ITEMNUM": r.ITEMNUM,
        "Warehouse": r.Warehouse,
        "AVAIL": r.AVAIL or 0,
        "IONOD": r.IONOD or 0
    } for r in rows])

    if df.empty:
        return {}

    # 3️⃣ 聚合数据
    grouped = df.groupby(["ITEMNUM", "Warehouse"]).agg({
        "AVAIL": "sum",
        "IONOD": "sum"
    }).reset_index()

    # 4️⃣ 构造原始字典
    raw_dict = {
        (row["ITEMNUM"], row["Warehouse"]): {
            "AVAIL": row["AVAIL"],
            "IONOD": row["IONOD"]
        }
        for _, row in grouped.iterrows()
    }

    # 5️⃣ 构造合并后库存字典（主件 + 替代件）
    result_dict = {}

    for item in items:
        key = (item.itemnum, item.warehouse)
        total_avail = 0.0
        total_ionod = 0.0

        # 加入主件库存
        main_val = raw_dict.get(key, {})
        total_avail += main_val.get("AVAIL", 0.0)
        total_ionod += main_val.get("IONOD", 0.0)

        # 加入可合并替代件库存
        replacements = replacing_map.get(item.itemnum, [])
        for rel in replacements:
            if rel.get("using_existing"):
                parent_key = (rel["parent"], item.warehouse)
                parent_val = raw_dict.get(parent_key, {})
                total_avail += parent_val.get("AVAIL", 0.0)
                total_ionod += parent_val.get("IONOD", 0.0)

        result_dict[key] = {
            "AvailableStock": round(total_avail, 2),
            "IntransitStock": round(total_ionod, 2)
        }

    return result_dict

def preload_safety_stock_for_items(session, items: list) -> dict:
    """
    一次性加载 ITEM_SAFETY 表中所有指定 item 的安全库存结果。
    返回 {(ITEMNUM, Warehouse): {...}} 格式字典，供 item.safetystock.set_values() 使用。
    """
    keys = set((item.itemnum, item.warehouse) for item in items)
    itemnums = list(set(k[0] for k in keys))
    whs = list(set(k[1] for k in keys))

    rows = (
        session.query(ItemSafetyRecord)
        .filter(ItemSafetyRecord.ITEMNUM.in_(itemnums))
        .filter(ItemSafetyRecord.Warehouse.in_(whs))
        .all()
    )

    result = {}
    for row in rows:
        key = (row.ITEMNUM, row.Warehouse)
        result[key] = {
            "RecommendedServiceLevel": row.RecommendedServiceLevel,
            "DynamicSafetyStock": row.DynamicSafetyStock,
            "FinalSafetyStock": row.FinalSafetyStock,
            "SafetyCalcDate": row.SafetyCalcDate
        }

    return result

def calculate_ema(series: pd.Series, alpha: float = 0.3) -> float:
    """
    对 pandas Series 执行指数移动平均（EMA）
    如果为空则返回 NaN
    """
    if series.empty:
        return np.nan
    return series.ewm(alpha=alpha, adjust=False).mean().iloc[-1]