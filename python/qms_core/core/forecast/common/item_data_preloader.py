from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd
from typing import Optional
from qms_core.core.item.item import Item

class ItemDataPreloader:
    def __init__(self, config, items: list[Item]):
        self.config = config
        self.items = items
        self.replacing_map = self._build_replacing_map()

    def _build_replacing_map(self, type_filter='1') -> dict:
        """
        从 DPS 表构建替代关系映射：{子件: [ {'parent': ..., 'using_existing': ...}, ... ]}
        """
        from qms_core.infrastructure.db.models import DPS
        df = fetch_orm_data(self.config, DPS)
        df = df[df["TYPE"] == type_filter]

        if df.empty:
            return {}

        result = {}
        for _, row in df.iterrows():
            child = row["ITEMNUM_CHILD"]
            parent = row["ITEMNUM_PARENT"]
            using_existing = (row.get("USING_EXISTING") or 'N') == 'Y'
            result.setdefault(child, []).append({
                "parent": parent,
                "using_existing": using_existing
            })

        return result

    def load_inventory_info(self) -> dict:
        """
        加载主件及其替代件的库存数据（可用库存和在途库存），自动合并替代件库存。
        返回字段：ITEMNUM, Warehouse, AvailableStock, IntransitStock
        """
        from qms_core.infrastructure.db.models import STKOHAvail

        df = fetch_orm_data(self.config, STKOHAvail)

        if df.empty:
            return pd.DataFrame(columns=["ITEMNUM", "Warehouse", "AvailableStock", "IntransitStock"])

        df["AVAIL"] = df["AVAIL"].fillna(0)
        df["IONOD"] = df["IONOD"].fillna(0)

        # 聚合
        grouped = df.groupby(["ITEMNUM", "Warehouse"]).agg({
            "AVAIL": "sum",
            "IONOD": "sum"
        }).reset_index()

        raw_dict = {
            (row["ITEMNUM"], row["Warehouse"]): {
                "AVAIL": row["AVAIL"],
                "IONOD": row["IONOD"]
            }
            for _, row in grouped.iterrows()
        }

        # 合并替代件库存
        results = []
        for item in self.items:
            key = (item.itemnum, item.warehouse)
            total_avail = raw_dict.get(key, {}).get("AVAIL", 0.0)
            total_ionod = raw_dict.get(key, {}).get("IONOD", 0.0)

            for rel in self.replacing_map.get(item.itemnum, []):
                if rel.get("using_existing"):
                    pkey = (rel["parent"], item.warehouse)
                    total_avail += raw_dict.get(pkey, {}).get("AVAIL", 0.0)
                    total_ionod += raw_dict.get(pkey, {}).get("IONOD", 0.0)

            results.append({
                "ITEMNUM": item.itemnum,
                "Warehouse": item.warehouse,
                "AvailableStock": round(total_avail, 2),
                "IntransitStock": round(total_ionod, 2)
            })

        return {
            (row["ITEMNUM"], row["Warehouse"]): {
            "AvailableStock": row["AvailableStock"],
            "IntransitStock": row["IntransitStock"]
            }
            for row in results
            }
    
    def load_demand_history(self, max_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        一次性加载所有 item（含其替代件父项）的需求历史。
        将母件的需求历史映射为子件。
        返回字段：ITEMNUM, Warehouse, YearWeek, TotalDemand
        """
        from qms_core.infrastructure.db.models import DemandHistoryWeekly

        if max_date is None:
            max_date = pd.Timestamp.today()

        # 构造主件 + 替代件父项的 key 集合
        all_keys = {(item.itemnum, item.warehouse) for item in self.items}
        parent_to_child = {}  # 新增：反向映射

        for item in self.items:
            for rel in self.replacing_map.get(item.itemnum, []):
                if rel.get("using_existing"):
                    key = (rel["parent"], item.warehouse)
                    all_keys.add(key)
                    parent_to_child[key] = item.itemnum  # 将母件映射为当前子件

        itemnums = list(set(k[0] for k in all_keys))
        warehouses = list(set(k[1] for k in all_keys))

        # 批量读取并筛选
        df = fetch_orm_data(self.config, DemandHistoryWeekly)
        df = df[df["ITEMNUM"].isin(itemnums) & df["Warehouse"].isin(warehouses)]

        if df.empty:
            return pd.DataFrame(columns=["ITEMNUM", "Warehouse", "YearWeek", "TotalDemand"])

        df["TotalDemand"] = df["TotalDemand"].fillna(0)
        df["YearWeek"] = pd.to_datetime(df["YearWeek"] + "-1", format="%G-W%V-%u")
        df = df[df["YearWeek"] <= max_date]

        # 替换母件 → 子件
        df["ITEMNUM"] = df.apply(
            lambda row: parent_to_child.get((row["ITEMNUM"], row["Warehouse"]), row["ITEMNUM"]),
            axis=1
        )

        # 合并母子件需求
        df = (
            df.groupby(["ITEMNUM", "Warehouse", "YearWeek"], as_index=False)
            .agg({"TotalDemand": "sum"})
        )

        return df
    
    def load_item_master_info(self) -> dict:
        """
        一次性读取所有 item 的主数据（IIM + IWI），
        返回 {(ITEMNUM, Warehouse): {...}} 格式字典。
        """
        from qms_core.infrastructure.db.models import IIM, IWI

        keys = {(item.itemnum, item.warehouse) for item in self.items}
        itemnums = list(set(k[0] for k in keys))
        warehouses = list(set(k[1] for k in keys))

        # IIM：全局主数据（按 ITEMNUM）
        iim_df = fetch_orm_data(self.config, IIM)
        iim_df = iim_df[iim_df["ITEMNUM"].isin(itemnums)]
        iim_dict = {row["ITEMNUM"]: row for _, row in iim_df.iterrows()}

        # IWI：库别主数据（按 ITEMNUM + Warehouse）
        iwi_df = fetch_orm_data(self.config, IWI)
        iwi_df = iwi_df[
            iwi_df["ITEMNUM"].isin(itemnums) &
            iwi_df["Warehouse"].isin(warehouses)
        ]
        iwi_dict = {
            (row["ITEMNUM"], row["Warehouse"]): row for _, row in iwi_df.iterrows()
        }

        # 合并为最终结构
        result = {}
        for itemnum, warehouse in keys:
            key = (itemnum, warehouse)
            iim = iim_dict.get(itemnum, {})
            iwi = iwi_dict.get(key, {})

            result[key] = {
                "item_type": iim.get("IITYP"),
                "idesc": iim.get("IDESC"),
                "vendor_code": iim.get("IVEND"),
                "vendor_name": iim.get("VNDNAM"),
                "cost": iim.get("ISCST"),
                "plc": iim.get("CXPPLC"),
                "pgc": iim.get("PGC"),
                "gac": iim.get("GAC"),
                "rpflag": iim.get("RPFLAG"),
                "lot_size": iwi.get("WLOTS"),
                "moq": iwi.get("MOQ"),
                "lead_time": iwi.get("WLEAD"),
                "safety_stock": iwi.get("WSAFE"),
                "default_location": iwi.get("WLOC"),
            }

        return result
    
    def load_demand_type(self) -> dict:
        """
        读取 ITEM_DEMAND_TYPE 表中指定 items 的分类结果。
        返回 {(ITEMNUM, Warehouse): {...}} 的字典。
        """
        from qms_core.infrastructure.db.models import DemandType

        keys = {(item.itemnum, item.warehouse) for item in self.items}
        itemnums = [k[0] for k in keys]
        warehouses = [k[1] for k in keys]

        df = fetch_orm_data(self.config, DemandType)
        df = df[df["ITEMNUM"].isin(itemnums) & df["Warehouse"].isin(warehouses)]

        result = {}
        for _, row in df.iterrows():
            key = (row["ITEMNUM"], row["Warehouse"])
            result[key] = {
                "DemandType": row["DemandType"],
                "ActivityLevel": row["ActivityLevel"],
                "WeeksWithDemand": row.get("WeeksWithDemand"),
                "ZeroRatio": row.get("ZeroRatio"),
                "CV": row.get("CV"),
                "TrendSlope": row.get("TrendSlope"),
                "SeasonalStrength": row.get("SeasonalStrength"),
            }

        return result
    
    def load_forecast_series(self) -> dict:
        """
        预加载指定 items 的预测序列（含 JSON、Series、模型信息等），返回字典。
        """
        import json
        from qms_core.infrastructure.db.models import ItemForecastRecord

        keys = {(item.itemnum, item.warehouse) for item in self.items}
        itemnums = [k[0] for k in keys]
        warehouses = [k[1] for k in keys]

        df = fetch_orm_data(self.config, ItemForecastRecord)
        df = df[df["ITEMNUM"].isin(itemnums) & df["Warehouse"].isin(warehouses)]

        result = {}
        for _, row in df.iterrows():
            key = (row["ITEMNUM"], row["Warehouse"])
            try:
                series = pd.Series(json.loads(row["ForecastSeriesJSON"] or "[]"))
            except Exception:
                series = pd.Series(dtype=float)

            result[key] = {
                "ForecastSeries": series,
                "ForecastSeriesJSON": row.get("ForecastSeriesJSON", ""),
                "Forecast_monthly": row.get("Forecast_monthly", float(series[:4].sum())),
                "ForecastModel": row.get("ForecastModel", "")
            }

        return result
    
    def load_safety_stock(self) -> dict:
        """
        加载 ITEM_SAFETY 表的安全库存结果，返回 {(ITEMNUM, Warehouse): {...}} 字典。
        """
        from qms_core.infrastructure.db.models import ItemSafetyRecord

        keys = {(item.itemnum, item.warehouse) for item in self.items}
        itemnums = [k[0] for k in keys]
        warehouses = [k[1] for k in keys]

        df = fetch_orm_data(self.config, ItemSafetyRecord)
        df = df[df["ITEMNUM"].isin(itemnums) & df["Warehouse"].isin(warehouses)]

        result = {}
        for _, row in df.iterrows():
            key = (row["ITEMNUM"], row["Warehouse"])
            result[key] = {
                "RecommendedServiceLevel": row.get("RecommendedServiceLevel"),
                "DynamicSafetyStock": row.get("DynamicSafetyStock"),
                "FinalSafetyStock": row.get("FinalSafetyStock"),
                "SafetyCalcDate": row.get("SafetyCalcDate")
            }

        return result
    
    def load_smart_lead_time(self) -> dict:
        """
        加载 ITEM_SMART_LEADTIME 表的动态交期结果，返回 {(ITEMNUM, Warehouse): {...}} 字典。
        """
        from qms_core.infrastructure.db.models import ItemSmartLeadtime,IIM,ItemTransportPreference

        keys = {(item.itemnum, item.warehouse) for item in self.items}
        itemnums = [k[0] for k in keys]
        warehouses = [k[1] for k in keys]

        # 全量拉取 (可以加 where 加速)
        df_slt = fetch_orm_data(self.config, ItemSmartLeadtime)
        df_master = fetch_orm_data(self.config, IIM)
        df_pref = fetch_orm_data(self.config, ItemTransportPreference)

        # 过滤 SmartLeadtime 记录
        df_slt = df_slt[df_slt["ITEMNUM"].isin(itemnums) & df_slt["Warehouse"].isin(warehouses)]
        df_pref = df_pref[df_pref["ITEMNUM"].isin(itemnums) & df_pref["Warehouse"].isin(warehouses)]
        df_master = df_master[df_master["ITEMNUM"].isin(itemnums)]

        # 合并 IIM 取 VendorCode
        df_master = df_master.rename(columns={"IVEND": "VendorCode"})
        df_slt = df_slt.merge(df_master[["ITEMNUM", "VendorCode"]], on=["ITEMNUM"], how="inner", suffixes=('', '_Master'))

        # 只保留 VendorCode 匹配的记录
        df_slt = df_slt[df_slt["VendorCode"] == df_slt["VendorCode_Master"]]

        # 合并 TransportPreference 取 Rank
        df_slt = df_slt.merge(
            df_pref[["ITEMNUM", "Warehouse", "VendorCode", "TransportMode", "Rank"]],
            on=["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
            how="left"
        )

        # 按 ITEMNUM + Warehouse + Rank 排序 (Rank 为 NaN 视为最大)
        df_slt["Rank"] = df_slt["Rank"].fillna(9999)
        df_slt = df_slt.sort_values(["ITEMNUM", "Warehouse", "Rank"])

        # 分组取每组第一条（Rank 最小）
        df_slt = df_slt.groupby(["ITEMNUM", "Warehouse"]).first().reset_index()

        # 输出结果 dict
        result = {}
        for _, row in df_slt.iterrows():
            key = (row["ITEMNUM"], row["Warehouse"])
            result[key] = {
                "VendorCode": row.get("VendorCode"),
                "TransportMode": row.get("TransportMode"),
                "Source": row.get("Source"),
                "Q60LeadTime": row.get("Q60LeadTime"),
                "Q60PrepDays": row.get("Q60PrepDays"),
                "Q60TransportLeadTime": row.get("Q60TransportLeadTime")
            }
        return result