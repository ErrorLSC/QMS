import pandas as pd

class RouteSelector:
    def __init__(
        self,
        iim_df :pd.DataFrame = None,
        stats_df: pd.DataFrame = None,
        vendor_df: pd.DataFrame = None,
        pref_df: pd.DataFrame = None,
        prepare_df: pd.DataFrame = None,
        iwi_df: pd.DataFrame = None
    ):
        self.stats_df = stats_df
        self.vendor_df = vendor_df
        self.pref_df = pref_df
        self.iim_df = iim_df
        self.prepare_df = prepare_df
        self.iwi_df = iwi_df

    def get_candidate_routes(
        self,
        ITEMNUM: str,
        Warehouse: str,
        confidence_threshold: float = 0.1,
        leadtime_field: str = "Q60",
        stats_df: pd.DataFrame = None,
        vendor_df: pd.DataFrame = None,
        pref_df: pd.DataFrame = None,
        iim_df: pd.DataFrame = None,
        enable_vendor_path: bool = True
    ) -> pd.DataFrame:
        stats = stats_df if stats_df is not None else self.stats_df
        vendor = vendor_df if vendor_df is not None else self.vendor_df
        pref = pref_df if pref_df is not None else self.pref_df
        iim = iim_df if iim_df is not None else self.iim_df

        if any(x is None for x in [stats, vendor, pref, iim]):
            raise ValueError("RouteSelector 缺少必要的 DataFrame 输入")

        leadtime_field = leadtime_field + "TransportLeadTime"
        if leadtime_field not in stats.columns:
            raise ValueError(f"leadtime_field {leadtime_field} 不存在于 stats_df 列中")

        # Step 1a: 从 TransportPreference 提取路径
        df_pref = pref[
            (pref["ITEMNUM"] == ITEMNUM) &
            (pref["Warehouse"] == Warehouse) &
            (pref["Confidence"] >= confidence_threshold)
        ][["VendorCode", "TransportMode"]].drop_duplicates()

        # Step 1b: 对历史出现的 Vendor 补全所有运输方式
        if not df_pref.empty and enable_vendor_path:
            vendors_in_pref = df_pref["VendorCode"].unique()

            stats_paths = stats[
                (stats["VendorCode"].isin(vendors_in_pref)) &
                (stats["Warehouse"] == Warehouse)
            ][["VendorCode", "TransportMode"]].drop_duplicates()

            vm_paths = vendor[
                (vendor["VendorCode"].isin(vendors_in_pref)) &
                (vendor["IS_ACTIVE"] == "Y")
            ][["VendorCode", "TransportMode"]].drop_duplicates()

            supplemental_paths = pd.concat([stats_paths, vm_paths], ignore_index=True).drop_duplicates()
            df_pref = pd.concat([df_pref, supplemental_paths], ignore_index=True).drop_duplicates()

        # Step 2: 补全默认供应商所有路径（不论 pref 是否为空）
        row = iim[iim["ITEMNUM"] == ITEMNUM]
        if not row.empty and enable_vendor_path:
            default_vendor = row.iloc[0]["IVEND"]

            stats_paths = stats[
                (stats["VendorCode"] == default_vendor) &
                (stats["Warehouse"] == Warehouse)
            ][["VendorCode", "TransportMode"]].drop_duplicates()

            vm_paths = vendor[
                (vendor["VendorCode"] == default_vendor) &
                (vendor["IS_ACTIVE"] == "Y")
            ][["VendorCode", "TransportMode"]].drop_duplicates()

            default_paths = pd.concat([stats_paths, vm_paths], ignore_index=True).drop_duplicates()
            df_pref = pd.concat([df_pref, default_paths], ignore_index=True).drop_duplicates()

        # Step 3: 执行主路径合并与过滤
        if df_pref.empty:
            return pd.DataFrame()

        df_pref["Warehouse"] = Warehouse

        merged = pd.merge(df_pref, stats, how="left", on=["VendorCode", "TransportMode", "Warehouse"])
        merged = pd.merge(
            merged,
            vendor[["VendorCode", "TransportMode", "IS_ACTIVE", "TransportLeadTimeDays"]],
            how="left",
            on=["VendorCode", "TransportMode"]
        )

        merged = merged[merged["IS_ACTIVE"] == "Y"]

        result = merged[merged[leadtime_field].notna()].copy()
        fallback = merged[merged[leadtime_field].isna()].copy()

        result["FromStats"] = True
        result["LeadTime"] = result[leadtime_field].astype(int)
        result["CostPerKg"] = result["CostPerKg"].fillna(0.0)

        fallback["FromStats"] = False
        fallback["LeadTime"] = fallback["TransportLeadTimeDays"].fillna(999).astype(int)
        fallback["CostPerKg"] = 99999

        df_result = pd.concat([result, fallback], ignore_index=True)
        df_result["ITEMNUM"] = ITEMNUM

        return df_result[[
            "ITEMNUM", "Warehouse", "VendorCode", "TransportMode",
            "FromStats", "LeadTime", "CostPerKg"
        ]]
    
    def enrich_with_prepare_time(
        self,
        df_routes: pd.DataFrame,
        prepare_df: pd.DataFrame,
        iwi_df: pd.DataFrame,
        preparetime_field: str = "Q60"
    ) -> pd.DataFrame:
        df = df_routes.copy()
        prepare_df = prepare_df if prepare_df is not None else self.prepare_df
        iwi_df = iwi_df if iwi_df is not None else self.iwi_df

        preparetime_field = preparetime_field + "PrepDays"
        if preparetime_field not in prepare_df.columns:
            raise ValueError(f"指定的 leadtime 字段 {preparetime_field} 不存在于 prepare_df 中")
        
        prepare_agg = (
            prepare_df
            .groupby(["ITEMNUM", "VendorCode"])[preparetime_field]
            .max()
            .reset_index()
            .rename(columns={preparetime_field: "PrepareTime"})
            )   
        # Step 1: merge ITEM_PREPARE_LT_STATS
        df = pd.merge(
            df,
            prepare_agg,
            how="left",
            on=["ITEMNUM", "VendorCode"]
        )
        df["PrepareFromStats"] = df["PrepareTime"].notna()


        # Step 2: fallback to WLEAD if PrepareTime missing
        fallback_rows = df[~df["PrepareFromStats"]].copy()

        if not fallback_rows.empty:
            iwi_sub = iwi_df[["ITEMNUM", "Warehouse", "WLEAD"]].drop_duplicates()
            fallback_rows = pd.merge(
                fallback_rows,
                iwi_sub,
                how="left",
                on=["ITEMNUM", "Warehouse"]
            )

        # WLEAD fallback - PrepareTime = WLEAD - LeadTime
            fallback_rows["PrepareTime"] = (
                fallback_rows["WLEAD"]
            ).where(fallback_rows["WLEAD"].notna())

            fallback_rows["PrepareTime"] = fallback_rows["PrepareTime"].clip(lower=0)
            fallback_rows["PrepareFromStats"] = False

            fallback_result = fallback_rows[[
                "ITEMNUM", "Warehouse", "VendorCode", "TransportMode", "PrepareTime", "PrepareFromStats"
            ]].copy()
        # print(df)
            df = pd.merge(
                df,
                fallback_result,
                how="left",
                on=["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
                suffixes=("", "_fb")
            )

            df["PrepareTime"] = df["PrepareTime"].combine_first(df["PrepareTime_fb"])
            df["PrepareFromStats"] = df["PrepareFromStats"].combine_first(df["PrepareFromStats_fb"])
            df = df.drop(columns=["PrepareTime_fb", "PrepareFromStats_fb"])
        # Step 3: compute TotalLeadTime
        df["PrepareTime"] = df["PrepareTime"].fillna(0).astype(int)
        df["TotalLeadTime"] = df["PrepareTime"] + df["LeadTime"]

        return df
    
    def select(
        self,
        ITEMNUM: str,
        Warehouse: str,
        leadtime_field: str = "Q60",
        preparetime_field: str = "Q60",
        confidence_threshold: float = 0.1,
        enable_vendor_path: bool = True,
        stats_df: pd.DataFrame = None,
        vendor_df: pd.DataFrame = None,
        pref_df: pd.DataFrame = None,
        iim_df: pd.DataFrame = None,
        prepare_df: pd.DataFrame = None,
        iwi_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        # Step 1: 候选路径
        candidates = self.get_candidate_routes(
            ITEMNUM=ITEMNUM,
            Warehouse=Warehouse,
            confidence_threshold=confidence_threshold,
            leadtime_field=leadtime_field,
            stats_df=stats_df,
            vendor_df=vendor_df,
            pref_df=pref_df,
            iim_df=iim_df,
            enable_vendor_path=enable_vendor_path
        )

        if candidates.empty:
            return pd.DataFrame()

        # Step 2: 准备期补全
        enriched = self.enrich_with_prepare_time(
            df_routes=candidates,
            prepare_df=prepare_df,
            iwi_df=iwi_df,
            preparetime_field=preparetime_field
        )

        return enriched