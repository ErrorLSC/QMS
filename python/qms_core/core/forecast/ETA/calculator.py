import pandas as pd
import numpy as np
from datetime import timedelta
from qms_core.core.common.base_transformer import BaseTransformer
from qms_core.core.analysis.common.shipmode_assigner import TransportModePredictor
from qms_core.core.forecast.common.forecast_utils import to_yearweek
from qms_core.core.common.params.enums import TransportMode
from qms_core.core.utils.po_utils import generate_virtual_po_sublines
from typing import Optional

class ETATransformer(BaseTransformer):
    COLUMN_TYPE_MAP = {
        "ITEMNUM": str,
        "Warehouse": str,
        "PONUM": str,
        "POLINE": str,
        "VendorCode": str,
        "TransportMode": str,
        "InTransitQty": float,
        "ETA_Date": "datetime64[ns]",
        "ETA_Week": str,
        "ETA_Flag": str,
        "TransportTime": float,  # 也可用 int
        "OriginalTransportMode": str,
        "PredictedTransportMode": str,
        "Fallback_TransportUsed": str,
        "Fallback_TotalLeadUsed":str,
        "Comment":str,
        "BatchIndex":int, 
        "IsFinalBatch":str,
        "ETA_Overdue":str,
    }

    def __init__(self, lead_metric: str = "Q60",predictor: Optional[TransportModePredictor] = None):
        self.lead_metric = lead_metric
        self.predictor = predictor

    def prepare_and_route_intransit(self, data: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, dict, dict]:
        df_intransit = data["intransit"]
        df_smartleadtime = data["smart_leadtime"]
        df_batch_profile = data["batch_profile"]
        df_iwi = data.get("iwi")

        lead_col = self.lead_metric + "LeadTime"

        # 合并 SmartLeadtime
        df = df_intransit.merge(
            df_smartleadtime[["ITEMNUM", "Warehouse", "VendorCode", "TransportMode", lead_col]],
            on=["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
            how="left"
        )

        # fallback
        if df_iwi is not None:
            df = self._fill_missing_total_leadtime(df, lead_col, df_iwi)
        df["TransportMode"] = df["TransportMode"].fillna("DEFAULT")

        # 合并 BatchProfile
        df = df.merge(
            df_batch_profile[[
                "ITEMNUM", "Warehouse", "VendorCode", "TransportMode",
                "IsBatchProne", "PredictedBatchCount", "PredictedBatchQty",
                "PredictedBatchIntervalDays", "PredictedTailQtyRate"
            ]],
            on=["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
            how="left"
        )

        # 分流
        df_cases = self.route_eta_cases(df)

        # 构建尾批所需查找字典
        last_delivery_dict = self.build_last_delivery_lookup(data["delivery"])

        return df, df_cases, last_delivery_dict

    def transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        df_vendor_lt = data["vendor_transport_stat"]
        df_vendor_master = data["vendor_master"]
        df_behavior = data["delivery_behavior"]

        # 🔧 步骤 1：预处理并分流
        df_intransit, df_cases, last_delivery_dict = self.prepare_and_route_intransit(data)

        # 🔧 步骤 2：五类路径预测
        df_confirmed = self.transform_confirmed(df_cases["Confirmed"])
        df_shipped = self.transform_shipped(df_cases["Shipped"], df_vendor_lt, df_vendor_master)
        df_single = self.simulate_single_eta(df_cases["SingleDelivery"])
        df_tail = self.simulate_tail_eta(
            df_tail=df_cases["SplitInProgress"],
            df_behavior=df_behavior,
            last_delivery_dict=last_delivery_dict,
            df_full=df_intransit,
            df_vendor_lt=df_vendor_lt,
            df_vendor_master=df_vendor_master
        )
        df_batch = self.simulate_batch_eta(
            df=df_cases["LikelySplit"],
            df_vendor_lt=df_vendor_lt,
            df_vendor_master=df_vendor_master
        )

        # 🔧 步骤 3：合并
        df_result = pd.concat(
            [df_confirmed, df_shipped, df_single, df_tail, df_batch],
            ignore_index=True
        )

        # ✅ 步骤 4：补齐所有预期字段
        expected_cols = [
            "ITEMNUM", "Warehouse", "PONUM", "POLINE",
            "VendorCode", "TransportMode", "InTransitQty",
            "ETA_Date", "ETA_Week", "ETA_Flag", "Comment",
            "BatchIndex", "IsFinalBatch",
            "Fallback_TransportUsed", "Fallback_TotalLeadUsed",
        ]
        for col in expected_cols:
            if col not in df_result.columns:
                df_result[col] = None
        if "BatchIndex" in df_result.columns:
            df_result["BatchIndex"] = df_result["BatchIndex"].fillna(0).astype(int)

        this_week = to_yearweek(pd.Timestamp.today())
        df_result["ETA_Overdue"] = df_result["ETA_Week"] < this_week
        df_result = df_result.where(pd.notnull(df_result), None)
        # ✅ 步骤 5：字段类型统一（自定义方法）
        return self.enforce_column_types(df_result)
    
    def build_last_delivery_lookup(self, df_delivery: pd.DataFrame) -> dict[tuple[str, str], pd.Timestamp]:
        """
        从 delivery 表构建上次发货日期的 lookup 字典。
        key: (PONUM, BasePOLINE)
        value: 最后 InvoiceDate
        """
        df_valid = df_delivery[df_delivery["InvoiceDate"].notna()].copy()
        df_valid["BasePOLINE"] = df_valid["POLINE"].astype(str).str.split("-").str[0]

        grouped = (
            df_valid.groupby(["PONUM", "BasePOLINE"])["InvoiceDate"]
            .max()
            .reset_index()
        )

        return {
            (row["PONUM"], row["BasePOLINE"]): row["InvoiceDate"]
            for _, row in grouped.iterrows()
        }
    
    def route_eta_cases(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        将在手 PO 数据按交期确认状态与分批状态分流为多个类别。
        """
        df["CommentNorm"] = df["Comment"].fillna("").str.upper()

        # 1️⃣ Confirmed：已确认交期（通过 Comment）
        df_confirmed = df[
            df["CommentNorm"].isin(["DELIVERY DATE CONFIRMED", "ESTIMATED DELIVERY DATE"])
        ].copy()

        # 已处理 index 累积器
        used_idx = set(df_confirmed.index)

        # 2️⃣ Shipped：InTransitQty > 0 且未确认
        df_remain_1 = df[~df.index.isin(used_idx)]
        df_shipped = df_remain_1[df_remain_1["InTransitQty"] > 0].copy()
        used_idx |= set(df_shipped.index)

        # 3️⃣ SplitInProgress：剩余量 < 订单量
        df_remain_2 = df[~df.index.isin(used_idx)]
        df_tail = df_remain_2[
            (df_remain_2["RemainingQty"] > 0) &
            (df_remain_2["RemainingQty"] < df_remain_2["OrderedQty"])
        ].copy()
        used_idx |= set(df_tail.index)

        # 4️⃣ 剩余 = 总量
        df_remain_3 = df[~df.index.isin(used_idx)]
        df_batch = df_remain_3[df_remain_3["IsBatchProne"] == "Y"].copy()
        df_single = df_remain_3[df_remain_3["IsBatchProne"] != "Y"].copy()

        return {
            "Confirmed": df_confirmed,
            "Shipped": df_shipped,
            "SplitInProgress": df_tail,
            "LikelySplit": df_batch,
            "SingleDelivery": df_single,
        }
    
    def transform_confirmed(self, df_confirmed: pd.DataFrame) -> pd.DataFrame:
        """
        使用 InvoiceDate 作为 ETA，补充 InTransitQty。
        """
        if df_confirmed.empty:
            return pd.DataFrame()

        df = df_confirmed.copy()
        df["ETA_Date"] = pd.to_datetime(df["InvoiceDate"])
        df["ETA_Week"] = df["ETA_Date"].apply(to_yearweek)
        df["ETA_Flag"] = "ConfirmedDate"
        df["InTransitQty"] = df["RemainingQty"]

        this_week = to_yearweek(pd.Timestamp.today())
        df["Comment"] = df["Comment"].fillna("")
        # df["IsOverdue"] = np.where(df["ETA_Week"] < this_week, "Y", "N")
        df.loc[df["ETA_Week"] < this_week, "Comment"] += "ETA overdue! Contact supplier."

        return df[[
            "ITEMNUM", "Warehouse", "PONUM", "POLINE",
            "VendorCode", "TransportMode", "InTransitQty",
            "ETA_Date", "ETA_Week", "ETA_Flag", "Comment",
        ]]
    
    def estimate_transport_days(self,
        row: pd.Series,
        df_vendor_lt: pd.DataFrame,
        df_vendor_master: pd.DataFrame,
        lead_metric: str = "Q60"
    ) -> int:
        vendor = row["VendorCode"]
        mode = row.get("TransportMode") or TransportMode.DEFAULT
        wh = row["Warehouse"]
        lead_col = lead_metric + "TransportLeadTime"  # e.g. Q60TransportLeadTime

        # ✅ Step 1: 查 Vendor LT Stats（优先使用统计值）
        match = df_vendor_lt[
            (df_vendor_lt["VendorCode"] == vendor) &
            (df_vendor_lt["TransportMode"] == mode) &
            (df_vendor_lt["Warehouse"] == wh)
        ]
        if not match.empty and pd.notna(match.iloc[0].get(lead_col)):
            return int(match.iloc[0][lead_col])

        # ✅ Step 2: fallback 到 Vendor Master 静态交期
        match2 = df_vendor_master[
            (df_vendor_master["VendorCode"] == vendor) &
            (df_vendor_master["TransportMode"] == mode)
        ]
        if not match2.empty and pd.notna(match2.iloc[0].get("TransportLeadTimeDays")):
            return int(match2.iloc[0]["TransportLeadTimeDays"])

        # ✅ Step 3: fallback 到 TransportMode 默认值
        try:
            default_days = TransportMode(mode).lt_range[1]
            return int(default_days)
        except Exception:
            return 14
        
    def transform_shipped(
        self,
        df_shipped: pd.DataFrame,
        df_vendor_lt: pd.DataFrame,
        df_vendor_master: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        处理已发货订单的 ETA 预测，使用运输期交期统计 + fallback 逻辑 + 智能运输方式修正。
        已在外部分流，仅处理“Shipped”订单。
        """

        if df_shipped.empty:
            print("ℹ️ 无已发货订单，无需处理 ETA")
            return pd.DataFrame(columns=[
                "ITEMNUM", "Warehouse", "PONUM", "POLINE",
                "VendorCode", "TransportMode", "InTransitQty",
                "ETA_Date", "ETA_Week", "ETA_Flag", "Fallback_TransportUsed"
            ])

        # 🧮 Step 1: 计算 TransportTime
        df_shipped["InvoiceDate"] = pd.to_datetime(df_shipped["InvoiceDate"])
        df_shipped["TransportTime"] = (pd.Timestamp.today() - df_shipped["InvoiceDate"]).dt.days

        # 🚦 Step 2: 判断是否需要预测修正（仅限快运类 + 运输时间过长）
        fast_group = TransportMode._transport_groups().get("INTERNATIONAL_FAST", set())
        threshold_days = max(
            TransportMode(mode).lt_range[1]
            for mode in fast_group
            if TransportMode(mode).lt_range[1] != float("inf")
        )

        mask_predict = df_shipped["TransportMode"].isin(fast_group) & (df_shipped["TransportTime"] > threshold_days)

        if hasattr(self, "predictor") and self.predictor and mask_predict.any():
            df_shipped.loc[mask_predict] = self.predictor.correct(df_shipped.loc[mask_predict], overwrite=True)
            print(f"🔄 已对 {mask_predict.sum()} 条疑似异常记录应用 TransportModePredictor（阈值：{threshold_days}天）")
        else:
            print("ℹ️ 无需修正运输方式")

        # 🧩 Step 3: merge vendor leadtime stats
        df_shipped["TransportMode"] = df_shipped["TransportMode"].fillna("DEFAULT")
        lead_col = self.lead_metric + "TransportLeadTime"  # e.g. "Q60TransportLeadTime"

        if lead_col not in df_vendor_lt.columns:
            raise ValueError(f"❌ 运输交期字段 {lead_col} 不存在于 VendorTransportStats 中")

        df_merge = pd.merge(
            df_shipped,
            df_vendor_lt[["VendorCode", "TransportMode", "Warehouse", lead_col]],
            how="left",
            on=["VendorCode", "TransportMode", "Warehouse"]
        )

        # 🔧 Step 4: fallback（静态 VendorMaster + TransportMode 默认值）
        df_filled = self._fill_missing_transport_leadtime(
            df_merge,
            lead_col=lead_col,
            df_vendor_master=df_vendor_master
        )

        # 📆 Step 5: 计算 ETA 日期与周
        df_filled["ETA_Date"] = df_filled["InvoiceDate"] + df_filled[lead_col].apply(lambda x: timedelta(days=int(x)))
        df_filled["ETA_Week"] = df_filled["ETA_Date"].apply(to_yearweek)
        df_filled["ETA_Flag"] = "TransportEstimatedDate"

        # 📤 Step 6: 输出标准字段
        return df_filled[[
            "ITEMNUM", "Warehouse", "PONUM", "POLINE",
            "VendorCode", "TransportMode", "InTransitQty",
            "ETA_Date", "ETA_Week", "ETA_Flag",
            "Fallback_TransportUsed"
        ]]

    def simulate_tail_eta(
        self,
        df_tail: pd.DataFrame,
        last_delivery_dict: dict[tuple[str, str], pd.Timestamp],
        df_behavior: pd.DataFrame,
        df_vendor_lt: pd.DataFrame,
        df_vendor_master: pd.DataFrame,
        df_full: Optional[pd.DataFrame] = None,

    ) -> pd.DataFrame:
        """
        尾批 ETA 模拟：
        - 优先用行为统计；
        - fallback 自举同一 PO 内历史节奏；
        - fallback fallback 用默认参数；
        - 强制输出整数分批，且总和保持不变；
        - 自动追加 POLINE 子行后缀如 -1/-2。
        """
        results = []
        df_tail = df_tail.merge(
            df_behavior,
            on=["ITEMNUM", "Warehouse", "VendorCode", "TransportMode"],
            how="left"
        )
        df_full = df_full if df_full is not None else df_tail

        for _, row in df_tail.iterrows():
            try:
                ponum = row["PONUM"]
                poline = row["POLINE"]
                base_poline = poline.split("-")[0]
                total_qty = int(round(row["RemainingQty"]))  # ✅ 强制整数

                # 上次发货日期
                last_date = last_delivery_dict.get((ponum, base_poline))
                if pd.isna(last_date):
                    last_date = row.get("POEntryDate", pd.Timestamp.today())
                    lead_days = row.get(self.lead_metric + "LeadTime") or 14
                    last_date += pd.Timedelta(days=int(lead_days))

                # 默认 fallback 参数
                batch_count = 1
                interval_days = 7
                tail_rate = 1.0
                min_batch_qty = 10  # 可调，单位为整数件
                eta_flag = "TailBatchSimulated_FB_Default"

                # 是否有行为记录
                has_behavior = not pd.isna(row.get("PredictedBatchCount")) and row.get("Fallback_TotalLeadUsed") != "Y"

                if has_behavior:
                    # 行为预测参数
                    batch_count = int(row.get("PredictedBatchCount") or 1)
                    interval_days = int(row.get("PredictedBatchIntervalDays") or 7)
                    tail_rate = float(row.get("PredictedTailQtyRate") or 1.0)
                    min_batch_qty = int(row.get("MaxSingleBatchQty") or 0)
                    eta_flag = "TailBatchSimulated"

                else:
                    # 自举法 fallback
                    same_po = df_full[
                        (df_full["PONUM"] == ponum) &
                        (df_full["POLINE"].str.startswith(base_poline)) &
                        (df_full["POLINE"] != poline) &
                        (~df_full["InvoiceDate"].isna())
                    ].sort_values("InvoiceDate")

                    if len(same_po) >= 2:
                        interval_days = int((same_po["InvoiceDate"].iloc[1] - same_po["InvoiceDate"].iloc[0]).days)
                        batch_count = 1  # 🧠 自举不拆，认为是尾批
                        eta_flag = "TailBatchSimulated_FB_SelfBoot"

                # 🧠 小量不拆逻辑
                if total_qty <= min_batch_qty or batch_count <= 1:
                    qtys = [total_qty]
                else:
                    # 分批整数分配策略（前 N-1 批均分，最后一批补差）
                    base_qty = total_qty // batch_count
                    qtys = [base_qty] * batch_count
                    qtys[-1] += total_qty - sum(qtys)

                # ⚠️ 严格去除为 0 的批次（可选）
                qtys = [q for q in qtys if q > 0]
                if not qtys:
                    continue  # 全是 0 就跳过

                for i, qty in enumerate(qtys):
                    # ✅ 模拟发货日
                    ship_date = last_date + pd.Timedelta(days=interval_days * (i + 1))

                    # ✅ 加上运输交期
                    transport_days = self.estimate_transport_days(
                        row=row,
                        df_vendor_lt=df_vendor_lt,
                        df_vendor_master=df_vendor_master,
                        lead_metric=self.lead_metric  # e.g. "Q60"
                    )
                    eta_date = ship_date + pd.Timedelta(days=transport_days)

                    # ✅ 子行编号
                    poline_sub = f"{poline}-{i+1}"

                    results.append({
                        "ITEMNUM": row["ITEMNUM"],
                        "Warehouse": row["Warehouse"],
                        "PONUM": ponum,
                        "POLINE": poline_sub,
                        "VendorCode":row["VendorCode"],
                        "TransportMode": row.get("TransportMode"),
                        "ETA_Date": eta_date,
                        "ETA_Week": to_yearweek(eta_date),
                        "InTransitQty": qty,
                        "ETA_Flag": eta_flag,
                        "BatchIndex": i + 1,
                        "IsFinalBatch": "Y" if i == len(qtys) - 1 else "N"
                    })

            except Exception as e:
                print(f"⚠️ 尾批 ETA 模拟失败: {e} @ {row.get('PONUM')}-{row.get('POLINE')}")

        return pd.DataFrame(results)

    def simulate_single_eta(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        对一次性交货订单，使用 SmartLeadtime（如 Q60LeadTime）模拟 ETA。
        必须预先通过 _fill_missing_total_leadtime() 处理缺失项。
        """
        if df.empty:
            return pd.DataFrame(columns=[
                "ITEMNUM", "Warehouse", "PONUM", "POLINE",
                "VendorCode", "TransportMode", "InTransitQty",
                "ETA_Date", "ETA_Week", "ETA_Flag", "Comment",
                "BatchIndex", "IsFinalBatch"
            ])

        df = df.copy()
        df["InTransitQty"] = df["RemainingQty"]
        lead_col = self.lead_metric + "LeadTime"

        if lead_col not in df.columns:
            raise ValueError(f"❌ SmartLeadtime 缺少字段 {lead_col}")

        if df[lead_col].isna().any():
            raise ValueError(f"❌ 存在未处理的 {lead_col} 缺失值，请先执行 fallback")

        df["LeadTimeUsed"] = df[lead_col]
        df["ETA_Date"] = pd.to_datetime(df["POEntryDate"]) + df["LeadTimeUsed"].apply(lambda x: timedelta(days=int(x)))
        df["ETA_Week"] = df["ETA_Date"].apply(to_yearweek)

        if "Fallback_TotalLeadUsed" in df.columns:
            df["ETA_Flag"] = df["Fallback_TotalLeadUsed"].map({"Y": "StaticWLEAD", "N": "SimulatedSingle"})
        else:
            df["ETA_Flag"] = "SimulatedSingle"

        if "Comment" not in df.columns:
            df["Comment"] = "SingleDelivery"
        df["BatchIndex"] = None
        df["IsFinalBatch"] = None
        

        return df[[
            "ITEMNUM", "Warehouse", "PONUM", "POLINE",
            "VendorCode", "TransportMode", "InTransitQty",
            "ETA_Date", "ETA_Week", "ETA_Flag", "Comment",
            "BatchIndex", "IsFinalBatch","Fallback_TotalLeadUsed"
        ]]

    def simulate_batch_eta(self, df: pd.DataFrame,
                           df_vendor_lt: pd.DataFrame,
                            df_vendor_master: pd.DataFrame,) -> pd.DataFrame:
        results = []

        for _, row in df.iterrows():
            try:
                po_date = pd.to_datetime(row["POEntryDate"])
                total_qty = float(row.get("RemainingQty") or 0.0)
                batch_count = int(row.get("PredictedBatchCount") or 1)
                interval = int(row.get("PredictedBatchIntervalDays") or 7)
                tail_rate = float(row.get("PredictedTailQtyRate") or 0.0)

                if batch_count < 1 or total_qty <= 0:
                    continue

                # 🚚 获取运输交期
                transport_days = self.estimate_transport_days(
                    row=row,
                    df_vendor_lt=df_vendor_lt,
                    df_vendor_master=df_vendor_master,
                    lead_metric=self.lead_metric  # 默认 Q60
                )

                # ⏱️ 计算首发日（可以灵活配置：po_date + prepare_time）
                ship_start = po_date + timedelta(days=int(row.get("PrepareLeadTime", 0)))  # 可注入字段控制准备期

                # 📦 分批数量
                if tail_rate > 0 and batch_count >= 2:
                    tail_qty = round(total_qty * tail_rate)
                    front_qty = total_qty - tail_qty
                    each_qty = front_qty / (batch_count - 1)
                    qty_list = [each_qty] * (batch_count - 1) + [tail_qty]
                else:
                    each_qty = total_qty / batch_count
                    qty_list = [each_qty] * batch_count

                # 生成批次记录
                for i, qty in enumerate(qty_list):
                    ship_date = ship_start + timedelta(days=i * interval)
                    eta_date = ship_date + timedelta(days=transport_days)
                    results.append({
                        "ITEMNUM": row["ITEMNUM"],
                        "Warehouse": row["Warehouse"],
                        "PONUM": row["PONUM"],
                        "POLINE": row["POLINE"],
                        "VendorCode": row.get("VendorCode"),
                        "TransportMode": row.get("TransportMode"),
                        "InTransitQty": int(round(qty)),
                        "ETA_Date": eta_date,
                        "ETA_Week": to_yearweek(eta_date),
                        "ETA_Flag": "TailBatchSimulated",
                        "BatchIndex": i + 1,
                        "IsFinalBatch": "Y" if i == batch_count - 1 else "N"
                    })

            except Exception as e:
                print(f"⚠️ 尾批 ETA 模拟失败: {e} @ {row.get('PONUM')}-{row.get('POLINE')}")

        df_batches = pd.DataFrame(results)

        if not df_batches.empty:
            df_batches = generate_virtual_po_sublines(
                df_batches,
                po_col="PONUM",
                line_col="POLINE",
                sort_cols=["ETA_Date"],
                new_col="POLINE"
            )

        return df_batches
    
    def _fill_missing_transport_leadtime(
        self,
        df: pd.DataFrame,
        lead_col: str,
        df_vendor_master: pd.DataFrame
    ) -> pd.DataFrame:
        """
        对运输期字段 lead_col 缺失行进行填充：
        1. VendorMaster（静态交期）
        2. TransportMode.default_leadtime（来自枚举类）
        """
        df["Fallback_TransportUsed"] = "N"
        missing_idx = df[df[lead_col].isna()].index
        if missing_idx.empty:
            return df

        print(f"⚠️ 运输期字段 {lead_col} 缺失，共 {len(missing_idx)} 条，将尝试 fallback")

        # 1️⃣ VendorMaster fallback（静态运输交期）
        df_vm = df_vendor_master[["VendorCode", "TransportMode", "TransportLeadTimeDays"]].copy()
        df = df.merge(df_vm, on=["VendorCode", "TransportMode"], how="left", suffixes=("", "_vm"))

        df.loc[missing_idx, lead_col] = df.loc[missing_idx, "TransportLeadTimeDays"]

        # 2️⃣ TransportMode 枚举默认值 fallback
        fallback_mask = df.index.isin(missing_idx) & df[lead_col].isna()
        df.loc[fallback_mask, lead_col] = df.loc[fallback_mask].apply(
            lambda row: TransportMode[row["TransportMode"]].default_leadtime
            if row["TransportMode"] in TransportMode.__members__ else 7,
            axis=1
        )

        # ✅ 标记 fallback 行（可选）
        
        df.loc[missing_idx, "Fallback_TransportUsed"] = "Y"

        return df.drop(columns=["TransportLeadTimeDays"], errors="ignore")

    def _fill_missing_total_leadtime(
        self,
        df: pd.DataFrame,
        lead_col: str,
        df_iwi: pd.DataFrame
    ) -> pd.DataFrame:
        """
        对总交期字段缺失行进行填补：
        来源：IWI.WLEAD
        """

        missing_idx = df[df[lead_col].isna()].index
        if missing_idx.empty:
            return df

        print(f"⚠️ 总交期缺失：{len(missing_idx)} 条记录，将尝试使用 IWI fallback")

        df_iwi_use = df_iwi[["ITEMNUM", "Warehouse", "WLEAD"]].copy()
        df = df.merge(df_iwi_use, on=["ITEMNUM", "Warehouse"], how="left", suffixes=("", "_iwi"))

        df.loc[missing_idx, lead_col] = df.loc[missing_idx, "WLEAD"]

        df["Fallback_TotalLeadUsed"] = "N"
        df.loc[missing_idx, "Fallback_TotalLeadUsed"] = "Y"
        # print(df)
        return df.drop(columns=["WLEAD"], errors="ignore")