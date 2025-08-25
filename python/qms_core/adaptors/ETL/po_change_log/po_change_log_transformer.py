import pandas as pd
from qms_core.core.common.base_transformer import BaseTransformer
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_transfomer import OpenPOTransformer 

class POChangeLogTransformer(BaseTransformer):
    """
    变更日志 Transformer：
    - 自动调用 open job 的 transformer 进行标准化预处理
    - 基于 prev + delv + now 进行快照对比
    """

    def __init__(self, raw: dict):
        self.df_now_raw = raw["df_now_raw"]
        self.df_prev = raw["df_prev"]
        self.df_delivery_history = raw["df_delv"]

        self.today = pd.Timestamp.today().normalize()

        self.key_cols = ["PONUM", "POLINE"]
        self.meta_cols = ["ITEMNUM", "Warehouse"]
        self.tracked_fields = {
            "PCQTY": "INVOICE",
            "PQREC": "RECEIPT"
        }

    def transform(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        df_now = OpenPOTransformer().transform(self.df_now_raw)

        # 标准化 key 列
        for col in self.key_cols:
            for df in [df_now, self.df_prev, self.df_delivery_history]:
                df[col] = df[col].astype(str)

        # 合并并追踪变更
        merged = self._merge_snapshots(df_now)
        logs = self._track_deltas(merged)
        logs += self._track_disappeared_lines(merged)
        return pd.DataFrame(logs), df_now

    def _merge_snapshots(self, df_now: pd.DataFrame) -> pd.DataFrame:
        cols_prev = self.key_cols + self.meta_cols + list(self.tracked_fields.keys()) + ["PQTRANSIT", "PQORD"]
        cols_now = self.key_cols + self.meta_cols + list(self.tracked_fields.keys()) + ["PQORD"]

        df_prev = self.df_prev[cols_prev].copy()
        df_now = df_now[cols_now].copy()

        return df_now.merge(df_prev, on=self.key_cols, suffixes=("", "_prev"), how="outer")

    def _track_deltas(self, df: pd.DataFrame) -> list[dict]:
        logs = []
        for _, row in df.iterrows():
            for field, event in self.tracked_fields.items():
                val_now = row.get(field, 0)
                val_prev = row.get(f"{field}_prev", 0)
                delta = val_now - val_prev
                if delta > 0:
                    logs.append({
                        "SNAPSHOT_DATE": self.today,
                        "PONUM": row["PONUM"],
                        "POLINE": row["POLINE"],
                        "ITEMNUM": row.get("ITEMNUM") or row.get("ITEMNUM_prev"),
                        "WAREHOUSE": row.get("Warehouse") or row.get("Warehouse_prev"),
                        "EVENT_TYPE": event,
                        "QTY_DELTA": delta,
                        "QTY_TOTAL": val_now,
                        "FIELD_NAME": field
                    })
        return logs

    def _track_disappeared_lines(self, df: pd.DataFrame) -> list[dict]:
        logs = []
        for _, row in df.iterrows():
            if pd.isna(row.get("ITEMNUM")) and not pd.isna(row.get("ITEMNUM_prev")):
                ponum = row["PONUM"]
                poline = row["POLINE"]
                pqtransit_prev = row.get("PQTRANSIT", 0)
                pqord_prev = row.get("PQORD", 0)
                pqrec_prev = row.get("PQREC_prev", 0)

                if pqtransit_prev > 0:
                    event = "RECEIPT"
                    qty = pqtransit_prev
                    qty_total = pqrec_prev + pqtransit_prev
                else:
                    delivery_exists = not self.df_delivery_history[
                        (self.df_delivery_history["PONUM"] == ponum) &
                        (self.df_delivery_history["POLINE"] == poline)
                    ].empty
                    event = "RECEIPT" if delivery_exists else "CANCEL"
                    qty = pqord_prev
                    qty_total = pqrec_prev

                logs.append({
                    "SNAPSHOT_DATE": self.today,
                    "PONUM": ponum,
                    "POLINE": poline,
                    "ITEMNUM": row["ITEMNUM_prev"],
                    "WAREHOUSE": row["Warehouse_prev"],
                    "EVENT_TYPE": event,
                    "QTY_DELTA": qty,
                    "QTY_TOTAL": qty_total,
                    "FIELD_NAME": "PQREC"
                })
        return logs
