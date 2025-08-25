import pandas as pd
from datetime import datetime
from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import DemandHistoryWeekly

class ItemDemand(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)
        self.history = pd.DataFrame(columns=["YearWeek", "TotalDemand"])

    def load(self, session, replacing_map=None, max_date=None, force_reload=False):
        """
        从数据库加载需求历史（可包含父件替代），并补齐缺失周
        """
        if not self.history.empty:
            return
        if self._loaded and not force_reload:
            return

        if max_date is None:
            max_date = pd.to_datetime(datetime.now().strftime("%G-W%V") + "-1", format="%G-W%V-%u")
        max_week_str = max_date.strftime("%G-W%V")

        related_items = [self.itemnum]
        if replacing_map and self.itemnum in replacing_map:
            related_items += [rel['parent'] for rel in replacing_map[self.itemnum]]

        rows = (
            session.query(DemandHistoryWeekly)
            .filter(
                DemandHistoryWeekly.ITEMNUM.in_(related_items),
                DemandHistoryWeekly.Warehouse == self.warehouse,
                DemandHistoryWeekly.YearWeek <= max_week_str,
            )
            .all()
        )

        if not rows:
            self.history = pd.DataFrame(columns=["YearWeek", "TotalDemand"])
            self._loaded = True
            return

        df = pd.DataFrame(
            [{"YearWeek": r.YearWeek, "TotalDemand": r.TotalDemand} for r in rows]
        )
        df["YearWeek"] = pd.to_datetime(df["YearWeek"] + "-1", format="%G-W%V-%u")
        df_grouped = df.groupby("YearWeek", as_index=False)["TotalDemand"].sum()

        all_weeks = pd.date_range(start=df_grouped["YearWeek"].min(), end=max_date, freq="W-MON")
        df_full = pd.DataFrame({"YearWeek": all_weeks})
        df_full = df_full.merge(df_grouped, on="YearWeek", how="left").fillna({"TotalDemand": 0})
        self.history = df_full.sort_values("YearWeek").reset_index(drop=True)
        self._loaded = True

    def load_from_df(self, df_all: pd.DataFrame, max_date=None, force_reload=False, debug=False):
        """
        从预加载的需求历史中提取自身记录，不聚合 parent
        """
        if not force_reload and not self.history.empty:
            return

        if max_date is None:
            max_date = pd.to_datetime(datetime.now().strftime("%G-W%V") + "-1", format="%G-W%V-%u")
        max_date = max_date.normalize()

        df = df_all[
            (df_all["ITEMNUM"] == self.itemnum) & (df_all["Warehouse"] == self.warehouse)
        ].copy()

        if df.empty:
            self.history = pd.DataFrame(columns=["YearWeek", "TotalDemand"])
            if debug:
                print(f"⚠️ No history found for {self.itemnum} @ {self.warehouse}")
            return

        if pd.api.types.is_string_dtype(df["YearWeek"]):
            df["YearWeek"] = pd.to_datetime(df["YearWeek"] + "-1", format="%G-W%V-%u")
        df = df[df["YearWeek"] <= max_date]

        df_grouped = df.groupby("YearWeek", as_index=False)["TotalDemand"].sum()

        all_weeks = pd.date_range(start=df_grouped["YearWeek"].min(), end=max_date, freq="W-MON")
        df_full = pd.DataFrame({"YearWeek": all_weeks})
        df_full = df_full.merge(df_grouped, on="YearWeek", how="left").fillna({"TotalDemand": 0})

        self.history = df_full.sort_values("YearWeek").reset_index(drop=True)

        if debug:
            print(f"✅ Loaded demand for {self.itemnum} @ {self.warehouse} → {len(self.history)} weeks")
            print(self.history.tail(3))

        self._loaded = True

    def _extra_fields(self) -> dict:
        return {
            "weeks": len(self.history),
            "total_demand": float(self.history["TotalDemand"].sum()) if not self.history.empty else 0.0,
        }

    def to_dataframe(self) -> pd.DataFrame:
        """获取需求历史的 DataFrame 表格副本"""
        return self.history.copy()
