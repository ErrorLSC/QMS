from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple

import pandas as pd

from qms_core.core.common.params.enums import TransportMode


# --------------------------------------------------------------------------- #
# ①  FallbackLeadTimeCache: 负责离线构建 & 查询 fallback 均值                 #
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class FallbackLeadTimeCache:
    """
    (Vendor, Warehouse, Mode) ➜ 均值交期
    - 仅保留位于静态范围内的数据
    """
    _cache: Dict[Tuple[str, str, str], float] = field(default_factory=dict)

    @classmethod
    def build(cls, raw_df: pd.DataFrame) -> "FallbackLeadTimeCache":
        grouped = (
            raw_df
            .dropna(subset=["TransportTime", "TransportMode"])
            .groupby(["VendorCode", "Warehouse", "TransportMode"])["TransportTime"]
            .mean()
            .reset_index()
        )

        cache: Dict[Tuple[str, str, str], float] = {}
        for _, r in grouped.iterrows():
            mode_val = r["TransportMode"]
            if mode_val not in TransportMode._value2member_map_:
                continue

            static_low, static_high = TransportMode(mode_val).lt_range
            if static_low <= r["TransportTime"] <= static_high:
                cache[(r["VendorCode"], r["Warehouse"], mode_val)] = float(r["TransportTime"])

        return cls(cache)

    # -------- 公共 API -------- #
    def get(self, vendor: str, wh: str, mode: str) -> Optional[float]:
        """返回均值（若不存在则 None）"""
        return self._cache.get((vendor, wh, mode))


# --------------------------------------------------------------------------- #
# ②  LeadTimeService: 统一提供 (low, high, mean) 查询                        #
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class LeadTimeService:
    stats_df: pd.DataFrame
    fb_cache: Optional[FallbackLeadTimeCache] = None

    def range_of(
        self, mode: str, vendor: str, warehouse: str
    ) -> Tuple[float, float, float]:
        """根据优先级：静态 ➜ stats_df ➜ fallback_cache"""
        static_low, static_high = TransportMode(mode).lt_range
        lt_low, lt_high, mean_lt = static_low, static_high, (static_low + static_high) / 2

        # 1) stats_df（粒度：Vendor+Warehouse+Mode）
        if {"VendorCode", "Warehouse", "TransportMode"}.issubset(self.stats_df.columns):
            subset = self.stats_df[
                (self.stats_df["VendorCode"] == vendor)
                & (self.stats_df["Warehouse"] == warehouse)
                & (self.stats_df["TransportMode"] == mode)
            ]
            if not subset.empty:
                row = subset.iloc[0]
                if pd.notna(row.get("Q90TransportLeadTime")) and row["Q90TransportLeadTime"] <= static_high:
                    lt_high = row["Q90TransportLeadTime"]
                if pd.notna(row.get("MeanTransportLeadTime")) and static_low <= row["MeanTransportLeadTime"] <= static_high:
                    mean_lt = row["MeanTransportLeadTime"]

                # 优先级：ModeTransportLeadTime > SmoothedTransportLeadTime
                for col in ("ModeTransportLeadTime", "SmoothedTransportLeadTime"):
                    v = row.get(col)
                    if pd.notna(v) and static_low <= v <= static_high:
                        lt_low = v
                        break

        # 2) fallback cache
        if self.fb_cache:
            fb_val = self.fb_cache.get(vendor, warehouse, mode)
            if fb_val is not None and static_low <= fb_val <= static_high:
                mean_lt = fb_val

        return float(lt_low), float(lt_high), float(mean_lt)


# --------------------------------------------------------------------------- #
# ③  TransportModePredictor: 入口类                                          #
# --------------------------------------------------------------------------- #
@dataclass
class TransportModePredictor:
    """
    - 支持向量化预测 / 修正 TransportMode
    - 依赖 LeadTimeService 做交期区间查询
    """
    leadtime_svc: LeadTimeService
    tolerance_days: int = 5
    group_penalty_matrix: Dict[Tuple[str, str], int] = field(init=False)

    # ---------------- 初始化构造 ---------------- #
    def __post_init__(self) -> None:
        self.group_penalty = TransportMode.group_penalty

    # ---------------- 公共 API ---------------- #
    def correct(
        self,
        df: pd.DataFrame,
        overwrite: bool = True,
        map_courier_to_air: bool = False,
        debug_tt: Optional[float] = None,
    ) -> pd.DataFrame:
        """
        主入口：返回 *新 DataFrame*（无副作用）  
        - 生成列 `PredictedTransportMode`  
        - `overwrite=True` 时同步写回 `TransportMode`
        """
        df = df.copy()

        # 0) 备份原始模式
        if "OriginalTransportMode" not in df.columns:
            df["OriginalTransportMode"] = df["TransportMode"]

        # 1) 可选：Courier → Air
        if map_courier_to_air:
            df["TransportMode"] = df["TransportMode"].replace(
                {TransportMode.COURIER.value: TransportMode.AIR.value}
            )

        # 2) 计算预测
        df["PredictedTransportMode"] = self._assign_predicted(df, debug_tt)

        # 3) 是否覆盖
        if overwrite:
            df["TransportMode"] = df["PredictedTransportMode"]

        return df

    # ---------------- 内部实现 ---------------- #
    def _assign_predicted(self, df: pd.DataFrame, debug_tt: Optional[float]) -> pd.Series:
        assignable = TransportMode.assignable_modes()
        # 缓存 (vendor, wh, mode) ➜ (lo, hi, mean)
        lt_lookup = {
            (v, w, m): self.leadtime_svc.range_of(m, v, w)
            for v, w in df[["VendorCode", "Warehouse"]].drop_duplicates().itertuples(index=False)
            for m in assignable
        }

        # --- 向量化遍历 ---
        predicted = []
        for _, r in df.iterrows():
            vendor, wh, mode, tt = r["VendorCode"], r["Warehouse"], r["TransportMode"], r["TransportTime"]
            static_low, static_high = TransportMode(mode).lt_range
            lo, hi, mean_ = lt_lookup.get((vendor, wh, mode), (static_low, static_high, (static_low + static_high) / 2))
            cur_group = TransportMode.group_of(mode)

            # a) 判断原模式是否可接受
            if lo <= tt <= hi and abs(tt - mean_) <= self.tolerance_days:
                predicted.append(mode)
                continue

            # b) 评估候选
            candidate_scores = []
            for m in assignable:
                if m == mode or not self._is_switch_allowed(mode, m):
                    continue
                lo2, hi2 = TransportMode(m).lt_range
                if not (lo2 <= tt <= hi2):
                    continue

                lo2, hi2, mean2 = lt_lookup.get((vendor, wh, m), (lo2, hi2, (lo2 + hi2) / 2))
                base_dist = abs(tt - mean2)
                penalty = self.group_penalty(cur_group, TransportMode.group_of(m))
                candidate_scores.append((m, base_dist + penalty + 20))

            # c) 选择分数最低者
            best = (
                sorted(candidate_scores, key=lambda x: x[1])[0][0]
                if candidate_scores
                else mode
            )
            predicted.append(best)

            if debug_tt and tt == debug_tt:
                print(f"[DEBUG] tt={tt}  原={mode} ➜  预测={best}")

        return pd.Series(predicted, index=df.index)

    # ------- 私有工具 ------- #
    @staticmethod
    def _is_switch_allowed(src: str, tgt: str) -> bool:
        return TransportMode.is_switch_allowed(src, tgt)
