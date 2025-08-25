from qms_core.core.common.params.enums import TransportMode
import pandas as pd
from typing import Optional

def build_fallback_leadtime_cache(raw_df: pd.DataFrame) -> dict:
    """
    构建干净的 fallback_cache：
    - 按 (Vendor, Warehouse, TransportMode) 聚合平均交期
    - 仅保留在静态范围内的条目
    """
    grouped = (
        raw_df
        .dropna(subset=["TransportTime", "TransportMode"])
        .groupby(["VendorCode", "Warehouse", "TransportMode"])["TransportTime"]
        .mean()
        .reset_index()
    )

    cache = {}
    for _, row in grouped.iterrows():
        vendor, warehouse, mode = row["VendorCode"], row["Warehouse"], row["TransportMode"]
        mean_tt = row["TransportTime"]
        if mode not in TransportMode._value2member_map_:
            continue
        static_low, static_high = TransportMode(mode).lt_range
        if static_low <= mean_tt <= static_high:
            cache[(vendor, warehouse, mode)] = float(mean_tt)
        else:
            print(f"⚠️ fallback_cache[{(vendor, warehouse, mode)}] = {mean_tt} 超出静态范围 [{static_low}, {static_high}] → 忽略")

    return cache


def get_leadtime_range(
    mode: str,
    vendor: str,
    warehouse: str,
    stats_df: pd.DataFrame,
    fallback_cache: Optional[dict] = None
) -> tuple[float, float, float]:
    static_low, static_high = TransportMode(mode).lt_range
    fallback_mean = (static_low + static_high) / 2
    lt_low, lt_high, mean_lt = static_low, static_high, fallback_mean

    # ✅ 只有在列存在时才尝试 subset
    if {'VendorCode', 'Warehouse', 'TransportMode'}.issubset(stats_df.columns):
        subset = stats_df[
            (stats_df['VendorCode'] == vendor) &
            (stats_df['Warehouse'] == warehouse) &
            (stats_df['TransportMode'] == mode)
        ]
        if not subset.empty:
            row = subset.iloc[0]
            if pd.notna(row.get('Q90TransportLeadTime')) and row['Q90TransportLeadTime'] <= static_high:
                lt_high = row['Q90TransportLeadTime']
            if pd.notna(row.get('MeanTransportLeadTime')) and static_low <= row['MeanTransportLeadTime'] <= static_high:
                mean_lt = row['MeanTransportLeadTime']
            for col in ['ModeTransportLeadTime', 'SmoothedTransportLeadTime']:
                v = row.get(col)
                if pd.notna(v) and static_low <= v <= static_high:
                    lt_low = v
                    break

    # fallback
    if fallback_cache:
        fb_val = fallback_cache.get((vendor, warehouse, mode))
        if fb_val is not None and static_low <= fb_val <= static_high:
            mean_lt = fb_val

    return float(lt_low), float(lt_high), float(mean_lt)


def assign_predicted_mode(
    df: pd.DataFrame,
    stats_df: pd.DataFrame,
    fallback_cache: Optional[dict] = None,
    tolerance_days: int = 5,
    debug_target_tt: Optional[float] = None
) -> pd.DataFrame:
    df = df.copy()
    assignable_modes = TransportMode.assignable_modes()
    leadtime_lookup = {}

    for vendor, warehouse in df[["VendorCode", "Warehouse"]].drop_duplicates().itertuples(index=False):
        for mode in assignable_modes:
            key = (vendor, warehouse, mode)
            leadtime_lookup[key] = get_leadtime_range(mode, vendor, warehouse, stats_df, fallback_cache)

    transport_groups = {
        "DOMESTIC": {TransportMode.TRUCK.value, TransportMode.TRAIN.value},
        "INTERNATIONAL_FAST": {
            TransportMode.AIR.value, TransportMode.COURIER.value, TransportMode.INTERNATIONAL_TRUCK.value
        },
        "INTERNATIONAL_MIDDLE": {TransportMode.INTERNATIONAL_TRAIN.value},
        "SLOW": {TransportMode.VESSEL.value},
    }
    mode_to_group = {m: g for g, modes in transport_groups.items() for m in modes}
    prohibited_transitions = {
        ("DOMESTIC", "INTERNATIONAL_FAST"),
        ("DOMESTIC", "INTERNATIONAL_MIDDLE"),
        ("DOMESTIC", "SLOW"),
        ("INTERNATIONAL_FAST", "DOMESTIC"),
        ("SLOW", "DOMESTIC"),
    }

    def is_switch_allowed(src_mode, tgt_mode):
        src_group = mode_to_group.get(src_mode, "UNKNOWN")
        tgt_group = mode_to_group.get(tgt_mode, "UNKNOWN")
        if (src_group, tgt_group) in prohibited_transitions:
            return False
        if src_group == tgt_group and src_mode != tgt_mode:
            return False  # 禁止同组切换
        return True

    group_penalty_matrix = {
        (g1, g2): 0 if g1 == g2 else 50 if "INTERNATIONAL" in g1 or "INTERNATIONAL" in g2 else 100
        for g1 in transport_groups for g2 in transport_groups
    }

    predicted = []
    for _, row in df.iterrows():
        vendor = row["VendorCode"]
        warehouse = row["Warehouse"]
        mode = row["TransportMode"]
        tt = row["TransportTime"]
        static_low, static_high = TransportMode(mode).lt_range
        key = (vendor, warehouse, mode)
        lt_low, lt_high, mean_lt = leadtime_lookup.get(key, (static_low, static_high, (static_low + static_high) / 2))
        current_group = mode_to_group.get(mode, "UNKNOWN")

        is_valid = lt_low <= tt <= lt_high and abs(tt - mean_lt) <= tolerance_days
        is_extreme_high = tt > lt_high + 5
        is_extreme_low = tt < static_low - 2

        if is_valid and not is_extreme_high and not is_extreme_low:
            predicted.append(mode)
            continue

        if debug_target_tt and tt == debug_target_tt:
            print(f"\n🔍 DEBUG {vendor}-{warehouse} TT={tt}, Mode={mode}")
            print(f"📎 当前范围: {lt_low}–{lt_high}, mean={mean_lt}, static=({static_low}–{static_high})")

        scores = []
        for m in assignable_modes:
            if m == mode or not is_switch_allowed(mode, m):
                continue

            lo, hi = TransportMode(m).lt_range
            if not (lo <= tt <= hi):
                continue

            try:
                stat_row = stats_df[
                    (stats_df["VendorCode"] == vendor) &
                    (stats_df["Warehouse"] == warehouse) &
                    (stats_df["TransportMode"] == m)
                ]
                stat_mean = stat_row["MeanTransportLeadTime"].values[0]
                avg = stat_mean if lo <= stat_mean <= hi else (lo + hi) / 2
            except:
                avg = (lo + hi) / 2

            base_dist = abs(tt - avg)
            group_penalty = group_penalty_matrix.get((current_group, mode_to_group.get(m, "UNKNOWN")), 100)
            score = base_dist + group_penalty + 20

            if debug_target_tt and tt == debug_target_tt:
                print(f"✅ 候选 {m}: avg={avg:.1f}, dist={base_dist:.1f}, group_penalty={group_penalty}, score={score:.1f}")

            scores.append((m, score))

        if scores:
            best = sorted(scores, key=lambda x: x[1])[0][0]
            predicted.append(best)
            if tt == debug_target_tt:
                print(f"🎯 推荐模式: {best}")
        else:
            if static_low <= tt <= static_high:
                predicted.append(mode)
                if tt == debug_target_tt:
                    print(f"✅ fallback 保留原模式: {mode}")
            else:
                min_dist, best_fallback = float("inf"), None
                for m in assignable_modes:
                    if m == mode or not is_switch_allowed(mode, m):
                        continue

                    lo, hi = TransportMode(m).lt_range
                    if not (lo <= tt <= hi):
                        continue

                    try:
                        stat_row = stats_df[
                            (stats_df["VendorCode"] == vendor) &
                            (stats_df["Warehouse"] == warehouse) &
                            (stats_df["TransportMode"] == m)
                        ]
                        stat_mean = stat_row["MeanTransportLeadTime"].values[0]
                        avg = stat_mean if lo <= stat_mean <= hi else (lo + hi) / 2
                    except:
                        avg = (lo + hi) / 2

                    dist = abs(tt - avg)
                    if dist < min_dist:
                        min_dist = dist
                        best_fallback = m

                    if debug_target_tt and tt == debug_target_tt:
                        print(f"🧪 fallback {m}: avg={avg:.1f}, dist={dist:.1f}")

                final_mode = best_fallback or mode
                predicted.append(final_mode)
                if tt == debug_target_tt:
                    print(f"🎯 fallback 最终推荐: {final_mode}")

    df["Predicted Transport Mode"] = predicted
    return df


def correct_transport_mode(
    df: pd.DataFrame,
    stats_df: pd.DataFrame,
    tolerance_days: int = 5,
    overwrite: bool = True,
    fallback_cache: Optional[dict] = None,
    map_courier_to_air: bool = False,
    debug_target_tt: Optional[float] = None
) -> pd.DataFrame:
    """
    修正 TransportMode 字段：
    - 使用 assign_predicted_mode 向量化逻辑
    - 支持动态上下限判断、不合理模式纠正
    - fallback 均值通过 cache 提供
    - 支持将 Courier 映射为 Air（简化模式分析）
    """

    df = df.copy()

    # Step 1: 备份原 TransportMode
    if "OriginalTransportMode" not in df.columns:
        df["OriginalTransportMode"] = df["TransportMode"]

    # Step 2: 可选将 Courier 映射为 Air（用于合并分组）
    if map_courier_to_air:
        df["TransportMode"] = df["TransportMode"].apply(
            lambda x: TransportMode.AIR.value
            if isinstance(x, str) and x.strip().lower() == TransportMode.COURIER.value.lower()
            else x
        )

    # Step 3: 调用新版 assign_predicted_mode（具备所有分组与静态修正能力）
    df = assign_predicted_mode(
        df=df,
        stats_df=stats_df,
        fallback_cache=fallback_cache,
        tolerance_days=tolerance_days,
        debug_target_tt=debug_target_tt
    )

    # Step 4: 是否覆盖原 TransportMode 字段
    if overwrite:
        df["TransportMode"] = df["Predicted Transport Mode"]

    return df
