from enum import Enum
from functools import lru_cache

class DemandType(str, Enum):
    STEADY = "Steady Demand"
    SEASONAL = "Seasonal Demand"
    TRENDED = "Trended Demand"
    INTERMITTENT = "Intermittent Demand"
    BURST = "Burst Demand"
    SINGLE = "Single Demand"
    NEW = "New Item"
    REPLACED = "Replaced"
    STOCKONLY = "Stock Only"

class ActivityLevel(str, Enum):
    ACTIVE = "Active"
    OCCASIONAL = "Occasional"
    INACTIVE = "Inactive"
    DORMANT = "Dormant"

class OrderReason(str, Enum):
    ON_DEMAND = "On-Demand"
    SAFETY_TOP_UP = "SafetyStockTopUp"
    REPLENISH = "Replenish"
    REPLENISH_AND_ON_DEMAND = "Replenish & On-Demand"

class ForecastType(str, Enum):
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"

class VendorType(str, Enum):
    INTERNAL = "INTERNAL"
    MANUFACTURING = "MANUFACTURING"
    OVERSEA_DC = "OVERSEA_DC"
    DOMESTIC_DC = "DOMESTIC_DC"
    DOMESTIC_EXTERNAL = "DOMESTIC_EXTERNAL"
    OVERSEA_EXTERNAL = "OVERSEA_EXTERNAL"
    REDISTRIBUTION = "REDISTRIBUTION"

class TransportMode(str, Enum):
    VESSEL = "Vessel"
    AIR = "Air"
    COURIER = "Courier"
    TRUCK = "Truck"
    TRAIN = "Train"
    PORTAL = "Teleportation"
    INTERNAL = "Internal Transfer"
    UNKNOWN = "Unknown"
    INTERNATIONAL_TRUCK = "International Truck"
    INTERNATIONAL_TRAIN = "International Train"
    DEFAULT = "Default"

    @staticmethod
    def list_modes():
        return [m.value for m in TransportMode if m != TransportMode.UNKNOWN]
    @staticmethod
    def assignable_modes():
        return [m.value for m in TransportMode if m not in {TransportMode.PORTAL, TransportMode.INTERNAL,TransportMode.INTERNATIONAL_TRUCK,TransportMode.INTERNATIONAL_TRAIN, TransportMode.UNKNOWN,TransportMode.DEFAULT}]

    @property
    def lt_range(self) -> tuple[int, int | float]:
        """返回默认交期范围（单位：天）"""
        return {
            TransportMode.VESSEL:  (15, float('inf')),
            TransportMode.AIR:     (0, 14),
            TransportMode.COURIER: (0, 7),
            TransportMode.TRUCK:  (0, 7),
            TransportMode.TRAIN:  (5, 10),
            TransportMode.PORTAL: (0, 0.1),
            TransportMode.INTERNAL: (0, 5),
            TransportMode.INTERNATIONAL_TRUCK: (5,10),
            TransportMode.INTERNATIONAL_TRAIN: (15,40)
        }.get(self, (0, float('inf')))  # 默认无限宽容
    
    @property
    def default_leadtime(self) -> int:
        """推荐默认值（中值 + 限幅）"""
        low, high = self.lt_range
        if high == float('inf'):
            return low + 7
        return (low + high) // 2
    
        # ---------- ① 运输组定义 & 反向映射 ---------- #
    @classmethod
    @lru_cache(maxsize=None)
    def _mode_to_group(cls) -> dict[str, str]:
        return {m: g for g, modes in cls._transport_groups().items() for m in modes}

    @classmethod
    def _transport_groups(cls) -> dict[str, set[str]]:
        return {
            "DOMESTIC": {cls.TRUCK.value, cls.TRAIN.value},
            "INTERNATIONAL_FAST": {cls.AIR.value, cls.COURIER.value, cls.INTERNATIONAL_TRUCK.value},
            "INTERNATIONAL_MIDDLE": {cls.INTERNATIONAL_TRAIN.value},
            "SLOW": {cls.VESSEL.value}
        }
    
    # ---------- ② 辅助查询 ---------- #
    @classmethod
    def group_of(cls, mode: str) -> str:
        mode_to_group = {
            m: g for g, modes in cls._transport_groups().items() for m in modes
        }
        return mode_to_group.get(mode, "UNKNOWN")

    @classmethod
    def group_penalty(cls, g1: str, g2: str) -> int:
        """两组间切换 penalty —— 逻辑与旧代码保持一致"""
        if g1 == g2:
            return 0
        return 50 if "INTERNATIONAL" in (g1 + g2) else 100

    # ---------- ③ 禁止切换对 ---------- #
    @classmethod
    def _prohibited_transitions(cls) -> set[tuple[str, str]]:
        return {
            ("DOMESTIC", "INTERNATIONAL_FAST"),
            ("DOMESTIC", "INTERNATIONAL_MIDDLE"),
            ("DOMESTIC", "SLOW"),
            ("INTERNATIONAL_FAST", "DOMESTIC"),
            ("SLOW", "DOMESTIC"),
        }
    # ---------- ④ 复用此规则 ---------- #
    @classmethod
    def is_switch_allowed(cls, src_mode: str, tgt_mode: str) -> bool:
        if src_mode == tgt_mode:
            return True
        g1, g2 = cls.group_of(src_mode), cls.group_of(tgt_mode)
        if (g1, g2) in cls._prohibited_transitions():  # ✅ 正确调用函数
            return False
        if g1 == g2 and src_mode != tgt_mode:
            return False
        return True