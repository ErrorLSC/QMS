from pydantic import BaseModel,Field

class ClassifierParamsSchema(BaseModel):
    decay_factor: float = Field(0.9, description="权重衰减因子")
    winsor_upper: float = Field(0.95, description="上分位数温莎化，用于异常值处理")

    # 新品判断
    new_item_weeks: int = Field(8, description="新品判定周数")

    # Single Demand 判断
    single_zero_ratio: float = Field(0.95, description="Single 类型的零需求周占比阈值")
    single_recent_demand: int = Field(0, description="Single 类型的近期需求周数最大值")

    # Intermittent 判断
    intermittent_zero_ratio: float = Field(0.7, description="Intermittent 类型零需求周占比阈值")

    # Steady 判断
    steady_cv_threshold: float = Field(1.0, description="Steady 类型的 CV 最大值")
    steady_min_weeks_ratio: float = Field(0.25, description="Steady 类型的有需求周占比下限")
    steady_max_zero_ratio: float = Field(0.6, description="Steady 类型允许的最大零需求周占比")

    # 趋势判断
    trend_slope_threshold: float = Field(0.3, description="趋势型判断的斜率阈值")

    # 季节性判断
    seasonal_strength_ratio: float = Field(0.4, description="季节性强度相对于波动的倍数")
    seasonal_strength_min: float = Field(0.5, description="最低季节性强度")

    # Burst 判断
    burst_cv_threshold: float = Field(1.5, description="爆发型最小 CV")
    burst_zero_ratio_max: float = Field(0.3, description="爆发型允许的最大 0 占比")
    burst_recent_max_multiplier: float = Field(3.0, description="尾部最大值相对于均值的倍数阈值")

    # 活跃度判断
    inactive_zero_ratio: float = Field(0.8, description="非活跃物料零需求周占比阈值")
    dormant_zero_ratio: float = Field(0.95, description="休眠类物料需求周占比阈值")
    inactive_recent_weeks_demand_threshold: int = Field(2, description="非活跃物料最近需求次数上限")
    occasional_zero_ratio_threshold: float = Field(0.6, description="偶发型物料零需求周占比阈值")

    # 其他
    debug: bool = Field(False, description="是否启用调试模式")
    default_service_level_if_replaced: float = Field(0.85, description="被替代件的默认服务水平")
    seasonal_decompose_period: int = Field(4, description="季节性分解的周期长度")
    burst_tail_window: int = Field(4, description="爆发型判断的尾部窗口")
    burst_mean_window: int = Field(12, description="爆发型判断的均值窗口")
    recent_weeks_window: int = Field(12, description="活跃度判断的近期窗口长度")

    model_config = {
        "extra": "forbid"
    }