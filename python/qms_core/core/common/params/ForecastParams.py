from typing import Dict, Any
from qms_core.core.common.params.enums import DemandType
from pydantic import BaseModel,Field

class ForecastParamsSchema(BaseModel):
    decay_factor: float = Field(default=0.9, description="权重衰减因子")
    winsor_upper: float = Field(default=0.95, description="上分位数截断")

    forecast_horizon_weeks: int = Field(default=12, description="预测窗口，单位：周")

    # SteadyForecaster
    steady_n_samples: int = Field(default=1000, description="bootstrap 采样数")
    steady_quantile: float = Field(default=0.6, description="预测分位数")

    # SeasonalForecaster
    seasonal_baseline_window_weeks: int = Field(default=12, description="季节性基准窗口")
    seasonal_fill_profile_default: float = Field(default=1.0, description="默认填充值")
    seasonal_debug: bool = Field(default=False, description="是否启用调试输出")

    # IntermittentForecaster
    intermittent_alpha: float = Field(default=0.1, description="Croston 方法的 alpha")

    def to_method_kwargs_dict(self) -> Dict[str, Dict[str, Any]]:
        """转换为每类 Forecaster 的参数字典"""
        return {
            DemandType.STEADY: {
                "n_samples": self.steady_n_samples,
                "quantile": self.steady_quantile,
            },
            DemandType.SEASONAL: {
                "baseline_window_weeks": self.seasonal_baseline_window_weeks,
                "fill_profile_default": self.seasonal_fill_profile_default,
                "debug": self.seasonal_debug,
            },
            DemandType.INTERMITTENT: {
                "alpha": self.intermittent_alpha,
            },
            # Trended/Burst 不需参数，使用默认模型
        }

    model_config = {
        "extra": "forbid"  # 禁止 YAML 中意外字段
    }