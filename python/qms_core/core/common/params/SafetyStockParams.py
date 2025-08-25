from pydantic import BaseModel, Field

class ServiceLevelParamsSchema(BaseModel):
    max_leadtime: int = Field(default=360, description="服务水平计算最大支持的交期（天）")
    min_servicelevel: float = Field(default=0.85, description="最低推荐服务水平")
    max_servicelevel: float = Field(default=0.95, description="最高推荐服务水平")

    model_config = {
        "extra": "forbid"
    }
    
class SafetyStockParamsSchema(BaseModel):
    decay_factor: float = Field(default=0.9, description="权重衰减因子")
    winsor_upper: float = Field(default=0.95, description="上分位数截断")

    # Intermittent 方法参数
    intermittent_alpha: float = Field(default=0.1, description="Croston 方法中的 alpha")

    service_level: ServiceLevelParamsSchema = Field(
        default_factory=ServiceLevelParamsSchema,
        description="服务水平相关参数"
    )

    model_config = {
        "extra": "forbid"
    }