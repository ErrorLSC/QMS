from pydantic import BaseModel, Field


class MRPParamsSchema(BaseModel):
    use_moq: bool = Field(default=True, description="是否启用最小起订量（MOQ）策略")
    min_moq: int = Field(default=1, description="最小起订量下限")
    round_up_to_wlots: bool = Field(default=False, description="是否向上补齐为 WLOTS 整数倍")
    use_wlot: bool = Field(default=False, description="是否启用补货批量对齐（WLOTS）")
    include_zero_qty: bool = Field(default=False, description="是否保留推荐补货量为 0 的物料（默认不保留）")

    model_config = {
        "extra": "forbid"
    }