from typing import Type, Any

class BaseStrategyRegistry:
    """
    通用策略注册器基类：用于将某种类型（如 DemandType）映射到相应的策略类实例。

    支持策略类实现 from_params() 进行参数注入，也支持 fallback 到默认构造。
    """

    def __init__(self, strategy_classes: list[Type], param_obj: Any = None):
        self.registry = self._register(strategy_classes, param_obj)

    def _register(self, strategy_classes: list[Type], param_obj: Any) -> dict:
        registry = {}
        for cls in strategy_classes:
            dtype = getattr(cls, "demand_type", None)
            if dtype is None:
                raise ValueError(f"Strategy class {cls.__name__} is missing `demand_type` attribute.")

            # 优先使用 from_params 注入参数
            if hasattr(cls, "from_params"):
                registry[dtype] = cls.from_params(param_obj)
            else:
                registry[dtype] = cls()
        return registry

    def get(self, key, default_cls: Type = None):
        """
        获取注册的策略类实例，如果不存在则返回 default_cls 的实例。

        参数:
            key: 映射的枚举值（如 DemandType.STEADY）
            default_cls: 回退用的策略类（必须提供）

        返回:
            策略类实例
        """
        if default_cls is None:
            raise ValueError("default_cls must be provided as fallback.")
        return self.registry.get(key, default_cls())
