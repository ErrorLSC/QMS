from qms_core.core.common.params.ClassifierParams import ClassifierParamsSchema
from qms_core.core.common.params.ForecastParams import ForecastParamsSchema
from qms_core.core.common.params.SafetyStockParams import SafetyStockParamsSchema,ServiceLevelParamsSchema
from qms_core.core.common.params.MRPParams import MRPParamsSchema
import yaml
from typing import Optional

class ParasCenter:
    def __init__(self, config_dict: Optional[dict] = None):
        config = config_dict or {}

        self.classifier_params = ClassifierParamsSchema(**config.get("ClassifierParams", {}))
        self.forecast_params = ForecastParamsSchema(**config.get("ForecastParams", {}))
        self.safety_params = SafetyStockParamsSchema(**config.get("SafetyStockParams", {}))
        self.service_level_params = ServiceLevelParamsSchema(**config.get("ServiceLevelParams", {}))
        self.mrp_params = MRPParamsSchema(**config.get("MRPParams", {}))
        # self.supplier_params = SupplierParamsSchema(**config.get("SupplierParams", {}))

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ParasCenter":
        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return cls(config_dict=config)

    def to_dict(self) -> dict:
        return {
            "ClassifierParams": self.classifier_params.model_dump(),
            "ForecastParams": self.forecast_params.model_dump(),
            "SafetyStockParams": self.safety_params.model_dump(),
            "ServiceLevelParams": self.service_level_params.model_dump(),
            "MRPParams": self.mrp_params.model_dump(),
            # "SupplierParams": self.supplier_params.model_dump(),
        }