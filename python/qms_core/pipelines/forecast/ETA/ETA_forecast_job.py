from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.forecast.ETA.extractor import ETAExtractor
from qms_core.core.forecast.ETA.calculator import ETATransformer
from qms_core.infrastructure.db.models import PO_ETA_Recommendation
from qms_core.core.forecast.ETA.service import build_mode_predictor
import pandas as pd

class ETAForecastJob(BaseJobCore):
    """
    ETA预测 Job：
    - 来源：ETAExtractor
    - 输出：PO_ETA_Recommendation
    """
    EXTRACTOR_CLASS = ETAExtractor
    TRANSFORMER_CLASS = ETATransformer
    TARGET_TABLE = PO_ETA_Recommendation

    def __init__(self, config, lead_metric="Q60", predictor=None):
        super().__init__(config)
        self.lead_metric = lead_metric
        self.predictor = predictor
        self.use_smart_writer = True
    
    def extract(self) -> dict[str, pd.DataFrame]:
        return self.EXTRACTOR_CLASS(self.config).fetch()

    def transform(self, data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
        predictor = self.predictor or build_mode_predictor(
            df_vendor_stats=data_dict["vendor_transport_stat"],
            df_history=data_dict["history"]
        )
        return self.TRANSFORMER_CLASS(lead_metric=self.lead_metric, predictor=predictor).transform(data=data_dict)

    def target_table(self):
        return self.TARGET_TABLE
    
    def run(self, dry_run: bool = True, session=None) -> pd.DataFrame:
        print(f"🚀 运行任务：{self.job_name}")
        data_dict = self.extract()
        df = self.transform(data_dict=data_dict)
        return self.load(df, dry_run=dry_run, session=session)