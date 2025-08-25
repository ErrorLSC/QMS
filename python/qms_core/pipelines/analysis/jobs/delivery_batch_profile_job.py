from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.analysis.delivery.delivery_behavior_analyzer import DeliveryBatchProfileAnalyzer
from qms_core.infrastructure.db.models import (
    ItemDeliveryBehaviorStats,
    ItemBatchProfile
)
from qms_core.infrastructure.db.reader import fetch_orm_data
import pandas as pd


class DeliveryBatchProfileJob(BaseJobCore):
    """
    发货节奏预测 Job：
    - 来源：ItemDeliveryBehaviorStats
    - 输出：ItemBatchProfile
    """

    ANALYZER_CLASS = DeliveryBatchProfileAnalyzer
    TARGET_TABLE = ItemBatchProfile

    def __init__(self, config):
        super().__init__(config)
        self.use_smart_writer = True
        self.write_params = {
            "delete_before_insert": False,
            "upsert": True
        }

    def extract(self) -> pd.DataFrame:
        return fetch_orm_data(self.config, ItemDeliveryBehaviorStats)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.ANALYZER_CLASS().analyze(df)

    def target_table(self):
        return self.TARGET_TABLE