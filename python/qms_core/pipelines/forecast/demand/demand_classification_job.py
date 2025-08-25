import pandas as pd
from qms_core.core.item.item_manager import ItemManager
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.core.forecast.demand.classifier import DemandClassifier
from qms_core.infrastructure.db.models import DemandType
from typing import Optional
from qms_core.pipelines.forecast.common import BaseItemJob


class DemandClassificationJob(BaseItemJob):
    def __init__(self, config,data_container: Optional[MRPDataContainer] = None, classifier: Optional[DemandClassifier] = None):
        super().__init__(config=config,data_container=data_container)
        self.classifier = classifier or DemandClassifier()

    def target_table(self):
        return DemandType
    
    def prepare_items(
        self,
        items: Optional[list] = None,
        item_ids: Optional[list[tuple[str, str]]] = None,
        data_container: Optional[MRPDataContainer] = None,
        **kwargs
    ):
        if self.data_container:  # 优先使用 self.data_container（通常在 __init__ 里注入）
            pass
        elif data_container:
            self.data_container = data_container
        else:
            if not items:
                manager = ItemManager(item_ids) if item_ids else ItemManager.from_demand_history(self.config)
                items = manager.items
            preloader = ItemDataPreloader(self.config, items)
            self.data_container = MRPDataContainer.from_preloader(preloader, items)

        # 只取你需要的子集数据
        self.demand_df = self.data_container.demand_df
        self.master_dict = self.data_container.master_dict

        for item in self.data_container.items:
            item.demand.load_from_df(self.demand_df)
            item.master.load_from_dict(self.master_dict.get((item.itemnum, item.warehouse), {}))

        return self.data_container.items

    def process_items(self, items: list, **kwargs):
        for item in items:
            try:
                self.classifier.calculate_for_item(item)
            except Exception as e:
                item.demand_type._loaded = False  # 明确标记为失败
                print(f"❌ 分类失败 {item.itemnum} @ {item.warehouse}: {e}")

    def _collect_result(self, items: list) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "ITEMNUM": item.itemnum,
                "Warehouse": item.warehouse,
                **item.demand_type.to_dict()
            }
            for item in items if item.demand_type._loaded
        ])

    def _export_core_fields(self, result: pd.DataFrame) -> dict:
        return {
            (row["ITEMNUM"], row["Warehouse"]): {
                "DemandType": row["DemandType"],
                "ActivityLevel": row["ActivityLevel"],
                "WeeksWithDemand": row.get("WeeksWithDemand"),
                "ZeroRatio": row.get("ZeroRatio"),
                "CV": row.get("CV"),
                "TrendSlope": row.get("TrendSlope"),
                "SeasonalStrength": row.get("SeasonalStrength"),
            }
            for _, row in result.iterrows()
        }