from qms_core.pipelines.forecast.common import BaseItemJob
from qms_core.core.item.item_manager import ItemManager
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.core.forecast.safety_stock import SafetyStockCalculator
from qms_core.infrastructure.db.models import ItemSafetyRecord
import pandas as pd
from typing import Optional


class SafetyStockGenerationJob(BaseItemJob):
    def __init__(self, config,data_container: Optional[MRPDataContainer] = None, calculator: Optional[SafetyStockCalculator] = None):
        super().__init__(config=config,data_container=data_container)
        self.calculator = calculator or SafetyStockCalculator()

    def target_table(self):
        return ItemSafetyRecord

    def prepare_items(
        self,
        items: Optional[list] = None,
        item_ids: Optional[list[tuple[str, str]]] = None,
        data_container: Optional[MRPDataContainer] = None,
        **kwargs
    ) -> list:
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


        self.demand_df = self.data_container.demand_df

        # 下面逻辑只做字典层覆盖
        self.forecast_dict = self.data_container.forecast_dict
        self.master_dict = self.data_container.master_dict
        self.demand_type_dict = self.data_container.demand_type_dict
        self.smart_leadtime_dict = self.data_container.smart_lead_time_dict

        # ✅ 完整数据注入流程
        for item in self.data_container.items:
            item.demand.load_from_df(self.demand_df)
            item.forecast.load_from_dict(self.forecast_dict)
            item.master.load_from_dict(self.master_dict.get((item.itemnum, item.warehouse), {}))
            item.demand_type.load_from_dict(self.demand_type_dict.get((item.itemnum, item.warehouse), {}))
            item.smart_leadtime.load_from_dict(self.smart_leadtime_dict.get((item.itemnum, item.warehouse), {}))

        return self.data_container.items

    def process_items(self, items: list, **kwargs):
        for item in items:
            try:
                self.calculator.calculate_for_item(item)
            except Exception as e:
                print(f"❌ SafetyStock 失败：{item.itemnum} @ {item.warehouse}: {e}")

    def _collect_result(self, items: list) -> pd.DataFrame:
        return pd.DataFrame([
            item.safetystock.to_dict()
            for item in items if item.safetystock._loaded
        ])

    def _export_core_fields(self, result: pd.DataFrame) -> dict:
        return {
            (row["ITEMNUM"], row["Warehouse"]): {
                "FinalSafetyStock": row["FinalSafetyStock"],
                "RecommendedServiceLevel": row["RecommendedServiceLevel"]
            }
            for _, row in result.iterrows()
        }
