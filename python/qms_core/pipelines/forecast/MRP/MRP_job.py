from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.pipelines.forecast.common import BaseItemJob
from qms_core.core.forecast.MRP.MRP_calculator import MRPCalculator
from qms_core.infrastructure.db.models import MRPOrder
from typing import Optional
import pandas as pd
from qms_core.core.item.item_manager import ItemManager
from qms_core.core.common.params.loader_params import LoaderParams

class MRPJob(BaseItemJob):
    def __init__(
        self, 
        config,
        data_container: Optional[MRPDataContainer] = None,
        use_vectorized: bool = True,
        calculator: Optional[MRPCalculator] = None,
        load_params: Optional[LoaderParams] = None,
    ):
        super().__init__(config=config,data_container=data_container,load_params=load_params)
        self.use_vectorized = use_vectorized
        self.calculator = calculator
        self.result_df = None

    def target_table(self):
        return MRPOrder

    def prepare_items(
            self,
            items: Optional[list] = None,
            item_ids: Optional[list[tuple[str, str]]] = None,
            **kwargs
        ):
    # ✅ 优先检查 calculator 是否已准备好（已包含数据）
        if self.calculator:
            self.data_container = self.calculator.data_container
            return self.data_container.items

        # ✅ 其次检查是否已注入 data_container
        if self.data_container:
            pass
        else:
            # 否则内部兜底预加载
            manager = ItemManager(item_ids) if item_ids else ItemManager.from_demand_history(self.config)
            self.items = manager.items
            preloader = ItemDataPreloader(self.config, self.items)
            self.data_container = MRPDataContainer.from_preloader(preloader, self.items)

        # ✅ 只有在 calculator 尚未提供时才新建 calculator
        self.calculator = MRPCalculator(
            data_container=self.data_container,
            use_vectorized=self.use_vectorized,
        )

        return self.data_container.items
        
    def process_items(self, items: list, **kwargs):
        self.result_df = self.calculator.run()
        return

    def _collect_result(self, items: list) -> pd.DataFrame:
        return self.calculator.run()

    def _export_core_fields(self, result: pd.DataFrame) -> dict:
        return {
            (row["ITEMNUM"], row["Warehouse"]): {
                "RecommendedQty": row["RecommendedQty"],
                "OrderReason": row["OrderReason"],
            }
            for _, row in result.iterrows()
        }