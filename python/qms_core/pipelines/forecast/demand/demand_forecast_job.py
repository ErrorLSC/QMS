import pandas as pd
from typing import Optional, Union
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from qms_core.core.item.item_manager import ItemManager
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.core.forecast.demand.calculator import DemandForecaster
from qms_core.infrastructure.db.models import ItemForecastRecord
from qms_core.pipelines.forecast.common import BaseItemJob
from qms_core.core.item.item import Item


class ForecastGenerationJob(BaseItemJob):
    def __init__(self, config,data_container: Optional[MRPDataContainer] = None,forecaster: Optional[DemandForecaster] = None):
        super().__init__(config,data_container=data_container)
        self.forecaster = forecaster or DemandForecaster()

    def target_table(self):
        return ItemForecastRecord
    
    def prepare_items(
        self,
        items: Optional[list[Item]] = None,
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
        self.demand_type_dict = self.data_container.demand_type_dict

        for item in self.data_container.items:
            item.demand.load_from_df(self.demand_df)
            item.demand_type.load_from_dict(self.demand_type_dict.get((item.itemnum, item.warehouse), {}))

        return self.data_container.items
    
    def run(
        self,
        items: Optional[list[Item]] = None,
        item_ids: Optional[list[tuple[str, str]]] = None,
        dry_run: bool = False,
        return_output: bool = False,
        session=None,
        **kwargs
        ) -> Union[pd.DataFrame, dict]:
        items = self.prepare_items(items=items, item_ids=item_ids, **kwargs)
        self.process_items(items, **kwargs)
        df_result = self._collect_result(items)

        if dry_run:
            self._dry_run_preview(df_result)

        if not dry_run:
            self.write_result(df_result, dry_run=False, session=session)

        if dry_run or return_output:
            core_result = self._export_core_fields(items)  # ⬅️ 用 Item list，不是 DataFrame
            return {
                "df": df_result,
                "core": core_result,
                "items": items,
            }

    def process_items(self, items: list[Item], **kwargs):
        for item in items:
            try:
                self.forecaster.forecast_item(item)
            except Exception as e:
                print(f"❌ Forecast 失败：{item.itemnum} @ {item.warehouse}: {e}")

    def _collect_result(self, items: list[Item]) -> pd.DataFrame:
        return pd.DataFrame([
            item.forecast.to_dict()
            for item in items if item.forecast._loaded
        ])

    def _export_core_fields(self, items:list[Item]) -> dict:
        export = {}
        for item in items:
            if not item.forecast._loaded:
                continue
            key = (item.itemnum, item.warehouse)
            export[key] = {
                "ForecastSeries": item.forecast.forecast_series,
                "ForecastSeriesJSON": item.forecast.forecast_series_json,
                "Forecast_monthly": item.forecast.forecast_monthly,
                "ForecastModel": item.forecast.model_used
            }
        return export