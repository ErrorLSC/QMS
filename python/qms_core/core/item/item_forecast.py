from typing import Optional
from datetime import datetime
import pandas as pd
import json

from qms_core.core.item.base_component import ItemComponentBase
from qms_core.infrastructure.db.models import ItemForecastRecord


class ItemForecast(ItemComponentBase):
    def __init__(self, itemnum: str, warehouse: str):
        super().__init__(itemnum, warehouse)
        self.forecast_series: Optional[pd.Series] = None
        self.forecast_series_json: Optional[str] = None
        self.forecast_monthly: float = 0.0
        self.model_used: Optional[str] = None

    def set_forecast_values(self, *, forecast_series: pd.Series, model_used: Optional[str] = None):
        self.forecast_series = forecast_series
        self.forecast_series_json = json.dumps(forecast_series.round(2).tolist())
        self.forecast_monthly = round(forecast_series[:4].sum(), 2)
        self.model_used = model_used
        self._loaded = True

    def load(self, session, *_, **__):
        row = session.query(ItemForecastRecord).filter_by(
            ITEMNUM=self.itemnum, Warehouse=self.warehouse
        ).first()

        if row:
            self.forecast_monthly = row.Forecast_monthly or 0.0
            self.model_used = row.ForecastModel or ""
            if row.ForecastSeriesJSON:
                try:
                    self.forecast_series = pd.Series(json.loads(row.ForecastSeriesJSON))
                    self.forecast_series_json = row.ForecastSeriesJSON
                except json.JSONDecodeError:
                    self.forecast_series = None
                    self.forecast_series_json = None

        self._loaded = True

    def load_from_dict(self, record: dict):
        self.forecast_series = record.get("ForecastSeries", pd.Series(dtype=float))
        self.forecast_series_json = record.get("ForecastSeriesJSON", "")
        self.forecast_monthly = record.get("Forecast_monthly", 0.0)
        self.model_used = record.get("ForecastModel", "")

        self._loaded = True

    def _extra_fields(self):
        return {
            "Forecast_monthly": self.forecast_monthly,
            "ForecastModel": self.model_used or "",
            "ForecastSeriesJSON": self.forecast_series_json or "",
            "ForecastDate": datetime.now()
        }

    def _to_orm(self):
        return ItemForecastRecord(
            ITEMNUM=self.itemnum,
            Warehouse=self.warehouse,
            Forecast_monthly=self.forecast_monthly,
            ForecastDate=datetime.now(),
            ForecastModel=self.model_used or "",
            ForecastSeriesJSON=self.forecast_series_json or "",
        )
    
    def _to_orm_class(self):
        return ItemForecastRecord
    
    def get_writer_config(self):
        return {
            "monitor_fields": [
                "ForecastModel", "ForecastSeriesJSON"
            ],
            "enable_logging":False,
            "exclude_fields": ["ForecastDate"],  # 避免因时间字段导致每次都更新
            "write_params": {
            "upsert": True,                  # 默认值，按主键 merge
            "delete_before_insert": False,   # ❌ 不清空整表
            "hot_zone_delete": False,
            }
        }
