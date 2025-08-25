from qms_core.core.forecast.MRP.MRP_calculator import MRPCalculator
import pandas as pd
from qms_core.core.common.params.enums import TransportMode
from qms_core.core.common.params.MRPParams import MRPParamsSchema
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from typing import Optional
from qms_core.core.item.item import Item

class DynamicMRPCalculator(MRPCalculator):
    def __init__(
        self, 
        data_container: Optional[MRPDataContainer]=None,
        dynamic_lt_dict: Optional[dict] = None,
        params: Optional[MRPParamsSchema] = None,
        item:Item=None,
        use_vectorized: bool = True
    ):
        super().__init__(
            data_container=data_container,
            params=params,
            item=item,
            use_vectorized=use_vectorized
        )
        self.dynamic_lt_dict = dynamic_lt_dict or {}

    def _get_smart_leadtime_days(self, item:Item) -> Optional[int]:
        key = (item.itemnum, item.warehouse)

        # ✅ 优先从 DataContainer 读
        if self.data_container:
            slt_dict = self.data_container.smart_lead_time_dict.get(key, {})
            lead_days = slt_dict.get("Q60LeadTime")
            if lead_days:
                return int(lead_days)

        # ✅ 其次看是否存在 dynamic_lt_dict 兜底
        lead_days = self.dynamic_lt_dict.get(key)
        if lead_days:
            return int(lead_days)

        # ✅ （可选）最后 fallback 到 item 级属性 (兼容单物料调用场景)
        slt = getattr(item, "smart_leadtime", None)
        if slt and getattr(slt, "total_days", None):
            return int(slt.total_days)

        return None

    def _get_lead_weeks(self, item:Item) -> int:
        lead_days = self._get_smart_leadtime_days(item)
        if lead_days:
            return max(int(lead_days) // 7, 1)
        return super()._get_lead_weeks(item)

    def _build_row(self, item:Item):
        lead_days = self._get_smart_leadtime_days(item)
        if lead_days is None:
            return None

        # 直接沿用父类计算逻辑
        row = super()._build_row(item)
        if row is None:
            return None

        row["WLEAD"] = lead_days

        # TransportMode 仍然可以从 smart_leadtime 里补充
        key = (item.itemnum, item.warehouse)
        slt_dict = self.data_container.smart_lead_time_dict.get(key, {})
        # print(slt_dict)
        transport_mode = slt_dict.get("TransportMode", TransportMode.DEFAULT)
        row["TransportMode"] = transport_mode
        return row

    def _postprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super()._postprocess_dataframe(df)
        df["Algorithm"] = "Dynamic"
        return df