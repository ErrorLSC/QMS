import pandas as pd
import numpy as np
from datetime import datetime
from qms_core.core.common.params.enums import OrderReason,TransportMode
from qms_core.core.common.params.ParasCenter import ParasCenter,MRPParamsSchema
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer
from typing import Optional
from qms_core.core.item.item import Item

class MRPCalculator:
    def __init__(
        self,
        data_container: Optional[MRPDataContainer] = None,
        params: Optional[MRPParamsSchema] = None,
        item=None,    # 仍保留单物料入口（兼容单测场景）
        use_vectorized: bool = True
    ):
        self.params = params or ParasCenter().mrp_params
        self.use_vectorized = use_vectorized

        self.is_single = item is not None
        self.item = item
        self.data_container = data_container

        # 扩展模式识别
        if not self.is_single and data_container:
            self.items = data_container.items

    def run(self):
        if self.is_single:
            return self._run_single(self.item)
        elif self.use_vectorized:
            return self.run_batch()
        else:
            results = []
            for item in self.items:
                row = self._run_single(item)
                if row:
                    results.append(row)
            return pd.DataFrame(results)

    def _run_single(self, item:Item):
        if not item.forecast or item.forecast.forecast_series is None:
            return None
        if not item.safetystock:
            return None

        moq = max(item.master.moq or 1, 1)
        lead_days = item.master.lead_time or 0
        lead_weeks = self._get_lead_weeks(item)

        fc_series = item.forecast.forecast_series.copy()
        if fc_series.empty:
            fc_series = pd.Series(dtype=float)
        if len(fc_series) < lead_weeks:
            last_val = fc_series.iloc[-1] if not fc_series.empty else 0.0
            pad = pd.Series([last_val] * (lead_weeks - len(fc_series)), index=range(len(fc_series), lead_weeks))
            fc_series = pd.concat([fc_series, pad])
        forecast = fc_series.head(lead_weeks).sum()

        dyn_ss = item.safetystock.dynamic_safety_stock or 0
        manual_ss = item.master.safety_stock or 0
        final_ss = item.safetystock.final_safety_stock or max(dyn_ss, manual_ss)

        stock = item.inventory.available_stock or 0
        transit = item.inventory.in_transit_stock or 0
        net = forecast + final_ss - stock - transit

        qty = 0
        if net > 0:
            qty = net
            if self.params.use_moq:
                moq = max(moq, self.params.min_moq)
                qty = max(qty, moq)
            if self.params.use_wlot:
                wlot = max(item.master.wlot or 1, 1)
                qty = np.ceil(qty / wlot) * wlot if self.params.round_up_to_wlot else round(qty / wlot) * wlot

        if qty <= 0 and not self.params.include_zero_qty:
            return None

        order_reason = None
        dt = item.demand_type.demand_type
        act = item.demand_type.activity_level
        if act in ["Dormant", "Inactive"]:
            if stock + transit < 0:
                order_reason = OrderReason.ON_DEMAND.value
            elif net > 0 and forecast < 1e-5:
                order_reason = OrderReason.SAFETY_TOP_UP.value
        elif qty > 0:
            order_reason = OrderReason.REPLENISH_AND_ON_DEMAND.value if stock < transit else OrderReason.REPLENISH.value

        return {
            "Warehouse": item.warehouse,
            "ITEMNUM": item.itemnum,
            "MOQ": moq,
            "TransportMode": TransportMode.DEFAULT.value,
            "WLEAD": lead_days,
            "ManualSafetyStock": manual_ss,
            "CXPPLC": item.master.plc,
            "ITEMDESC": item.master.idesc,
            "IVEND": item.master.vendor_code,
            "VNDNAM": item.master.vendor_name,
            "AvailableStock": stock,
            "IntransitStock": transit,
            "DemandType": dt,
            "ActivityLevel": act,
            "RecommendedServiceLevel": item.safetystock.recommended_service_level,
            "Forecast_within_LT": forecast,
            "DynamicSafetyStock": dyn_ss,
            "FinalSafetyStock": final_ss,
            "NetRequirement": net,
            "RecommendedQty": int(qty),
            "OrderReason": order_reason,
            "Algorithm": "Static",
            "CalcDate": datetime.now()
        }

    def run_batch(self):
        rows = [self._build_row(item) for item in self.items]
        rows = [r for r in rows if r]
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)

        return self._postprocess_dataframe(df)

    def _build_row(self, item:Item):
        key = (item.itemnum, item.warehouse)
        fc = self.data_container.forecast_dict.get(key, {})
        inv = self.data_container.inventory_dict.get(key, {})
        master = self.data_container.master_dict.get(key, {})
        dt = self.data_container.demand_type_dict.get(key, {})
        ss = self.data_container.safety_stock_dict.get(key, {})

        fc_series = fc.get("ForecastSeries")
        if fc_series is None or fc_series.empty or not ss:
            return None

        lead_days = master.get("lead_time") or 0
        lead_weeks = self._get_lead_weeks(item)
        fc_series = fc_series.copy()
        if len(fc_series) < lead_weeks:
            last_val = fc_series.iloc[-1] if not fc_series.empty else 0.0
            pad = pd.Series([last_val] * (lead_weeks - len(fc_series)), index=range(len(fc_series), lead_weeks))
            fc_series = pd.concat([fc_series, pad])
        forecast = fc_series.head(lead_weeks).sum()
        transport_mode = TransportMode.DEFAULT.value

        return {
            "ITEMNUM": item.itemnum,
            "Warehouse": item.warehouse,
            "Forecast_within_LT": forecast,
            "AvailableStock": inv.get("AvailableStock", 0.0),
            "IntransitStock": inv.get("IntransitStock", 0.0),
            "WLEAD": lead_days,
            "LotSize": master.get("lot_size") or 1,
            "MOQ": master.get("moq") or 1,
            "TransportMode": transport_mode,
            "ManualSafetyStock": master.get("safety_stock", 0.0),
            "DynamicSafetyStock": ss.get("DynamicSafetyStock", 0.0),
            "FinalSafetyStock": ss.get("FinalSafetyStock", max(ss.get("DynamicSafetyStock", 0.0), master.get("safety_stock", 0.0))),
            "RecommendedServiceLevel": ss.get("RecommendedServiceLevel"),
            "DemandType": dt.get("DemandType"),
            "ActivityLevel": dt.get("ActivityLevel"),
            "CXPPLC": master.get("plc"),
            "ITEMDESC": master.get("idesc"),
            "IVEND": master.get("vendor_code"),
            "VNDNAM": master.get("vendor_name"),
        }

    def _postprocess_dataframe(self, df):
        df["NetRequirement"] = df["Forecast_within_LT"] + df["FinalSafetyStock"] - df["AvailableStock"] - df["IntransitStock"]
        moq = np.maximum(df["MOQ"], self.params.min_moq if self.params.use_moq else 1)
        qty = df["NetRequirement"].clip(lower=0)

        positive_demand_mask = qty > 0

        if self.params.use_moq:
            moq = np.maximum(df["MOQ"], self.params.min_moq)
            qty[positive_demand_mask] = np.maximum(qty[positive_demand_mask], moq[positive_demand_mask])

        if self.params.use_wlot:
            wlot = df["LotSize"].replace(0, 1)
            qty[positive_demand_mask] = np.where(
                self.params.round_up_to_wlots,
                np.ceil(qty[positive_demand_mask] / wlot[positive_demand_mask]) * wlot[positive_demand_mask],
                np.round(qty[positive_demand_mask] / wlot[positive_demand_mask]) * wlot[positive_demand_mask]
            )
        df["RecommendedQty"] = np.rint(qty).astype(int)
        df["CalcDate"] = datetime.now()

        df["OrderReason"] = None
        cond1 = (df["ActivityLevel"].isin(["Dormant", "Inactive"])) & ((df["AvailableStock"] + df["IntransitStock"]) < 0)
        cond2 = (df["ActivityLevel"].isin(["Dormant", "Inactive"])) & (df["NetRequirement"] > 0) & (df["Forecast_within_LT"] < 1e-5)
        cond3 = (df["RecommendedQty"] > 0) & (df["AvailableStock"] < df["IntransitStock"])
        cond4 = (df["RecommendedQty"] > 0)

        df.loc[cond1, "OrderReason"] = OrderReason.ON_DEMAND.value
        df.loc[cond2, "OrderReason"] = OrderReason.SAFETY_TOP_UP.value
        df.loc[cond3, "OrderReason"] = OrderReason.REPLENISH_AND_ON_DEMAND.value
        df.loc[(~cond1 & ~cond2 & cond4 & df["OrderReason"].isna()), "OrderReason"] = OrderReason.REPLENISH.value

        if not self.params.include_zero_qty:
            df = df[df["RecommendedQty"] > 0]

        df["Algorithm"] = "Static"

        return df[[
            "Warehouse", "ITEMNUM", "MOQ","TransportMode" ,"WLEAD", "ManualSafetyStock", "CXPPLC", "ITEMDESC", "IVEND", "VNDNAM",
            "AvailableStock", "IntransitStock", "DemandType", "ActivityLevel", "RecommendedServiceLevel",
            "Forecast_within_LT", "DynamicSafetyStock", "FinalSafetyStock", "NetRequirement",
            "RecommendedQty", "OrderReason", "Algorithm","CalcDate"
        ]]
    

    def _get_lead_weeks(self, item:Item) -> int:
        lead_days = item.master.lead_time or 0
        return max(round(lead_days / 7), 1)
