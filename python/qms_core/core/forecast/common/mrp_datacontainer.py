from qms_core.core.item.item import Item
from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
import pandas as pd

class MRPDataContainer:
    """
    MRP 预加载数据容器
    - 仅负责承载预加载好的输入数据
    - 供 Forecast / Safety / MRP / Evaluation 统一复用
    """
    def __init__(self, items:list[Item],demand_df:pd.DataFrame, forecast_dict:dict, inventory_dict:dict, master_dict:dict,
                 demand_type_dict:dict, safety_stock_dict:dict, smart_lead_time_dict:dict):
        self.items = items
        self.demand_df = demand_df
        self.forecast_dict = forecast_dict
        self.inventory_dict = inventory_dict
        self.master_dict = master_dict
        self.demand_type_dict = demand_type_dict
        self.safety_stock_dict = safety_stock_dict
        self.smart_lead_time_dict = smart_lead_time_dict

    @classmethod
    def from_preloader(cls, preloader:ItemDataPreloader, items:list[Item]):
        return cls(
            items=items,
            demand_df=preloader.load_demand_history(),
            forecast_dict=preloader.load_forecast_series(),
            inventory_dict=preloader.load_inventory_info(),
            master_dict=preloader.load_item_master_info(),
            demand_type_dict=preloader.load_demand_type(),
            safety_stock_dict=preloader.load_safety_stock(),
            smart_lead_time_dict=preloader.load_smart_lead_time(),
        )