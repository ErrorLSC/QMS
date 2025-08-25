from qms_core.core.route.route_selector import RouteSelector
import pandas as pd
from qms_core.infrastructure.db.models import VendorMaster,VendorTransportStats,ItemTransportPreference,IIM,IWI,ItemPrepareLTStats
from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.infrastructure.config import MRPConfig

def prepare_data(config:MRPConfig):
    vendor_df = fetch_orm_data(config=config,orm_class=VendorMaster)
    stats_df = fetch_orm_data(config=config,orm_class=VendorTransportStats)
    pref_df = fetch_orm_data(config=config,orm_class=ItemTransportPreference)
    iim_df = fetch_orm_data(config=config,orm_class=IIM)
    iwi_df = fetch_orm_data(config=config,orm_class=IWI)
    prepare_df = fetch_orm_data(config=config,orm_class=ItemPrepareLTStats)
    return {"vendor_master":vendor_df,"transport_stat":stats_df,"item_transport_pref":pref_df,
            "IIM":iim_df,"item_prepare":prepare_df,"IWI":iwi_df}

def candidate_output(item:str,warehouse:str,selector:RouteSelector,data_dict:dict[str:pd.DataFrame]):
    transport_df = selector.get_candidate_routes(ITEMNUM=item,Warehouse=warehouse,stats_df=data_dict["transport_stat"],vendor_df=data_dict["vendor_master"],pref_df=data_dict["item_transport_pref"],iim_df=data_dict["IIM"])
    # prepare_enriched_df = selector.enrich_with_prepare_time(df_routes=transport_df,iwi_df=data_dict["IWI"],prepare_df=data_dict["item_prepare"])
    return transport_df

def rich_select(item:str,warehouse:str,selector:RouteSelector):
    result = selector.select(ITEMNUM=item,Warehouse=warehouse)
    return result

if __name__ == "__main__":
    config = MRPConfig()
    data_dict = prepare_data(config=config)
    selector_blank = RouteSelector()
    selector_rich = RouteSelector(iim_df=data_dict["IIM"],stats_df=data_dict["transport_stat"],
                                  vendor_df=data_dict["vendor_master"],pref_df=data_dict["item_transport_pref"],
                                  prepare_df=data_dict["item_prepare"],iwi_df=data_dict["IWI"])
    # routes = candidate_output(item='6060008039',warehouse='5',selector=selector_blank,data_dict=data_dict)
    routes = rich_select(item="90502308",warehouse="6",selector=selector_rich)
    print(routes)
