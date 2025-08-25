from qms_core.infrastructure.config import MRPConfig
from qms_core.core.forecast.ETA.extractor import ETAExtractor
from qms_core.core.forecast.ETA.calculator import ETATransformer
from qms_core.core.forecast.ETA.service import build_mode_predictor
from qms_core.pipelines.forecast.ETA.ETA_forecast_job import ETAForecastJob
import pandas as pd
from typing import Optional

def prepare_data()->dict:

    config = MRPConfig()

    extractor = ETAExtractor(config=config)

    data_dict = extractor.fetch()
    return data_dict

def prepare_transfomer(data_dict:dict[str,pd.DataFrame]) -> ETATransformer:
    predictor = build_mode_predictor(df_vendor_stats=data_dict["vendor_transport_stat"],df_history=data_dict["history"])
    
    transformer = ETATransformer(lead_metric="Q60",predictor=predictor)

    return transformer

def preprocess(transformer:ETATransformer,data_dict:dict) -> tuple:
    df_intransit, dict_cases, last_delivery_dict = transformer.prepare_and_route_intransit(data=data_dict)
    return df_intransit, dict_cases, last_delivery_dict

def test_confirmed(transformer:ETATransformer,df_confirmed:pd.DataFrame)-> None:
    df_confirmed = transformer.transform_confirmed(df_confirmed=df_confirmed)
    print(df_confirmed)
    
def test_shipped(transformer:ETATransformer,df_shipped:pd.DataFrame,df_vendor_lt:pd.DataFrame,df_vendor_master:pd.DataFrame)-> pd.DataFrame:
    df_shipped = transformer.transform_shipped(df_shipped=df_shipped,df_vendor_lt=df_vendor_lt,df_vendor_master=df_vendor_master)
    print(df_shipped)
    return df_shipped

def test_single(transformer:ETATransformer,df_single:pd.DataFrame)->pd.DataFrame:
    df_single = transformer.simulate_single_eta(df=df_single)
    print(df_single)
    return df_single

def test_tailed(transformer:ETATransformer,df_tail:pd.DataFrame,df_behavior:pd.DataFrame,last_delivery_dict:dict,df_full:Optional[pd.DataFrame],df_vendor_lt:pd.DataFrame,df_vendor_master:pd.DataFrame)->pd.DataFrame:
    df_tail = transformer.simulate_tail_eta(df_tail=df_tail,df_behavior=df_behavior,last_delivery_dict=last_delivery_dict,df_full=df_full,df_vendor_lt=df_vendor_lt,df_vendor_master=df_vendor_master)
    print(df_tail)
    return df_tail

def test_batch_shipment(transformer:ETATransformer,df_batch:pd.DataFrame,df_vendor_lt:pd.DataFrame,df_vendor_master:pd.DataFrame):
    df_batch = transformer.simulate_batch_eta(df=df_batch,df_vendor_lt=df_vendor_lt,df_vendor_master=df_vendor_master)
    print(df_batch)
    return df_batch

def test_ETA_job(config:MRPConfig,dry_run:bool=True,lead_metric:str="Q60"):
    job = ETAForecastJob(config=config,lead_metric=lead_metric)
    df = job.run(dry_run=dry_run)
    return df


if __name__ == "__main__":
    # data_dict = prepare_data()
    # transformer = prepare_transfomer(data_dict=data_dict)
    # final_df=transformer.transform(data=data_dict)
    # print(final_df)
    # final_df.to_excel("ETA_final.xlsx",index=False)
    # df_intransit, dict_cases, last_delivery_dict = preprocess(transformer=transformer,data_dict=data_dict)
    # print(dict_cases["SplitInProgress"])
    # test_confirmed(transformer=transformer,df_confirmed = dict_cases["Confirmed"])
    # test_shipped(transformer=transformer,df_shipped=dict_cases["Shipped"],df_vendor_lt=data_dict["vendor_transport_stat"],df_vendor_master=data_dict["vendor_master"])
    # test_single(transformer=transformer,df_single=dict_cases["SingleDelivery"])
    # test_tailed(transformer=transformer,df_tail=dict_cases["SplitInProgress"],
    #             last_delivery_dict=last_delivery_dict, df_behavior=data_dict["delivery_behavior"],df_full=data_dict["intransit"],
    #             df_vendor_lt=data_dict["vendor_transport_stat"],df_vendor_master=data_dict["vendor_master"])
    # test_batch_shipment(transformer=transformer,df_batch=dict_cases["LikelySplit"],df_vendor_lt=data_dict["vendor_transport_stat"],df_vendor_master=data_dict["vendor_master"])
    config = MRPConfig()
    ETA_df = test_ETA_job(config=config,dry_run=False,lead_metric="Q60")
    # ETA_df.to_excel("ETA_final.xlsx",index=False)
    # print(ETA_df)