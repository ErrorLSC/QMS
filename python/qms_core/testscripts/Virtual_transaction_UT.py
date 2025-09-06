from qms_core.core.forecast.transaction.virtual_transaction_extractor import VirtualTransactionExtractor
from qms_core.core.forecast.transaction.virtual_transaction_transformer import VirtualTransactionTransformer
import pandas as pd
from qms_core.infrastructure.config import MRPConfig
from qms_core.pipelines.forecast.virtual_transaction.virtual_transaction_job import VirtualTransactionJob

def prepare_data()->dict:

    config = MRPConfig()

    extractor = VirtualTransactionExtractor(config=config)

    data_dict = extractor.fetch()
    return data_dict

def prepare_transformer() -> VirtualTransactionTransformer:
    transformer = VirtualTransactionTransformer()
    return transformer

def turn_on_transformer(transformer:VirtualTransactionTransformer,data_dict:dict)->pd.DataFrame:
    df_result = transformer.transform(data_dict=data_dict)
    return df_result

def execute_job(config:MRPConfig,dry_run=True)-> pd.DataFrame:
    job = VirtualTransactionJob(config=config)
    df = job.run(dry_run=dry_run)
    return df

if __name__ == "__main__":
    # data_dict = prepare_data()
    # print(data_dict)
    # transformer = prepare_transformer()
    # df_result = turn_on_transformer(transformer=transformer,data_dict=data_dict)
    # print(df_result)
    # df_result.to_excel("virtual_transaction.xlsx",index=False)
    config = MRPConfig()
    df = execute_job(config=config,dry_run=True)
    # print(df)