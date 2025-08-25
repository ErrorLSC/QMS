from qms_core.core.forecast.stock_simulator.simulator_extractor import StockSimulatorExtractor
from qms_core.core.forecast.stock_simulator.simulator_transformer import StockSimulatorTransformer
import pandas as pd
from qms_core.infrastructure.config import MRPConfig

def prepare_data(config:MRPConfig)->dict[str,pd.DataFrame]:

    extractor = StockSimulatorExtractor(config=config)

    data_dict = extractor.fetch()
    return data_dict

def prepare_transformer()->StockSimulatorTransformer:
    transformer = StockSimulatorTransformer()
    return transformer

if __name__ == "__main__":
    config = MRPConfig()
    data_dict = prepare_data(config=config)
    transformer = prepare_transformer()
    result_df = transformer.transform(data_dict=data_dict)
    print(result_df)
    # print(data_dict)