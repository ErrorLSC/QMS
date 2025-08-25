from qms_core.infrastructure.db.models import STKOHAvail,VirtualStockTransaction
from qms_core.infrastructure.db.reader import fetch_orm_data
from qms_core.core.common.base_extractor import BaseExtractor
import pandas as pd

class StockSimulatorExtractor(BaseExtractor):
    def __init__(self, config):
        self.config = config
    
    def fetch_stock(self)->pd.DataFrame:
        return fetch_orm_data(self.config, STKOHAvail,columns=["ITEMNUM","Warehouse","AVAIL"])

    def fetch_transaction(self) ->pd.DataFrame:
        return fetch_orm_data(self.config,VirtualStockTransaction)

    def fetch(self) -> dict[str,pd.DataFrame]:
        """
        返回格式:
        {
            "stock": stock_df,  
            "transaction":transaction_df
        }
        """
        stock_df = self.fetch_stock()
        transaction_df = self.fetch_transaction()
        return {
            "stock": stock_df,
            "transaction": transaction_df
        }