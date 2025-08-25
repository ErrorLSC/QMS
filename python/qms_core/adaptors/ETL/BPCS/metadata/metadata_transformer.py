import pandas as pd
from qms_core.core.common.base_transformer import BaseTransformer
import re
from qms_core.adaptors.ETL.BPCS.common.utils import (
    map_vendor_type,
    map_transport_mode,
    determine_lead_time
)

class IWITransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "WAREHOUSE": "Warehouse"
        })

class ILMTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._standardize_fields(df)
        df = self._filter_valid_xyz(df)
        df = self._split_xyz_coordinates(df)
        df = self._select_desired_columns(df)
        return df

    def _standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "WWHS": "Warehouse"
        })

    def _filter_valid_xyz(self, df: pd.DataFrame) -> pd.DataFrame:
        pattern = r'^\s*-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?\s*$'
        df = df.copy()
        df = df.drop_duplicates(subset=['Warehouse', 'WLOC'], keep='first')
        df = df[df['WDESC'].apply(lambda s: isinstance(s, str) and re.match(pattern, s) is not None)]
        return df

    def _split_xyz_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[['X', 'Y', 'Z']] = (
            df['WDESC']
            .str.replace(' ', '', regex=False)
            .str.split(',', expand=True)
            .astype(float)
        )
        return df

    def _select_desired_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ['Warehouse', 'WLOC', 'WDESC', 'X', 'Y', 'Z', 'WLTYP', 'WZONE', 'VOLCAP', 'WEIGHTCAP']
        return df[columns].copy()

class AVMTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df["VendorCode"] = df["VENDOR"].astype(str)
        df["VendorName"] = df["VENDORNAME"].str.strip()
        df["VendorType"] = df.apply(lambda row: map_vendor_type(row["VTYPE"], row["VENDOR"]), axis=1)
        df["TransportMode"] = df["STTPTC"].apply(map_transport_mode)
        df["TransportLeadTimeDays"] = df.apply(lambda row: determine_lead_time(row["TransportMode"], row["STNMDY"]), axis=1)
        df["IS_ACTIVE"] = "Y"

        df = df.sort_values(by=["VendorCode", "TransportMode", "TransportLeadTimeDays"])
        df = df.drop_duplicates(subset=["VendorCode", "TransportMode"], keep="first")

        df_final = df[[
            "VendorCode", "VendorName", "TransportMode",
            "TransportLeadTimeDays", "VendorType", "IS_ACTIVE"
        ]]

        return df_final

class GCCTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "CCNVDT": "ValidDate",
            "CCNVFC": "ExchangeRate",
            "CCFRCR": "FromCurrency",
            "CCTOCR": "ToCurrency",
            "CCRTYP": "UsageType",
            "CCMETH": "ExchangeMethod"
        })

