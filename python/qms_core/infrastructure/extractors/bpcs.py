import pyodbc
import pandas as pd
import warnings

class BPCSConnector:
    def __init__(self, dsn: str, system: str = "EPISBE20"):
        self.dsn = dsn
        self.system = system

    def _build_connection_string(self) -> str:
        conn_str =  (
            "DRIVER={IBM i Access ODBC Driver};"
            f"DSN = {self.dsn};"
            f"system = {self.system};"
            "Trusted_Connection = yes;"
        )
        # print(f"[DEBUG] Connection string: {conn_str}")
        return conn_str

    def query(self, sql: str, date_columns: list[str] = None) -> pd.DataFrame:
        conn_str = self._build_connection_string()
        with pyodbc.connect(conn_str) as conn:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                df = pd.read_sql(sql, conn)

        df = df.map(self._strip_and_convert)
        if date_columns:
            df = self._convert_db2_dates(df, date_columns)
        return df

    @staticmethod
    def _strip_and_convert(x):
        if isinstance(x, str):
            return x.strip()
        elif isinstance(x, float) and x.is_integer():
            return int(x)
        else:
            return x

    @staticmethod
    def _convert_db2_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        for col in cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col].astype(str), format="%Y%m%d", errors='coerce')
                except Exception as e:
                    print(f"列 {col} 转换失败: {e}")
        return df
