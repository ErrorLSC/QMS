from qms_core.core.common.params.enums import VendorType,TransportMode
import pandas as pd
import pyodbc
import warnings

def map_vendor_type(vtype, vendor):
    if vendor in (99908, 99909):
        return VendorType.INTERNAL.value
    if vtype == 'EDIC':
        return VendorType.OVERSEA_DC.value
    if vtype == 'LOCC':
        return VendorType.DOMESTIC_EXTERNAL.value
    if vtype in ('ACGC', 'EXTC'):
        return VendorType.OVERSEA_EXTERNAL.value
    return VendorType.DOMESTIC_EXTERNAL.value

def map_transport_mode(sttptc):
    try:
        if pd.isna(sttptc):
            return TransportMode.TRUCK.value
        sttptc = int(sttptc)
        if sttptc in (53, 50, 60):
            return TransportMode.AIR.value
        elif sttptc == 55:
            return TransportMode.COURIER.value
        elif sttptc == 20:
            return TransportMode.VESSEL.value
        elif sttptc == 40:
            return TransportMode.TRUCK.value
    except:
        pass
    return TransportMode.TRUCK.value

def determine_lead_time(mode, stnmdy):
    if mode == TransportMode.TRUCK.value:
        return 3
    return int(stnmdy) if pd.notna(stnmdy) else None

def strip_and_convert(x):
    if isinstance(x, str):
        return x.strip()
    elif isinstance(x, float) and x.is_integer():
        return int(x)
    else:
        return x
    
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query
    
def convert_db2_dates(df, cols):
    """将 DB2 返回的整数型日期字段（如 20241101）转为 datetime 类型"""
    for col in cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col].astype(str), format="%Y%m%d", errors='coerce')
            except Exception as e:
                print(f"列 {col} 转换失败: {e}")
    return df
    
def BPCSquery(SQL_query,dsn,system="EPISBE20",date_column_list=None):
    """
    从 DB2（BPCS）系统中查询数据，并可选进行日期字段转换。
    自动屏蔽 pandas 对非 SQLAlchemy 连接对象的 UserWarning。
    """

    conn_str = (
        "DRIVER={IBM i Access ODBC Driver};"
        f"DSN = {dsn};"
        f"system = {system};"
        "Trusted_Connection=yes;"
    )

    with pyodbc.connect(conn_str) as conn:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_sql(SQL_query, conn)

    df = df.map(strip_and_convert)
    
    if date_column_list:
        if isinstance(date_column_list, str):
            date_column_list = [date_column_list]
        df = convert_db2_dates(df, date_column_list)
    return df