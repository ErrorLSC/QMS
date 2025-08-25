from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer
from qms_core.core.common.params.enums import TransportMode
import pandas as pd
import numpy as np
from datetime import datetime


class SmartLeadtimeAnalyzer(BaseAnalyzer):
    """
    智能交期分析器：
    - 结合准备期 + 动态运输期 + 静态 vendor 表
    - 输出完整交期估计 + 来源等级 + 空运标志
    """

    REQUIRED_COLUMNS = [
        "ITEMNUM", "Warehouse", "VendorCode", "TransportMode",
        "Q60PrepDays", "MeanPrepDays", "ModePrepDays", "Q90PrepDays", "ExpSmoothPrepDays", "PrepStd",
        "Q60TransportLeadTime", "MeanTransportLeadTime", "ModeTransportLeadTime", "Q90TransportLeadTime", "SmoothedTransportLeadTime", "TransportLeadTimeStd",
        "SampleCount_x", "SampleCount_y",  # from df_prep & df_trans
        "StaticLeadTime"
    ]

    def analyze(
        self,
        df_pref: pd.DataFrame,
        df_prep: pd.DataFrame,
        df_trans: pd.DataFrame,
        df_vendor_master: pd.DataFrame
    ) -> pd.DataFrame:

        df = df_pref.merge(df_prep, on=["ITEMNUM", "Warehouse", "VendorCode"], how="left").merge(
            df_trans, on=["VendorCode", "Warehouse", "TransportMode"], how="left"
        )

        vendor_static = df_vendor_master[["VendorCode", "TransportMode", "TransportLeadTimeDays"]].copy()
        df = df.merge(vendor_static, on=["VendorCode", "TransportMode"], how="left")
        df = df.rename(columns={"TransportLeadTimeDays": "StaticLeadTime"})

        # 校验字段完整性
        self._validate_input_columns(df)

        # Step 1: 标注交期来源
        def detect_source(row):
            if pd.notnull(row['Q60PrepDays']) and pd.notnull(row['Q60TransportLeadTime']):
                return 'SMART', 0
            elif pd.notnull(row['Q60PrepDays']) and pd.notnull(row['StaticLeadTime']):
                return 'SEMI_SMART_STATIC', 1
            elif pd.notnull(row['Q60PrepDays']):
                return 'MISSING_TRANSPORT', 2
            else:
                return 'FALLBACK_WLEAD', 3

        df[['Source', 'TransportFallbackLevel']] = df.apply(lambda r: pd.Series(detect_source(r)), axis=1)

        # Step 2: 补全缺失交期
        df['Q60TransportLeadTime'] = df['Q60TransportLeadTime'].fillna(df['StaticLeadTime'])
        df['MeanTransportLeadTime'] = df['MeanTransportLeadTime'].fillna(df['StaticLeadTime'])
        df['ModeTransportLeadTime'] = df['ModeTransportLeadTime'].fillna(df['StaticLeadTime'])
        df['Q90TransportLeadTime'] = df['Q90TransportLeadTime'].fillna(df['StaticLeadTime'])

        # Step 3: 合并准备期 + 运输期，形成估计交期
        df['MeanLeadTime'] = df['MeanPrepDays'].fillna(0) + df['MeanTransportLeadTime'].fillna(0)
        df['ModeLeadTime'] = df['ModePrepDays'].fillna(0) + df['ModeTransportLeadTime'].fillna(0)
        df['Q60LeadTime'] = df['Q60PrepDays'].fillna(0) + df['Q60TransportLeadTime'].fillna(0)
        df['Q90LeadTime'] = df['Q90PrepDays'].fillna(0) + df['Q90TransportLeadTime'].fillna(0)
        df['ExpSmoothedLeadTime'] = df['ExpSmoothPrepDays'].fillna(0) + df['SmoothedTransportLeadTime'].fillna(0)

        # Step 4: 不确定性 + 样本数量
        df['LeadTimeStd'] = np.sqrt(df['PrepStd'].fillna(0) * df['TransportLeadTimeStd'].fillna(0))
        df['SampleCount'] = df[['SampleCount_x', 'SampleCount_y']].min(axis=1)

        # Step 5: 标注是否为空运
        df['AirFlag'] = df['TransportMode'].apply(
            lambda x: 'Y' if str(x).upper() == TransportMode.AIR.value.upper() else None
        )

        # Step 6: 剔除仍缺失运输期的行
        df = df[df['Q60TransportLeadTime'].notnull()]

        # Step 7: 输出结构标准化
        df_final = df[[
            'ITEMNUM', 'Warehouse', 'VendorCode', 'TransportMode',
            'MeanLeadTime', 'ModeLeadTime', 'Q60LeadTime', 'Q90LeadTime',
            'ExpSmoothedLeadTime', 'LeadTimeStd',
            'Q60PrepDays', 'Q60TransportLeadTime',
            'SampleCount', 'Source', 'TransportFallbackLevel', 'AirFlag'
        ]].copy()

        df_final["LastUpdated"] = datetime.now()
        return df_final

