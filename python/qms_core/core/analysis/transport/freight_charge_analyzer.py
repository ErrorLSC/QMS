from qms_core.core.analysis.common.base_analyzer import BaseAnalyzer
from qms_core.core.common.params.enums import TransportMode
import pandas as pd
import numpy as np
from scipy.optimize import minimize
import numpy as np

def fit_constrained_positive_regression(x: np.ndarray, y: np.ndarray) -> dict | None:
    """
    拟合线性模型 Y = intercept + slope * x，要求 intercept ≥ 0 且 slope ≥ 0
    返回 BaseCharge（截距）和 CostPerKg（斜率）
    """
    def loss(beta):
        intercept, slope = beta
        y_pred = intercept + slope * x
        return np.sum((y - y_pred) ** 2)

    bounds = [(0, None), (0, None)]  # 拟合参数 >= 0
    initial = [1.0, 1.0]

    result = minimize(loss, x0=initial, bounds=bounds, method="L-BFGS-B")

    if not result.success:
        return None

    intercept, slope = result.x
    y_pred = intercept + slope * x
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

    return {
        "BaseCharge": intercept,
        "CostPerKg": slope,
        "RSquare": r2
    }

class FreightChargeAnalyzer(BaseAnalyzer):
    REQUIRED_COLUMNS = [
        "InvoiceDate", "POCurrency", "InvoiceTotal",
        "ItemTotal", "FreightCharge", "TransportMode",
        "SupplierGlobalCode", "Warehouse", "GrossWeight"
    ]

    # 运费占比阈值（按运输方式）
    MAX_FREIGHT_RATIO = {
        TransportMode.VESSEL.value: 0.2,
        # 可按需添加其他模式
    }

    def __init__(self):
        super().__init__()

    def _convert_currency_columns(self, df: pd.DataFrame, currency_df: pd.DataFrame, base_currency: str = "JPY") -> pd.DataFrame:
        df = df.copy()
        df["InvoiceMonth"] = pd.to_datetime(df["InvoiceDate"]).values.astype("datetime64[M]")

        fx = currency_df.copy()
        fx["ValidDate"] = pd.to_datetime(fx["ValidDate"])
        fx["InvoiceMonth"] = fx["ValidDate"].values.astype("datetime64[M]")
        fx = fx[(fx["ToCurrency"] == base_currency) & (fx["UsageType"] == "SPOT")]

        df = pd.merge(
            df,
            fx[["InvoiceMonth", "FromCurrency", "ExchangeRate", "ExchangeMethod"]],
            left_on=["InvoiceMonth", "POCurrency"],
            right_on=["InvoiceMonth", "FromCurrency"],
            how="left"
        ).drop(columns=["FromCurrency"])

        def convert(val, rate, method, pocur):
            if pd.isna(val):
                return np.nan
            if pocur == base_currency:
                return val
            if pd.isna(rate) or method not in ("M", "D"):
                return np.nan
            return val * rate if method == "M" else val / rate

        for col in ["InvoiceTotal", "ItemTotal", "FreightCharge"]:
            df[f"{col}_{base_currency}"] = df.apply(
                lambda row: convert(row[col], row["ExchangeRate"], row["ExchangeMethod"], row["POCurrency"]),
                axis=1
            )

        return df
    
    def _sanity_check(self, df: pd.DataFrame, base_currency: str = "JPY") -> pd.DataFrame:
        df = df.copy()
        ratio_col = f"FreightCharge_{base_currency}"
        base_col = f"ItemTotal_{base_currency}"

        df["FreightRatio"] = df[ratio_col] / df[base_col]

        def is_valid(row) -> bool:
            mode = row["TransportMode"]
            limit = self.MAX_FREIGHT_RATIO.get(mode)
            if limit is None:
                return True
            return row["FreightRatio"] <= limit

        df["FreightCheckPassed"] = df.apply(is_valid, axis=1)
        return df[df["FreightCheckPassed"]]

    def _estimate_freight_cost_by_path(self, df: pd.DataFrame, base_currency: str = "JPY") -> pd.DataFrame:
        """
        计算每条路径的 BaseCharge + CostPerKg 模型
        路径 = (SupplierGlobalCode, Warehouse, TransportMode)
        """
        results = []
        grouped = df.groupby(["SupplierGlobalCode", "Warehouse", "TransportMode"])

        for (vendor, wh, mode), subdf in grouped:
            x = subdf[["GrossWeight"]].values
            y = subdf[f"FreightCharge_{base_currency}"].values

            if len(x) < 3 or np.std(x) == 0:
                continue  # 样本不足或无变化

            fit = fit_constrained_positive_regression(x.flatten(), y)

            if fit:
                results.append({
                    "SupplierGlobalCode": vendor,
                    "Warehouse": wh,
                    "TransportMode": mode,
                    "BaseCharge": float(fit["BaseCharge"]),
                    "CostPerKg": float(fit["CostPerKg"]),
                    "SampleCount": len(subdf),
                    "RSquare": float(fit["RSquare"]),
                })

        return pd.DataFrame(results)
    
    def map_vendor_code(self, df_result: pd.DataFrame, vendor_master_df: pd.DataFrame) -> pd.DataFrame:
        """
        根据 GlobalCode → VendorCode 的映射，替换分析结果中的 SupplierGlobalCode
        """
        df_result = df_result.copy()
        df_vendor = vendor_master_df[["GlobalCode", "VendorCode"]].drop_duplicates()
        df_vendor = df_vendor.rename(columns={"GlobalCode": "SupplierGlobalCode"})

        df_result = pd.merge(
            df_result,
            df_vendor,
            on="SupplierGlobalCode",
            how="left"
        )

        # 可按需调整字段顺序
        columns = [
            "VendorCode", "SupplierGlobalCode", "Warehouse", "TransportMode",
            "BaseCharge", "CostPerKg", "SampleCount", "RSquare"
        ]
        return df_result[columns]

    def analyze(
        self,
        df: pd.DataFrame,
        vendor_master_df: pd.DataFrame,
        currency_df: pd.DataFrame,
        base_currency: str = "JPY"
    ) -> pd.DataFrame:
        self._validate_input_columns(df)

        df = self._convert_currency_columns(df, currency_df=currency_df, base_currency=base_currency)
        df = self._sanity_check(df, base_currency=base_currency)

        df_result = self._estimate_freight_cost_by_path(df, base_currency=base_currency)
        df_result = self.map_vendor_code(df_result, vendor_master_df)
        # 保留两位小数（只处理数值字段）
        df_result["BaseCharge"] = df_result["BaseCharge"].round(2)
        df_result["CostPerKg"] = df_result["CostPerKg"].round(2)
        df_result["RSquare"] = df_result["RSquare"].round(4)  # R² 可保留更多位，利于判断
        return df_result
