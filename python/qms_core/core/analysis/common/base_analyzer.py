from abc import ABC, abstractmethod
import pandas as pd


class BaseAnalyzer(ABC):
    # 子类声明所需字段（列名）
    REQUIRED_COLUMNS: list[str] = []

    def _validate_input_columns(self, df: pd.DataFrame):
        missing = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        empty = [col for col in self.REQUIRED_COLUMNS if col in df.columns and df[col].dropna().empty]

        if missing:
            raise ValueError(f"❌ 缺少必需字段: {missing}")
        if empty:
            raise ValueError(f"⚠️ 字段存在但值全为空: {empty}")

    @abstractmethod
    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        """执行数据分析逻辑，返回结果"""
        pass