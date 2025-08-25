from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """通用 transform 抽象基类：输入 df → 输出 d
    可通过子类定义 COLUMN_TYPE_MAP，或注入 orm_class 自动推断类型。
    """

    COLUMN_TYPE_MAP: dict[str, type] = {}

    def __init__(self):
        pass  # 可扩展配置项，如 debug, verbose 等

    def enforce_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, dtype in self.COLUMN_TYPE_MAP.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    print(f"⚠️ 字段 {col} 类型转换失败: {e}")
        return df

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
class IdentityTransformer:
    def transform(self, df):
        return df
