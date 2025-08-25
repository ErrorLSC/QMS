from typing import Type
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from qms_core.core.common.base_loader import BaseLoader
from qms_core.core.common.params.loader_params import LoaderParams

class BaseJobCore(ABC):
    def __init__(
        self,
        config,
        job_name: Optional[str] = None,
        load_params: Optional[LoaderParams] = None
    ):
        self.config = config
        self.job_name = job_name or self.__class__.__name__

        # ✅ 如果未显式传入 load_params，则使用 ORM 表上的默认写入策略（如有）
        table_cls = self.target_table()
        self.load_params: LoaderParams = (
            load_params
            or getattr(table_cls, "__default_loader_params__", None)
            or LoaderParams()
        )

        # ✅ 初始化 loader
        self.loader = BaseLoader(
            config=self.config,
            orm_class=table_cls,
            params=self.load_params
        )
        
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def target_table(self) -> Type:
        pass

    def load(self, df: pd.DataFrame, dry_run: bool = True, session=None) -> pd.DataFrame:
        """
        默认加载方法，使用 BaseLoader + SmartWriter。
        子类可 override 以自定义写库方式。
        """
        return self.loader.write(df, dry_run=dry_run, session=session)

    def run(self, dry_run: bool = True, session=None) -> pd.DataFrame:
        print(f"🚀 运行任务：{self.job_name}")
        data = self.extract()
        df = self.transform(data)
        return self.load(df, dry_run=dry_run, session=session)
