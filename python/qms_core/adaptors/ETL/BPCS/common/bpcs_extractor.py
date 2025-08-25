from typing import List, Optional,Type, Union
from types import ModuleType
from qms_core.core.common.base_extractor import BaseExtractor
import pandas as pd
from qms_core.infrastructure.extractors.bpcs import BPCSConnector
from pydantic import BaseModel,Field
from importlib import resources
from qms_core.adaptors.ETL.sql_template_engine import render_sql_template
import importlib

class BPCSExtractorMeta(BaseModel):
    dsn: str = Field(default="JPNPRDF", description="Data Source Name")
    system: str = Field(default="EPISBE20", description="BPCS Server Name")
    date_columns: List[str] = Field(default_factory=list, description="Field Name to covert to datetime format")

class BPCSExtractor(BaseExtractor):
    """
    所有 BPCS 数据提取类的基类（增强版）。
    子类仅需声明 TEMPLATE_FILE、PARAMS_CLASS、DEFAULT_DATE_COLUMNS 即可。
    """
    TEMPLATE_FILE: str = ""                         # ⛔ 子类必须指定
    TEMPLATE_NAMESPACE: Union[str, ModuleType] = None  # ⛔ 子类必须指定
    PARAMS_CLASS: Optional[Type[BaseModel]] = None
    DEFAULT_DATE_COLUMNS: list[str] = []

    def __init__(self, meta: BPCSExtractorMeta, params: BaseModel):
        # 🛡️ 防御性拷贝
        meta = meta.model_copy(deep=True)

        if not meta.date_columns and self.DEFAULT_DATE_COLUMNS:
            meta.date_columns = self.DEFAULT_DATE_COLUMNS
        
        self.meta = meta
        self.dsn = meta.dsn
        self.system = meta.system
        self.date_columns = meta.date_columns

        # ✅ 自动类型校验（如果未声明 PARAMS_CLASS，允许直接使用 BaseModel）
        self.params = self._validate_params(params)

    def _validate_params(self, params: BaseModel) -> BaseModel:
        if self.PARAMS_CLASS is None:
            raise ValueError(f"❌ {self.__class__.__name__} 未指定 PARAMS_CLASS")

        if not issubclass(self.PARAMS_CLASS, BaseModel):
            raise TypeError(f"❌ PARAMS_CLASS 必须是 BaseModel 子类，而不是 {self.PARAMS_CLASS}")
        
        if self.PARAMS_CLASS and not isinstance(params, self.PARAMS_CLASS):
            try:
                return self.PARAMS_CLASS.model_validate(params)
            except Exception as e:
                raise ValueError(f"❌ 参数类型不符：期望 {self.PARAMS_CLASS}, 但收到 {type(params)} → {e}")
        return params

    def render_sql(self) -> str:
        if not self.TEMPLATE_FILE:
            raise ValueError(f"❌ {self.__class__.__name__} 未指定 TEMPLATE_FILE")

        if not self.TEMPLATE_NAMESPACE:
            raise ValueError(f"❌ {self.__class__.__name__} 未指定 TEMPLATE_NAMESPACE")

        if isinstance(self.TEMPLATE_NAMESPACE, str):
            try:
                namespace = importlib.import_module(self.TEMPLATE_NAMESPACE)
            except Exception as e:
                raise ImportError(f"❌ 无法导入模板模块 '{self.TEMPLATE_NAMESPACE}' → {e}")
        else:
            namespace = self.TEMPLATE_NAMESPACE

        sql_path = resources.files(namespace).joinpath(self.TEMPLATE_FILE)
        return render_sql_template(sql_path, self.params.model_dump())

    def _query_bpcs(self, sql: str) -> pd.DataFrame:
        connector = BPCSConnector(self.dsn,self.system)
        return connector.query(sql, date_columns=self.date_columns)

    def fetch(self) -> pd.DataFrame:
        sql = self.render_sql()
        return self._query_bpcs(sql)
    
    def __repr__(self):
        return f"<{self.__class__.__name__}(dsn={self.dsn}, system={self.system})>"
