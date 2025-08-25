from typing import Optional, Type
from qms_core.core.common.base_transformer import IdentityTransformer
import inspect
from qms_core.core.common.base_job import BaseJobCore
from qms_core.core.common.params.loader_params import LoaderParams
import pandas as pd

class GenericExtractorJob(BaseJobCore):
    EXTRACTOR_CLASS = None           # ⛔ 子类必须指定
    TARGET_TABLE = None              # ⛔ 子类必须指定
    TRANSFORMER_CLASS = IdentityTransformer  # 可选指定
    DEFAULT_EXTRACT_PARAMS = {}      # ✅ 可由子类覆盖

    def __init__(
        self,
        config,
        *,
        extractor_meta: dict| None=None,
        extract_params: dict| None = None,
        load_params: Optional[LoaderParams] = None,
    ):
        super().__init__(config=config, load_params=load_params)
        
        extractor_meta = extractor_meta or {}
        extract_params = {**self.DEFAULT_EXTRACT_PARAMS, **(extract_params or {})}

        extractor_init = inspect.signature(self.EXTRACTOR_CLASS.__init__)
        kwargs = {}

        if "meta" in extractor_init.parameters:
            meta_schema = extractor_init.parameters["meta"].annotation
            if hasattr(meta_schema, "model_validate"):
                kwargs["meta"] = meta_schema.model_validate(extractor_meta)
            else:
                kwargs["meta"] = extractor_meta

        if "params" in extractor_init.parameters:
            params_schema = extractor_init.parameters["params"].annotation
            if hasattr(params_schema, "model_validate"):
                kwargs["params"] = params_schema.model_validate(extract_params)
            else:
                kwargs["params"] = extract_params

        self.extractor = self.EXTRACTOR_CLASS(**kwargs)

        # ✅ 初始化 transformer（如果构造函数接受 orm_class，则注入）
        transformer_init = inspect.signature(self.TRANSFORMER_CLASS.__init__)
        if "orm_class" in transformer_init.parameters:
            self.transformer = self.TRANSFORMER_CLASS(orm_class=self.TARGET_TABLE)
        else:
            self.transformer = self.TRANSFORMER_CLASS()

    def extract(self) -> pd.DataFrame:
        return self.extractor.fetch()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.transformer.transform(df)

    def target_table(self) -> Type:
        return self.TARGET_TABLE