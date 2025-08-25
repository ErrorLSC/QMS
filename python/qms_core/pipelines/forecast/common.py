from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Union ,Type 
from qms_core.core.common.base_loader import BaseLoader
from qms_core.core.common.params.loader_params import LoaderParams
from qms_core.core.item.item import Item
from qms_core.core.forecast.common.mrp_datacontainer import MRPDataContainer

class BaseItemJob(ABC):
    """
    面向 item 分析流程的通用 Job 基类。
    结构遵循 ETL 三段：prepare_items (E), process_items (T), write_result (L)
    支持 dry_run、外部缓存注入、统一日志与核心字段导出。
    """

    def __init__(
        self,
        config,
        job_name: Optional[str] = None,
        data_container: Optional[MRPDataContainer] = None, 
        loader: Optional[BaseLoader] = None,
        load_params: Optional[LoaderParams] = None,
    ):
        self.config = config
        self.data_container = data_container
        self.job_name = job_name or self.__class__.__name__

        orm_cls = self.target_table()

        # ✅ 自动从 ORM 提取 loader 参数
        self.load_params: LoaderParams = (
            load_params
            or getattr(orm_cls, "__default_loader_params__", None)
            or LoaderParams()
        )

        # ✅ 自动构建 loader
        self.loader = loader or BaseLoader(
            config=self.config,
            orm_class=orm_cls,
            params=self.load_params
        )

    def run(
        self,
        items: Optional[list[Item]] = None,
        item_ids: Optional[list[tuple[str, str]]] = None,
        dry_run: bool = False,
        return_output:bool =False,
        session=None,
        **kwargs
    ) -> Union[pd.DataFrame, dict]:
        items = self.prepare_items(items=items, item_ids=item_ids, **kwargs)
        self.process_items(items, **kwargs)
        result = self._collect_result(items)

        if dry_run:
            self._dry_run_preview(result)

        if not dry_run:
            self.write_result(result, dry_run=False, session=session)

        if dry_run or return_output:
            return {
                "df": result,
                "core": self._export_core_fields(result) if hasattr(self, "_export_core_fields") else None,
                "items": items,
            }
        
    @abstractmethod
    def prepare_items(self, 
                      items: Optional[list] = None, 
                      item_ids: Optional[list[tuple[str, str]]] = None,
                        **kwargs
                    ) -> list:
        """
        若提供 items，则直接使用；否则根据 item_ids 创建 ItemManager 实例。
        返回一个 item 列表。
        """
        pass

    @abstractmethod
    def process_items(self, items: list, **kwargs):
        """
        主处理逻辑，如 classify/forecast/mrp 等。
        接收 prepare_items 提供的 item 列表。
        """
        pass

    @abstractmethod
    def _collect_result(self, items: list) -> Union[pd.DataFrame, dict]:
        """
        收集中间结果：可为 DataFrame 或字典。
        通常由 item 子模块 to_dict() 实现。
        """
        pass

    def write_result(self, result: Union[pd.DataFrame, dict], dry_run: bool = True, session=None):
        """
        写库逻辑（通常使用 SmartTableWriter 或 write_dataframe_to_table_by_orm）
        """
        if self.loader and isinstance(result, pd.DataFrame):
            self.loader.write(result,  dry_run, session)
        else:
            raise NotImplementedError("write_result() 未实现，或未提供 BaseLoader")

    def _export_core_fields(self, result: Union[pd.DataFrame, dict]) -> Union[pd.DataFrame, dict]:
        """
        dry_run 模式下可重载此方法导出最小必要字段（供 pipeline 缓存使用）
        默认原样返回。
        """
        return result
    
    def _dry_run_preview(self, result: Union[pd.DataFrame, dict]) -> Union[pd.DataFrame, dict]:
        print(f"🧪 Dry run 预览 {self.job_name}")

        if isinstance(result, pd.DataFrame):
            print(f"📊 预览前 5 行（共 {len(result)} 条）:")
            print(result.head(5))
        elif isinstance(result, dict):
            preview = dict(list(result.items())[:5])
            print(f"📊 预览前 5 条键值对（共 {len(result)} 项）:")
            print(preview)
        else:
            print("⚠️ 无法预览：未知类型结果")
    
    @abstractmethod
    def target_table(self) -> Type:
        """
        返回目标 ORM 类，例如 ItemForecastRecord。
        用于 loader 自动初始化。
        """
        pass




