from abc import ABC, abstractmethod
from qms_core.infrastructure.db.bulk_writer import SmartTableWriter
from qms_core.infrastructure.config import MRPConfig
import pandas as pd

class ItemComponentBase(ABC):
    """所有 Item-级子模块的共同基类"""

    def __init__(self, itemnum: str, warehouse: str):
        self.itemnum = itemnum
        self.warehouse = warehouse
        self._loaded = False       # 懒加载标记

    # ---------- 数据加载 ----------
    @abstractmethod
    def load(self, session, *args, **kwargs):
        """必须实现：从数据库加载自身所需字段"""
        ...

    def load_from_dict(self, record: dict):
        """可选：从预加载字典注入字段（批量模式友好）"""
        # 默认什么都不做；子类按需覆盖
        pass

    # ---------- 序列化 ----------
    def to_dict(self) -> dict:
        """统一提供序列化（子类扩展 _extra_fields() 决定输出）"""
        base = {"ITEMNUM": self.itemnum, "Warehouse": self.warehouse}
        base.update(self._extra_fields())
        for k, v in base.items():
            if hasattr(v, "value"):  # 是 Enum
                base[k] = v.value

        return base

    def _extra_fields(self) -> dict:
        """子类返回自身要额外序列化的字段"""
        return {}

    # ---------- 写库 ----------
    def write_to_db(self, config: MRPConfig = None, dry_run=False, existing_df: pd.DataFrame = None):
        orm_obj = self._to_orm()
        if orm_obj is None:
            print(f"ℹ️ {self.__class__.__name__} 无需写入（无结果）")
            return pd.DataFrame(), pd.DataFrame()

        config = config or MRPConfig()
        writer = SmartTableWriter(
            config=config,
            orm_class=self._to_orm_class(),
            key_fields=["ITEMNUM", "Warehouse"],
            **self.get_writer_config()
        )
        df = pd.DataFrame([self.to_dict()])
        return writer.write(df=df, dry_run=dry_run, existing_df=existing_df)

    def _to_orm(self):
        """子类返回 ORM 实例，用于 write_to_db()"""
        return None
    
    def _to_orm_class(self):
        """返回 ORM 类（用于 SmartTableWriter）"""
        return None
        

    def get_writer_config(self) -> dict:
        """可选：返回 SmartTableWriter 需要的字段，如 key_fields、monitor_fields、log_table_model 等"""
        return {}


    # ---------- 调试输出 ----------
    def show_summary(self):
        pretty = ", ".join(f"{k}: {v}" for k, v in self.to_dict().items())
        print(f"[{self.__class__.__name__}] {pretty}")
