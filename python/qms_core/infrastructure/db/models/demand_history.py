from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class DemandHistory(Base):
    __tablename__ = "DEMANDHISTORY"
    __table_args__ = (
        PrimaryKeyConstraint('LORD', 'LLINE','Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        write_params={
            "delete_before_insert": False,
            "upsert": True,
            "hot_zone_delete": True,
            "hot_zone_days": 180,
            "hot_zone_column": "LODTE"
        }
    )

    # 主键
    LORD = Column(String)       # 订单号
    LLINE = Column(String)     # 订单行号
    Warehouse = Column(String)        # 仓库

    # 其他字段
    HDTYP = Column(String)     # 订单类型
    HCUST = Column(String)     # 客户编码
    LODTE = Column(Date)     # 订单日期
    ITEMNUM = Column(String)   # 物料编码
    LQORD = Column(Float)      # 订单数量
    LRDTE = Column(Date)     # 要求交货日
    LSDTE = Column(Date)     # 发货日
    SBSWKT = Column(String)    # JOB TYPE

    def __repr__(self):
        return f"<DemandHistory(LORD={self.LORD}, POLINE={self.LLINE}, Warehouse={self.Warehouse})>"

class DemandHistoryWeekly(Base):
    __tablename__ = "DEMANDHISTORY_WEEKLY"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'YearWeek','Warehouse'),
    )
    ITEMNUM = Column(String)
    Warehouse = Column(String)
    YearWeek = Column(String)  # 格式如 '2024-W12'
    TotalDemand = Column(Float)

class DemandHistoryRaw(Base):
    __tablename__ = "DEMANDHISTORY_RAW"
    __table_args__ = (
        PrimaryKeyConstraint('LORD', 'LLINE','Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    # 主键
    LORD = Column(String)   # 订单号
    LLINE = Column(String)  # 订单行号
    Warehouse = Column(String)   # 仓库

    # 其他字段
    HDTYP = Column(String)     # 订单类型
    HCUST = Column(String)     # 客户编码
    LODTE = Column(Date)       # 订单日期
    ITEMNUM = Column(String)   # 物料编码
    LQORD = Column(Float)      # 订单数量
    LRDTE = Column(Date)       # 要求交货日
    LSDTE = Column(Date)       # 发货日
    SBSWKT = Column(String)    # JOB TYPE

    # 新增字段：变化标记（0 = 取消，1 = 正常）
    IS_CHANGED = Column(Integer, default=0)    # 1 = changed, 0 = not changed

    # 新增字段：变化日期（如果取消，记录取消时间）
    CHANGE_DATE = Column(Date, nullable=True) 
    CHANGE_REASON = Column(String)

