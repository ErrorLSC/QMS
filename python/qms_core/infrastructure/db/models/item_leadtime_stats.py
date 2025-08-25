from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class ItemPrepareLTStats(Base):
    __tablename__ = 'ITEM_PREPARE_LT_STATS'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'VendorCode'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["LastUpdated"],
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    ITEMNUM = Column(Text)
    VendorCode = Column(Text)
    Warehouse = Column(Text)
    MeanPrepDays = Column(Float)
    PrepStd = Column(Float)
    ModePrepDays = Column(Float)
    Q60PrepDays = Column(Float)
    Q90PrepDays = Column(Float)
    ExpSmoothPrepDays = Column(Float)
    SampleCount = Column(Integer)
    LastUpdated = Column(Date)

class ItemTransportPreference(Base):
    __tablename__ = 'ITEM_TRANSPORT_PREFERENCE'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'VendorCode', 'TransportMode'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["LastUpdated"],
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    ITEMNUM = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    VendorCode = Column(String, nullable=False)
    TransportMode = Column(String, nullable=False)  # Enum.name
    Rank = Column(Integer, nullable=False)
    Count = Column(Integer, nullable=False)
    Confidence = Column(Float, nullable=False)
    LastUsedDate = Column(Date, nullable=True)
    LastUpdated = Column(Date, nullable=False)

class ItemSmartLeadtime(Base):
    __tablename__ = 'ITEM_SMART_LEADTIME'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'VendorCode', 'TransportMode'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["LastUpdated"],
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    ITEMNUM = Column(Text)
    Warehouse = Column(Text)
    VendorCode = Column(Text)
    TransportMode = Column(String)

    MeanLeadTime = Column(Float)
    ModeLeadTime = Column(Float)
    Q60LeadTime = Column(Float)
    Q90LeadTime = Column(Float)
    ExpSmoothedLeadTime = Column(Float)
    LeadTimeStd = Column(Float)

    Q60PrepDays = Column(Float)
    Q60TransportLeadTime = Column(Float)

    SampleCount = Column(Integer)
    Source = Column(String)  # SMART / SEMI_SMART / FALLBACK_WLEAD
    TransportFallbackLevel = Column(Integer)  # 0 / 1 / 2
    AirFlag = Column(String)  # 'Y' or null

    LastUpdated = Column(Date)

class ItemDeliveryBehaviorStats(Base):
    __tablename__ = 'ITEM_DELIVERY_BEHAVIOR_STATS'
    __table_args__ = (
        PrimaryKeyConstraint('VendorCode', 'TransportMode', 'Warehouse', 'ITEMNUM'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["LastUpdated"],
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    VendorCode = Column(String)
    TransportMode = Column(String)
    Warehouse = Column(String)
    ITEMNUM = Column(String)

    TotalPOs = Column(Integer)  # 历史总 PO 数
    SplitPO_Rate = Column(Float)  # 出现分批的 PO 占比
    AvgBatchCount_SplitPOs = Column(Float)  # 仅在分批 PO 中的平均批次数

    TypicalSingleBatchQty = Column(Float)  # 常规单批发货量（中位或均值）
    MaxSingleBatchQty = Column(Float)  # 单批最大承载量（如 Q90）

    AvgSpreadDays_SplitPOs = Column(Float)  # 分批订单的首尾跨度平均值
    AvgIntervalBetweenBatches = Column(Float)  # 分批间的平均间隔
    AvgTailQtyRate = Column(Float)  # 尾批发货量占比（均值）

    LastUpdated = Column(Date)

class ItemBatchProfile(Base):
    __tablename__ = 'ITEM_BATCH_PROFILE'
    __table_args__ = (
        PrimaryKeyConstraint('VendorCode', 'TransportMode', 'Warehouse', 'ITEMNUM'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["LastUpdated"],
        write_params={
            "delete_before_insert": False,
            "upsert": True,
        }
    )
    VendorCode = Column(String)
    TransportMode = Column(String)
    Warehouse = Column(String)
    ITEMNUM = Column(String)

    IsBatchProne = Column(String)  # 'Y' or 'N' 可选，也可以用 Boolean 映射为 True/False

    BatchTriggerQty = Column(Float)  # 超过此数可考虑分批

    PredictedBatchCount = Column(Integer)  # 模拟批次数
    PredictedBatchQty = Column(Float)      # 每批数量
    PredictedBatchIntervalDays = Column(Float)  # 批次间隔
    PredictedTailQtyRate = Column(Float)   # 尾批占比

    LastUpdated = Column(Date)  # 日期戳，格式 yyyy-mm-dd（建议保留）

