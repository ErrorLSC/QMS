from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class ItemSafetyRecord(Base):
    __tablename__ = "ITEM_SAFETY"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        key_fields=["ITEMNUM", "Warehouse"],
        monitor_fields=["RecommendedServiceLevel", "DynamicSafetyStock", "FinalSafetyStock"],
        exclude_fields=["SafetyDate"],
        write_params={"upsert": True},
        enable_logging=False
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)
    RecommendedServiceLevel = Column(Float)
    DynamicSafetyStock = Column(Float)
    FinalSafetyStock = Column(Float)
    SafetyCalcDate = Column(Date)  # 用字符串存日期 "YYYY-MM-DD"

class SAFETY_TRANSFER_LOG(Base):
    __tablename__ = 'SAFETY_TRANSFER_LOG'
    __table_args__ = (
        PrimaryKeyConstraint('TRANSFER_DATE', 'Warehouse','ITEMNUM_PARENT', 'ITEMNUM_CHILD'),
    )

    TRANSFER_DATE = Column(Date)
    Warehouse = Column(String)
    ITEMNUM_PARENT = Column(String)
    ITEMNUM_CHILD = Column(String)
    PARENT_WSAFE = Column(Float)
    CHILD_WSAFE_BEFORE = Column(Float)
    CHILD_WSAFE_AFTER = Column(Float)
    PSCQTY = Column(Float)
    OPERATOR = Column(String)