from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class MRPOrder(Base):
    __tablename__ = "MRP_ORDERS"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse','Algorithm'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        only_update_delta= False,
        skip_if_unchanged=True,
        exclude_fields=["CalcDate"],
        write_params={
            "delete_before_insert": True,
            "upsert": False
        }
    )
    ITEMNUM = Column(String)
    Warehouse = Column(String)

    MOQ = Column(Integer)
    TransportMode = Column(String)
    WLEAD = Column(Integer)
    ManualSafetyStock = Column(Float)
    CXPPLC = Column(String)
    ITEMDESC = Column(String)
    IVEND = Column(String)
    VNDNAM = Column(String)

    AvailableStock = Column(Float)
    IntransitStock = Column(Float)

    DemandType = Column(String)
    ActivityLevel = Column(String)
    RecommendedServiceLevel = Column(Float)

    Forecast_within_LT = Column(Float)
    DynamicSafetyStock = Column(Float)
    FinalSafetyStock = Column(Float)

    NetRequirement = Column(Float)
    RecommendedQty = Column(Float)

    OrderReason = Column(String)
    Algorithm = Column(String)
    CalcDate = Column(Date)