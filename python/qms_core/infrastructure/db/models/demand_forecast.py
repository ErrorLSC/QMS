from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class DemandType(Base):
    __tablename__ = "ITEM_DEMAND_TYPE"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        key_fields=["ITEMNUM", "Warehouse"],
        monitor_fields=["DemandType", "ActivityLevel"],
        log_table_model="qms_core.infrastructure.db.models.DemandType_ChangeLog",
        change_reason="Demand classification updated",
        exclude_fields=["ClassifyDate"],
        write_params={"upsert": True}
    )
    ITEMNUM = Column(String)
    Warehouse = Column(String)
    DemandType = Column(String)
    ActivityLevel = Column(String)
    WeeksWithDemand = Column(Integer)
    ZeroRatio = Column(Float)
    CV = Column(Float)
    TrendSlope = Column(Float)
    SeasonalStrength = Column(Float)
    ClassifyDate = Column(Date)

class ItemForecastRecord(Base):
    __tablename__ = "ITEM_FORECAST"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        key_fields=["ITEMNUM", "Warehouse"],
        monitor_fields=["ForecastModel", "ForecastSeriesJSON"],
        exclude_fields=["ForecastDate"],
        enable_logging=False,
        write_params={"upsert": True}
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)
    ForecastSeriesJSON = Column(Text)
    Forecast_monthly = Column(Float)
    ForecastDate = Column(Date)  # 格式: 'YYYY-MM-DD'
    ForecastModel = Column(String) 