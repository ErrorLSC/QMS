from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base

class ForecastEvaluation(Base):
    __tablename__ = "FORECAST_EVALUATION"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse'),
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)

    EvalStart = Column(Date)
    EvalEnd = Column(Date)
    BacktestWindow = Column(Integer)

    PredictedDemand = Column(Float)
    ActualDemand = Column(Float)
    DynamicSafetyStock = Column(Float)

    APE = Column(Float)
    MoM_Growth = Column(Float)
    YoY_Growth = Column(Float)
    ForecastScore = Column(Float)

    Covered = Column(String)  # "Y" / "N"
    CoverageGap = Column(Float)

    DemandType = Column(String)
    ActivityLevel = Column(String) 
    LastUpdated = Column(Date)