from sqlalchemy import Column, String, Integer, Float, Date, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class VendorMaster(Base):
    __tablename__ = 'VENDOR_MASTER'
    __table_args__ = (
        PrimaryKeyConstraint('VendorCode', 'TransportMode'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta= True,
        write_params={
            "upsert": True
        }
    )
    
    VendorCode = Column(String)
    VendorName = Column(String, nullable=False)
    TransportMode = Column(String)  
    TransportLeadTimeDays = Column(Integer, nullable=False)  
    VendorType = Column(String, nullable=False)
    GlobalCode = Column(String) 
    IS_ACTIVE = Column(String(1), default='Y')   

class VendorTransportStats(Base):
    __tablename__ = 'VENDOR_TRANSPORT_STATS'
    __table_args__ = (
        PrimaryKeyConstraint('VendorCode', 'Warehouse', 'TransportMode'),
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

    MeanTransportLeadTime = Column(Float)
    TransportLeadTimeStd = Column(Float)   # 标准差
    ModeTransportLeadTime = Column(Float)
    Q60TransportLeadTime = Column(Float)
    Q90TransportLeadTime = Column(Float)
    SampleCount = Column(Integer) # 样本数
    SmoothedTransportLeadTime = Column(Float)   

    CostPerKg = Column(Float, default=0.0)    # 单价（元/公斤）
    BaseCharge = Column(Float, default=0.0)   # 起价
    LastUpdated = Column(Date)