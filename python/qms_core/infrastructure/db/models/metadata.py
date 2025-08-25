from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class ILM(Base):
    __tablename__ = 'ILM'
    __table_args__ = (
        PrimaryKeyConstraint('WLOC','Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        monitor_fields=["Warehouse","WLOC","WDESC","WLTYP","WZONE","VOLCAP","WEIGHTCAP"],
        only_update_delta= True,
        write_params={
            "upsert": True
        }
    )
    Warehouse = Column(String)
    WLOC = Column(String)
    WDESC = Column(String)
    X = Column(Float)
    Y = Column(Float)
    Z = Column(Float)
    WLTYP = Column(String)
    WZONE = Column(String)
    VOLCAP = Column(Float)
    WEIGHTCAP = Column(Float)

class IIM(Base):
    __tablename__ = 'IIM'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        monitor_fields=["IVEND"],
        only_update_delta= True,
        log_table_model="qms_core.infrastructure.db.models.IIM_ChangeLog",
        change_reason= "ERP Sync",
        write_params={
            "upsert": True
        }
    )

    ITEMNUM = Column(String)
    IITYP = Column(String)
    IDESC = Column(String)
    IDSCE = Column(String)
    IVEND = Column(String)
    VNDNAM = Column(String)
    ISCST = Column(Float)
    CXPPLC = Column(String)
    PGC = Column(String)
    GAC = Column(String)
    NETWEIGHT_KG = Column(Float)
    RPFLAG = Column(String)

class IWI(Base):
    __tablename__ = 'IWI'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM','Warehouse'),
    )

    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        monitor_fields=["WLEAD"],
        only_update_delta= True,
        log_table_model="qms_core.infrastructure.db.models.IWI_ChangeLog",
        change_reason= "ERP Sync",
        write_params={
            "upsert": True
        }
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)
    WLOTS = Column(Integer)
    WLEAD = Column(Integer)
    WSAFE = Column(Integer)
    WLOC = Column(String)
    MOQ = Column(Integer)

class IWM(Base):
    __tablename__ = 'IWM'
    __table_args__ = (
        PrimaryKeyConstraint('Warehouse', 'COUNTRYCODE', 'WMFAC'),
    )
    
    Warehouse = Column(String)
    LDESC = Column(String)
    ADDRESS = Column(String)
    COUNTRYCODE = Column(String)
    STATECODE = Column(String)
    POSTALCODE = Column(String)
    WMFAC = Column(String)
    STATUSCODE = Column(Integer)

class DPS(Base):
    __tablename__ = "DPS"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM_PARENT', 'ITEMNUM_CHILD','TYPE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta= True,
        write_params={
            "upsert": True
        }
    )
    ITEMNUM_PARENT = Column(String)
    ITEMNUM_CHILD = Column(String)
    TYPE = Column(String)
    PSCQTY = Column(Float)
    USING_EXISTING = Column(String) 

class MultiCurrency(Base):
    __tablename__ = "MULTICURRENCY"
    __table_args__ = (
        PrimaryKeyConstraint('ValidDate', 'FromCurrency','ToCurrency',"UsageType"),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta= True,
        write_params={
            "upsert": True
        }
    )

    ValidDate = Column(Date)
    ExchangeRate = Column(Float)
    FromCurrency = Column(String)
    ToCurrency = Column(String)
    UsageType = Column(String)
    ExchangeMethod = Column(String)

