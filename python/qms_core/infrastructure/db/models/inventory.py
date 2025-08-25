from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class STKOH(Base):
    __tablename__ = 'STKOH'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM','Warehouse','LOCATION','SERIAL'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        only_update_delta= False,
        write_params={
            "delete_before_insert": True,
            "upsert": False
        }
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)
    LOCATION = Column(String)
    QTYOH = Column(Float)
    ITEMDESC = Column(String)
    ISCST = Column(Float)
    AVAIL = Column(Float)
    LIALOC = Column(Float)
    IONOD = Column(Float)
    STOCKVAL = Column(Float)
    SERIAL = Column(String)

class STKOHAvail(Base):
    __tablename__ = 'STKOH_IWI_AVAIL'
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM','Warehouse'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        only_update_delta= False,
        write_params={
            "delete_before_insert": True,
            "upsert": False
        }
    )
    ITEMNUM = Column(String)
    Warehouse = Column(String)
    ITEMDESC = Column(String)
    AVAIL = Column(Float)
    IONOD = Column(Float)
    ISCST = Column(Float)
    QTYOH = Column(Float)
    IONOD = Column(Float)
    STOCKVAL = Column(Float)