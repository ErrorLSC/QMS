from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class FieldChangeMixin:
    FIELD_NAME = Column(String)
    OLD_VALUE = Column(String)
    NEW_VALUE = Column(String)
    CHANGE_DATE = Column(Date)
    CHANGE_REASON = Column(String)

class DemandChangeLog(Base,FieldChangeMixin):
    __tablename__ = "DEMANDCHANGELOG"
    __table_args__ = (
        PrimaryKeyConstraint('LORD', 'LLINE','Warehouse','FIELD_NAME','CHANGE_DATE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        only_update_delta= False,
    )
    LORD = Column(String)
    LLINE = Column(String)
    Warehouse = Column(String)

class IWI_ChangeLog(Base, FieldChangeMixin):
    __tablename__ = "CHANGELOG_IWI"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'FIELD_NAME', 'CHANGE_DATE'),
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)

class IIM_ChangeLog(Base, FieldChangeMixin):
    __tablename__ = "CHANGELOG_IIM"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'FIELD_NAME', 'CHANGE_DATE'),
    )

    ITEMNUM = Column(String)

class DemandType_ChangeLog(Base, FieldChangeMixin):
    __tablename__ = "CHANGELOG_ITEM_DEMANDTYPE"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'FIELD_NAME', 'CHANGE_DATE'),
    )

    ITEMNUM = Column(String)
    Warehouse = Column(String)