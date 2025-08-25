from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class PO_IntransitRaw(Base):
    __tablename__ = "PO_INTRANSIT_RAW"
    __table_args__ = (
        PrimaryKeyConstraint('PONUM', 'POLINE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=False,
        exclude_fields=["POEntryDate", "InvoiceDate","PQREM_Corrected"],
        write_params={"delete_before_insert": True}
    )
    PONUM = Column(String)
    POLINE = Column(String)  
    ITEMNUM = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    VendorCode = Column(String, nullable=False)

    OrderedQty = Column(Float, nullable=False)
    RemainingQty = Column(Float, nullable=False)
    InTransitQty = Column(Float, nullable=False)

    POEntryDate = Column(Date)
    InvoiceDate = Column(Date,nullable=True)

    TransportMode = Column(String)  # Enum 值，如 "AIR", "VESSEL"

    OrderType = Column(String)
    Comment = Column(String)

    PQREM_Corrected = Column(Float, default=0)

class PO_DeliveryHistoryRaw(Base):
    __tablename__ = 'PO_DELIVERY_HISTORY_RAW'
    __table_args__ = (
        PrimaryKeyConstraint('PONUM', 'POLINE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        exclude_fields=["POEntryDate", "InvoiceDate","ActualDeliveryDate"],
        write_params={"upsert":True}
    )
    PONUM = Column(String)
    POLINE = Column(String)
    VendorCode = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    ITEMNUM = Column(String, nullable=False)
    OrderedQty = Column(Float)
    ReceivedQty = Column(Float)
    POEntryDate = Column(Date)
    InvoiceDate = Column(Date)
    ActualDeliveryDate = Column(Date)
    PrepareTime = Column(Float)
    TransportTime = Column(Float)
    TotalLeadTime = Column(Float)
    TransportMode = Column(String)
    IsClosed = Column(String)

class PO_LAST_SNAPSHOT(Base):
    __tablename__ = 'PO_LAST_SNAPSHOT'
    __table_args__ = (
        PrimaryKeyConstraint('PONUM', 'POLINE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=False,
        exclude_fields=["POEntryDate", "DueDate", "AcknowledgedDeliveryDate"],
        write_params={"delete_before_insert": True}
    )
    PONUM = Column(String, primary_key=True)
    POLINE = Column(String, primary_key=True)
    VendorCode = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    ITEMNUM = Column(String, nullable=False)
    POEntryDate = Column(Date)
    TransportMode = Column(String)
    PQORD = Column(Float)
    PQREC = Column(Float)
    PQREM = Column(Float)
    PCQTY = Column(Float)
    PQTRANSIT = Column(Float)
    LotNumber = Column(String)
    DueDate = Column(Date)
    AcknowledgedDeliveryDate = Column(Date)
    Comment = Column(String)

class PO_SnapshotLog(Base):
    __tablename__ = 'PO_SNAPSHOT_LOG'
    __table_args__ = (
        PrimaryKeyConstraint('SNAPSHOT_DATE', 'PONUM', 'POLINE','FIELD_NAME'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        only_update_delta= False,
    )
    SNAPSHOT_DATE = Column(Date)
    PONUM = Column(String)
    POLINE = Column(String)
    ITEMNUM = Column(String)
    WAREHOUSE = Column(String)
    EVENT_TYPE = Column(String)       # INVOICE / RECEIPT / CANCEL
    QTY_DELTA = Column(Float)
    QTY_TOTAL = Column(Float)
    FIELD_NAME = Column(String)

class PO_ETA_Recommendation(Base):
    __tablename__ = "PO_ETA_RECOMMENDATION"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse', 'PONUM', 'POLINE'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=False,
        write_params={"delete_before_insert": True}
    )

    ITEMNUM = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    PONUM = Column(String, nullable=False)
    POLINE = Column(String, nullable=False)

    VendorCode = Column(String)
    TransportMode = Column(String)
    InTransitQty = Column(Float)
    ETA_Date = Column(Date)
    ETA_Week = Column(String)
    ETA_Flag = Column(String)
    
    Fallback_TransportUsed = Column(String)
    Fallback_TotalLeadUsed = Column(String)
    Comment = Column(String)

    BatchIndex = Column(Integer)
    IsFinalBatch = Column(String)  # "Y"/"N"
    ETA_Overdue = Column(String)   # "Y"/"N"

class PO_Freight_Charge(Base):
    __tablename__ = 'PO_FREIGHT_CHARGE'
    __table_args__ = (
        PrimaryKeyConstraint('SupplierGlobalCode', 'ShipmentNum','InvoiceDate'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=True,
        write_params={"upsert": True}
    )
    InvoiceDate = Column(Date)
    TransportMode = Column(String)
    ShipmentNum = Column(String)
    SupplierGlobalCode = Column(String)
    InvoiceTotal = Column(Float)
    POCurrency = Column(String)
    ItemTotal = Column(Float)
    Warehouse = Column(String)
    GrossWeight = Column(Float)
    FreightCharge = Column(Float)
