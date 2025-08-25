from sqlalchemy import Column, String, Integer, Float, Date, Text, PrimaryKeyConstraint
from qms_core.infrastructure.db.models.base import Base
from qms_core.core.common.params import LoaderParams

class VirtualStockTransaction(Base):
    __tablename__ = "VIRTUAL_STOCK_TRANSACTION"
    __table_args__ = (
        PrimaryKeyConstraint('ITEMNUM', 'Warehouse','YearWeek','StockChangeType'),
    )
    __default_loader_params__ = LoaderParams(
        use_smart_writer=True,
        skip_if_unchanged=True,
        only_update_delta=False,
        write_params={"delete_before_insert": True}
    )

    ITEMNUM = Column(String, nullable=False)
    Warehouse = Column(String, nullable=False)
    YearWeek = Column(String)
    StockChangeType = Column(String)
    QtyChange = Column(Float)