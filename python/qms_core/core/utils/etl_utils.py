from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import String, Integer, Float

def infer_dtype_from_orm(orm_class: DeclarativeMeta) -> dict[str, type]:
    """从 SQLAlchemy ORM 类提取字段 → pandas dtype 对应映射"""
    dtype_map = {}

    for col in orm_class.__table__.columns:
        colname = col.name
        coltype = col.type

        # SQLAlchemy → pandas 类型映射
        if isinstance(coltype, String):
            dtype_map[colname] = str
        elif isinstance(coltype, Integer):
            dtype_map[colname] = "Int64"  # 支持 pd.NA
        elif isinstance(coltype, Float):
            dtype_map[colname] = float
        else:
            # 默认不转
            continue

    return dtype_map
