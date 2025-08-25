import pandas as pd
from sqlalchemy.inspection import inspect
from typing import Optional, List, Type
from sqlalchemy import text

def fetch_orm_data(
    config,
    orm_class: Type,
    filters: Optional[List] = None,
    columns: Optional[List[str]] = None,
    session = None
) -> pd.DataFrame:
    """
    通用函数：通过 ORM 类获取数据并转为 DataFrame
    - config: MRPConfig 实例
    - orm_class: SQLAlchemy ORM 类
    - filters: 可选的 SQLAlchemy filter 表达式列表
    """
    external_session = session is not None
    session = session or config.get_session()

    try:
        if columns:
            orm_columns = [getattr(orm_class, c) for c in columns]
            query = session.query(*orm_columns)
        else:
            query = session.query(orm_class)

        if filters:
            query = query.filter(*filters)

        results = query.all()

        if not results:
            return pd.DataFrame(columns=columns if columns else None)

        if columns:
            # 每行是元组，需要转成 dict
            return pd.DataFrame([dict(zip(columns, row)) for row in results])
        else:
            all_columns = [c.key for c in inspect(orm_class).mapper.column_attrs]
            return pd.DataFrame([{col: getattr(r, col) for col in all_columns} for r in results])

    finally:
        if not external_session:
            session.close()
    
def run_sql(session, sql):
    return session.execute(text(sql))
