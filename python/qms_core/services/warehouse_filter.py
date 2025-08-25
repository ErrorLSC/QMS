"""
从 IWM 仓库主数据表中提取所有启用状态的仓库代码（Warehouse 字段）。
该模块用于生成注入 SQL 模板的仓库白名单。
"""

from typing import Optional
from qms_core.infrastructure.db.models.metadata import IWM
from qms_core.infrastructure.config import MRPConfig


def get_active_warehouses(
    config: MRPConfig,
    country: Optional[str] = None,
    WMFAC: Optional[str] = None
) -> list[str]:
    """
    获取启用状态（STATUSCODE=1）的有效仓库列表。

    参数:
        config (MRPConfig): 数据库配置对象
        country (Optional[str]): 可选国家筛选（如 'JP'）
        WMFAC (Optional[str]): 可选工厂代码（如 'JPE'）

    返回:
        List[str]: 仓库编码列表
    """
    with config.get_session() as session:
        query = session.query(IWM.Warehouse).filter(IWM.STATUSCODE == 1)

        if country:
            query = query.filter(IWM.COUNTRYCODE == country)

        if WMFAC:
            query = query.filter(IWM.WMFAC == WMFAC)

        rows = query.distinct().all()
        return [row.Warehouse for row in rows]
