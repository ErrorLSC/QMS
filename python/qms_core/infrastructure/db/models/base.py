"""
定义全系统 ORM 使用的 declarative base。
所有模型类都应继承自 Base。
"""

from sqlalchemy.orm import declarative_base

Base = declarative_base()