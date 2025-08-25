# qms/infrastructure/config.py
from __future__ import annotations
from pathlib import Path
from typing import Optional

import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

DEFAULT_DB_REL = Path("data") / "WHMaster.db"  

class MRPConfig:
    """
    统一管理 SQLite / SQLAlchemy 入口。
    未来若迁 PostgreSQL，可在此切换连接串，业务代码零改动。
    """

    def __init__(self, db_path: Optional[str | Path] = None, echo: bool = False):
        project_root = Path(__file__).resolve().parents[3]   # qms/ ← infra/ ← THIS
        default_path = project_root / DEFAULT_DB_REL
        self.db_path: Path = Path(db_path).expanduser() if db_path else default_path

        # SqlAlchemy objects
        uri = f"sqlite:///{self.db_path.as_posix()}"
        self.engine = create_engine(uri, echo=echo, future=True,
                                    connect_args={"check_same_thread": False})
        self._SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    # --- public API -------------------------------------------------

    def get_sqlite_conn(self) -> sqlite3.Connection:
        """低层次 sqlite3 连接（仅少数场景需要）"""
        self._ensure_file()
        return sqlite3.connect(self.db_path)

    def get_session(self) -> Session:
        """SQLAlchemy ORM Session（推荐业务代码用）"""
        self._ensure_file()
        return self._SessionLocal()

    def get_engine(self):
        """直接给 Pandas read_sql / to_sql 使用"""
        self._ensure_file()
        return self.engine

    # --- helpers ----------------------------------------------------

    def _ensure_file(self):
        if not self.db_path.exists():
            raise FileNotFoundError(f"❌ 数据库文件不存在: {self.db_path}")

    # --- dunder -----------------------------------------------------

    def __repr__(self) -> str:
        return f"<MRPConfig db_path='{self.db_path}'>"
