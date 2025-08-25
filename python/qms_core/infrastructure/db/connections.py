from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def sqlite_engine(db_name: str | Path = "WHMaster.db", echo=False):
    db_path = Path(__file__).resolve().parents[2] / "data" / db_name
    return create_engine(f"sqlite:///{db_path}", echo=echo,
                         connect_args={"check_same_thread": False})

# Default session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False,
                            bind=sqlite_engine())