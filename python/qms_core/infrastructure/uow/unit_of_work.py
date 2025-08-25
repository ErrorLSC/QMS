from sqlalchemy.orm import Session
from qms_core.infrastructure.config import MRPConfig

class UnitOfWork:
    def __init__(self, config: MRPConfig, session: Session = None):
        self.config = config
        self._external_session = session is not None
        self.session = session or config.get_session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            if not self._external_session:
                self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        if not self._external_session:
            self.session.close()