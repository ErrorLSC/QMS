from qms_core.infrastructure.db.models import Base
from qms_core.infrastructure.config import MRPConfig

if __name__ == "__main__":
    engine = MRPConfig().get_engine()
    Base.metadata.create_all(engine)
    print("✅ 所有表结构已初始化（如不存在则已创建）")