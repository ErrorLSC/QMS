from qms_core.adaptors.ETL.postprocess import run_postprocess_tasks
from qms_core.infrastructure.config import MRPConfig

config = MRPConfig()

# 👇 全部运行
run_postprocess_tasks(config,dry_run=False)

# 👇 Dry run 某个任务
# run_postprocess_tasks(config, task="safety_transfer", dry_run=True)