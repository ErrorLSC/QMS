from .registry import PostprocessManager
from .update_rpflag import update_rpflag
from .safety_stock_transfer import transfer_safety_stock_with_log
from .order_change import detect_and_mark_order_changes

def run_postprocess_tasks(config, task: str = None, dry_run: bool = False):
    """
    调度后处理任务
    :param task: 指定任务名（如 'rpflag'），若为 None 则运行所有任务
    """
    manager = PostprocessManager(config)
    manager.register("rpflag", update_rpflag)
    manager.register("safety_transfer", transfer_safety_stock_with_log)
    manager.register("order_change", detect_and_mark_order_changes)

    if task:
        manager.run(task, dry_run=dry_run)
    else:
        manager.run_all(dry_run=dry_run)
