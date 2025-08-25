class PostprocessManager:
    """
    后处理任务注册器与调度器，支持 dry_run 和单任务执行。
    """

    def __init__(self, config):
        self.config = config
        self.registry = {}

    def register(self, name: str, func):
        """
        注册后处理函数
        :param name: 唯一任务名
        :param func: 函数签名必须包含 (config, dry_run=False)
        """
        self.registry[name] = func

    def run(self, name: str, dry_run=False):
        if name not in self.registry:
            raise ValueError(f"❌ 未注册的后处理任务: {name}")
        print(f"\n🔧 执行后处理任务：{name} (dry_run={dry_run})")
        self.registry[name](self.config, dry_run=dry_run)

    def run_all(self, dry_run=False):
        print(f"\n🚀 执行全部后处理任务 (dry_run={dry_run}) ...")
        for name, func in self.registry.items():
            self.run(name, dry_run=dry_run)
        print("\n✅ 所有后处理任务执行完毕。")