from typing import Type
import inspect
from qms_core.infrastructure.uow.unit_of_work import UnitOfWork

class BasePipeline:
    JOB_CLASSES: dict[str, Type] = {}
    JOB_INIT_ARGS: dict[str, list[str]] = {}
    DEFAULT_RUN_ORDER: list[str] = []

    def __init__(self, config, dsn=None, job_configs: dict[str, dict] = None, only_configured: bool = False):
        self.config = config
        self.dsn = dsn
        self.job_configs = job_configs or {}
        self.only_configured = only_configured
        self.jobs = self._init_jobs()
        print(f"📋 初始化 Job 数量: {len(self.jobs)} → {list(self.jobs.keys())}")

    def _get_job_init_kwargs(self, job_name: str) -> dict:
        job_conf = self.job_configs.get(job_name, {})
        param_names = job_conf.get("init_args", self.JOB_INIT_ARGS.get(job_name, ["config"]))
        kwargs = {}

        for param in param_names:
            if param == "config":
                kwargs["config"] = self.config
            elif param == "dsn":
                kwargs["dsn"] = self.dsn
            else:
                kwargs[param] = job_conf.get(param)
        for k, v in job_conf.items():
            if k not in kwargs and k != "init_args":
                kwargs[k] = v

        # ✅ 自动识别 extractor schema 并转换 extract_params 为 Pydantic 实例（再 model_dump）
        job_cls = self.JOB_CLASSES[job_name]
        if "extract_params" in kwargs and hasattr(job_cls, "EXTRACTOR_CLASS"):
            extractor_cls = getattr(job_cls, "EXTRACTOR_CLASS", None)
            if extractor_cls:
                extractor_init = inspect.signature(extractor_cls.__init__)
                if "params" in extractor_init.parameters:
                    param_annotation = extractor_init.parameters["params"].annotation
                    if hasattr(param_annotation, "model_validate"):
                        try:
                            kwargs["extract_params"] = param_annotation.model_validate(kwargs["extract_params"]).model_dump()
                        except Exception as e:
                            print(f"⚠️ 参数转换失败 [{job_name}.extract_params] → {e}")
                
        return kwargs

    def _init_jobs(self) -> dict[str, object]:
        job_names = (
            self.job_configs.keys() if self.only_configured else self.JOB_CLASSES.keys()
        )
        result_dict = {
            name: self.JOB_CLASSES[name](**self._get_job_init_kwargs(name))
            for name in job_names if name in self.JOB_CLASSES
        }
        return result_dict

    def get_job(self, name: str):
        return self.jobs.get(name)

    def run(self, job_name: str, dry_run: bool = False, session=None):
        print(f"\n🚀 开始运行 Job：{job_name}")
        job = self.jobs.get(job_name)
        if not job:
            raise ValueError(f"❌ 未知 job: {job_name}")
        
        df = job.run(dry_run=dry_run,session=session)  # ✅ 注入事务 session
        print(f"✅ {job_name} 完成，输出 {len(df)} 行。")
        return df

    def run_all(self, dry_run: bool = False, skip_jobs: list[str] = None, session=None) -> dict[str, object]:
        skip_jobs = skip_jobs or []
        run_order = self.DEFAULT_RUN_ORDER or list(self.jobs.keys())
        results = {}
        def _run_pipeline_jobs(session):
            for name in run_order:
                if name not in self.jobs:
                    print(f"⚠️ 跳过未注册 Job：{name}")
                    continue
                if name in skip_jobs:
                    print(f"⏭️ 手动跳过 Job：{name}")
                    continue

                job = self.jobs[name]
                print(f"\n🚀 开始运行 Job：{name}")
                output = job.run(dry_run=dry_run, session=session)
                results[name] = output
                print(f"✅ {name} 完成，输出 {len(output)} 行")

        if session is None:
            with UnitOfWork(self.config) as uow:
                _run_pipeline_jobs(uow.session)
        else:
            _run_pipeline_jobs(session)

        return results
    
