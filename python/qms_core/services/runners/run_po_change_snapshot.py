from qms_core.infrastructure.config import MRPConfig
from qms_core.services.warehouse_filter import get_active_warehouses
from qms_core.pipelines.ETL.po_change_log.po_change_log_job import POChangeLogJob

def main(dry_run=False):
    print("🚀 启动 PO Change Log Job...")

    # 初始化配置
    config = MRPConfig()
    dsn = "JPNPRDF"
    active_warehouses = get_active_warehouses(config=config, WMFAC="JPE")

    # 构造 job 配置（与旧 Pipeline 参数一致）


    # 执行 Job（不再使用自定义 Pipeline 类）
    job = POChangeLogJob(
        config=config,
        dsn=dsn,
        extract_params={"warehouses": active_warehouses}  # ✅ 字段名称对应 schema
    )
    df_log,df_open_po = job.run(dry_run=dry_run)

    print(f"✅ 运行完成，共生成变更记录 {len(df_log)} 条,Open PO {len(df_open_po)} 条")
    if dry_run:
        print("🧪 当前为 dry_run 模式，未执行写库。")

if __name__ == "__main__":
    main(dry_run=False)