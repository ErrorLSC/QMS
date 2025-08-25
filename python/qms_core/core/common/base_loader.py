from qms_core.core.common.params.loader_params import LoaderParams
import pandas as pd
from qms_core.infrastructure.db.bulk_writer import SmartTableWriter,write_dataframe_to_table_by_orm
import importlib
import os
from datetime import datetime

class BaseLoader:
    def __init__(self, config, orm_class, params: LoaderParams):
        self.config = config
        self.orm_class = orm_class
        self.params = params

    def write(self, df: pd.DataFrame, dry_run=True, session=None,export_csv=False, csv_dir="output") -> pd.DataFrame:
        if self.params.use_smart_writer:
            writer = SmartTableWriter(
                config=self.config,
                orm_class=self.orm_class,
                key_fields=self.params.key_fields,
                monitor_fields=self.params.monitor_fields,
                log_table_model=self._resolve_log_model(),
                enable_logging=self.params.log_table_model is not None,
                change_reason=self.params.change_reason,
                exclude_fields=self.params.exclude_fields,
                only_update_delta=self.params.only_update_delta,
                skip_if_unchanged=self.params.skip_if_unchanged,
                write_params=self.params.write_params,
            )
            delta_df, log_df = writer.write(df, session=session, dry_run=dry_run)
            self._print_report(delta_df, log_df, dry_run)

            if export_csv:
                self._export_to_csv(delta_df, log_df, csv_dir)

            return delta_df
        else:
            if dry_run:
                print("🧪 DRY RUN 模式（常规写入）：")
                print(df.head(5))
                return df
            write_dataframe_to_table_by_orm(
                config=self.config,
                df=df,
                orm_class=self.orm_class,
                session=session,
                **self.params.write_params,
            )
            print(f"✅ 写入完成，共 {len(df)} 行")
            return df

    def _resolve_log_model(self):
        model = self.params.log_table_model
        if isinstance(model, str):
            try:
                module_path, class_name = model.rsplit(".", 1)
                module = importlib.import_module(module_path)
                resolved = getattr(module, class_name)
                print(f"🛠️ 日志表模型已解析: {model} → {resolved}")
                return resolved
            except Exception as e:
                print(f"⚠️ 日志表模型无法解析: {model} → {e}")
                return None
        return model
    
    def _print_report(self, delta_df, log_df, dry_run):
        if dry_run:
            print(f"🧪 DRY RUN（SmartWriter）：检测到 {len(delta_df)} 条变更，{len(log_df)} 条日志")
            if not delta_df.empty:
                print("📌 差异数据预览（前5行）：")
                print(delta_df.head(5))
            if not log_df.empty:
                print("📝 字段变更日志预览（前5行）：")
                print(log_df.head(5))
        else:
            print(f"✅ 写入完成（SmartWriter），变更记录数：{len(delta_df)}")

    def _export_to_csv(self, delta_df, log_df, csv_dir):
        os.makedirs(csv_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not delta_df.empty:
            delta_path = os.path.join(csv_dir, f"delta_{timestamp}.csv")
            delta_df.to_csv(delta_path, index=False)
            print(f"📤 差异数据已导出至 {delta_path}")
        if not log_df.empty:
            log_path = os.path.join(csv_dir, f"log_{timestamp}.csv")
            log_df.to_csv(log_path, index=False)
            print(f"📤 字段变更日志已导出至 {log_path}")