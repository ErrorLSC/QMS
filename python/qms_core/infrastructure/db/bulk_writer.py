import pandas as pd
import numpy as np
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
import datetime
from sqlalchemy import text,inspect
from qms_core.infrastructure.db.reader import fetch_orm_data


def smart_batch_size(df: pd.DataFrame, user_batch_size: int = None, max_params: int = 999) -> int:
    """
    智能批量计算函数：自动在性能和 SQLite 安全限制之间取得最佳平衡
    - 若用户指定 batch_size，则优先使用
    - 否则自动根据列数和最大SQL参数限制推断最优批大小
    """

    if user_batch_size:
        return user_batch_size

    num_rows = len(df)
    num_columns = max(1, len(df.columns))

    # 严格控制：总变量不超过 max_params（默认999）
    max_safe_batch = max_params // num_columns

    # 切忌批量过小，设置性能保底
    return min(num_rows, max(max_safe_batch, 25))

def write_dataframe_to_table_by_orm(
    config,
    df,
    orm_class,
    delete_before_insert: bool = False,
    upsert: bool = False,
    hot_zone_delete: bool = False,
    hot_zone_column: str = None,
    hot_zone_days: int = 180,
    batch_size: int = None,
    session=None,
    manage_transaction: bool = True
):
    if df.empty:
        print(f"⚠️ DataFrame为空，跳过写入 {orm_class.__tablename__}。")
        return

    df = df.dropna(how="all")
    engine = config.get_engine()
    session = session or config.get_session()

    if delete_before_insert and upsert:
        print("⚠️ 同时启用了 delete_before_insert 和 upsert，系统将自动执行 INSERT 模式。")
        upsert = False

    dialect = engine.dialect.name
    if dialect == "sqlite":
        insert_stmt_fn = sqlite_insert
    elif dialect == "postgresql":
        insert_stmt_fn = pg_insert
    else:
        raise NotImplementedError(f"❌ 当前数据库 {dialect} 不支持自动upsert，请手动扩展！")

    batch_size = smart_batch_size(df, batch_size)

    def _write_core():
        if delete_before_insert:
            session.query(orm_class).delete()
            print(f"🗑️ 表 {orm_class.__tablename__} 已清空，准备重新插入。")
        elif hot_zone_delete:
            if not hot_zone_column:
                raise ValueError("⚠️ 开启 hot_zone_delete 时，必须指定 hot_zone_column。")
            cutoff_date = (datetime.datetime.today() - datetime.timedelta(days=hot_zone_days)).strftime("%Y-%m-%d")
            session.execute(
                text(f"DELETE FROM {orm_class.__tablename__} WHERE {hot_zone_column} >= '{cutoff_date}'")
            )
            print(f"🗑️ 表 {orm_class.__tablename__} 已清除热区（最近{hot_zone_days}天）数据。")

        valid_columns = [col.name for col in orm_class.__table__.columns]
        df_trimmed = df[[col for col in valid_columns if col in df.columns]]

        total_inserted = 0
        for i in range(0, len(df_trimmed), batch_size):
            batch_df = df_trimmed.iloc[i:i+batch_size]
            if upsert:
                insert_stmt = insert_stmt_fn(orm_class).values(batch_df.to_dict(orient="records"))
                update_columns = [
                    col.name for col in orm_class.__table__.columns
                    if not col.primary_key and col.name in batch_df.columns
                ]
                update_dict = {col: insert_stmt.excluded[col] for col in update_columns}
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[col.name for col in orm_class.__table__.primary_key],
                    set_=update_dict
                )
                session.execute(upsert_stmt)
            else:
                records = [orm_class(**row.dropna().to_dict()) for _, row in batch_df.iterrows()]
                session.bulk_save_objects(records)

            total_inserted += len(batch_df)

        mode = "upsert" if upsert else "insert"
        print(f"✅ 成功 {mode} {total_inserted} 条记录到 {orm_class.__tablename__}（每批最多{batch_size}条）")

    try:
        if manage_transaction:
            with session.begin():
                _write_core()
        else:
            _write_core()

    except Exception as e:
        session.rollback()
        print(f"❌ 写入 {orm_class.__tablename__} 失败: {e}")
        raise

    finally:
        if manage_transaction:
            session.close()


class SmartTableWriter:
    def __init__(self, 
                 config, orm_class, 
                 key_fields=None, monitor_fields=None, log_table_model=None, 
                 enable_logging=False,change_reason: str = "System Update",
                 exclude_fields: list[str]= None,
                 only_update_delta: bool = True,  # ✅ 新增
                 skip_if_unchanged: bool = True,  # ✅ 新增
                 write_params: dict = None):
        """
        :param config: 配置对象，包含数据库连接
        :param orm_class: ORM 类（目标表）
        :param key_fields: 主键字段名列表；若为空将自动从 ORM 元数据推断
        :param monitor_fields: 要监测变更的字段列表（用于比较和记录日志）
        :param log_table_model: ORM 类（日志表）
        :param enable_logging: 是否启用字段变化日志记录
        """
        self.config = config
        self.orm_class = orm_class
        self.exclude_fields = exclude_fields or [] 

        self.key_fields = key_fields or self._infer_primary_keys()
        self.monitor_fields = monitor_fields or self._infer_monitor_fields()

        self.log_table_model = log_table_model
        self.enable_logging = enable_logging
        self.change_reason = change_reason
        self.write_params = write_params or {}
        self.only_update_delta = only_update_delta
        self.skip_if_unchanged = skip_if_unchanged
        

    def _infer_primary_keys(self):
        """自动获取 ORM 表的主键字段名"""
        mapper = inspect(self.orm_class)
        return [col.name for col in mapper.primary_key]
    
    def _infer_monitor_fields(self) -> list[str]:
        mapper = inspect(self.orm_class)
        all_fields = [c.key for c in mapper.columns]
        pk_fields = [c.name for c in mapper.primary_key]

        monitor_fields = [f for f in all_fields if f not in pk_fields and f not in self.exclude_fields]

        print(f"ℹ️ monitor_fields 未指定，默认使用所有非主键字段（排除字段：{self.exclude_fields}）")
        return monitor_fields

    def write(
        self,
        df: pd.DataFrame,
        dry_run: bool = True,
        existing_df: pd.DataFrame = None,
        session=None  # ✅ 外部事务注入
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        智能写入目标 ORM 表。支持外部事务（session）传入。
        若 dry_run=True，则不会执行数据库写入，仅返回差异行与日志内容。
        """
        print("📝 实际写入参数：", self.write_params)
        print("📝 log table：", self.log_table_model)
        if df.empty:
            print("⚠️ DataFrame 为空，跳过写入。")
            return df, pd.DataFrame()

        self._external_session = session is not None
        self.session = session or self.config.get_session()

        try:
            if not self._external_session:
                self.session.begin()  # ✅ 仅内部 session 开启事务

            df_existing = self._fetch_existing(df, override_df=existing_df)

            if self.only_update_delta:
                delta_df, log_df = self._get_delta(df, df_existing)
            else:
                delta_df = df
                log_df = pd.DataFrame()

            if self.skip_if_unchanged and delta_df.empty:
                print("✅ 无变更数据，跳过写入")
                return delta_df, log_df

            if dry_run:
                print(f"🧪 Dry Run 模式下发现 {len(delta_df)} 条变更，未写入数据库。")
                return delta_df, log_df

            # 写入日志（若启用）
            if self.enable_logging and self.log_table_model and not log_df.empty:
                print(f"{len(log_df)} 条变化将会被写入日志")
                write_dataframe_to_table_by_orm(
                    config=self.config,
                    df=log_df,
                    orm_class=self.log_table_model,
                    session=self.session,
                    manage_transaction=False,
                    upsert=True
                )

            # 写入主表
            write_dataframe_to_table_by_orm(
                config=self.config,
                df=delta_df,
                orm_class=self.orm_class,
                session=self.session,
                manage_transaction=False,
                **self.write_params
            )

            if not self._external_session:
                self.session.commit()

            return delta_df, log_df

        except Exception as e:
            if not self._external_session:
                self.session.rollback()
            print("❌ 写入失败，已回滚")
            raise

        finally:
            if not self._external_session:
                self.session.close()
                self.session = None

    def _fetch_existing(self,df:pd.DataFrame,override_df:pd.DataFrame=None) -> pd.DataFrame:
        if override_df is not None:
            return override_df
        return fetch_orm_data(self.config, self.orm_class,session = self.session)
    
    def _align_column_types(self,df1: pd.DataFrame, df2: pd.DataFrame, fields: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        for field in fields:
            if field in df1.columns and field in df2.columns:
                if df1[field].dtype == "object":
                    df1[field] = df1[field].apply(lambda x: x.name if hasattr(x, "value") else x)
                if df2[field].dtype == "object":
                    df2[field] = df2[field].apply(lambda x: x.name if hasattr(x, "value") else x)

                common_dtype = np.result_type(df1[field].dtype, df2[field].dtype)
                df1[field] = df1[field].astype(common_dtype)
                df2[field] = df2[field].astype(common_dtype)
        return df1, df2
    
    def _get_delta(self, df_new: pd.DataFrame, df_old: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df_old.empty:
            print("BLANK!!!!")
            return df_new, pd.DataFrame()  # 全部新行，写主表但无日志

        df_new_indexed = df_new.set_index(self.key_fields)
        df_old_indexed = df_old.set_index(self.key_fields)

        # 可比较字段 = 除去 exclude 的所有字段交集
        all_fields = [f for f in df_new.columns if f not in self.key_fields and f not in self.exclude_fields]
        valid_fields = [f for f in all_fields if f in df_old.columns]

        # 日志字段：在 valid_fields 中又在 monitor_fields 中的字段
        monitored_fields = [f for f in self.monitor_fields if f in valid_fields]

        # 强制类型对齐 & 枚举 value 化
        df_new_indexed, df_old_indexed = self._align_column_types(df_new_indexed, df_old_indexed, valid_fields)
        # print("🧪 字段对齐前类型：")
        # for f in valid_fields:
        #     dtype_new = df_new_indexed[f].dtype if f in df_new_indexed.columns else "N/A"
        #     dtype_old = df_old_indexed[f].dtype if f in df_old_indexed.columns else "N/A"
        #     print(f"  - {f}: new={dtype_new}, old={dtype_old}")

        df_old_subset = df_old_indexed[valid_fields]
        joined = df_new_indexed.join(df_old_subset, rsuffix="_old", how="left")

        matched = joined[[f"{f}_old" for f in valid_fields]].notna().all(axis=1)
        is_new = ~matched

        changed_any = pd.Series(False, index=joined.index)
        changed_monitored = pd.Series(False, index=joined.index)
        changed_fields_by_row = {}

        for field in valid_fields:
            new_val = joined[field]
            old_val = joined[f"{field}_old"]
            diff = (new_val != old_val) & ~(new_val.isna() & old_val.isna())

            changed_any |= diff
            if field in monitored_fields:
                changed_monitored |= diff

            if field in monitored_fields:
                for idx in joined.index[diff]:
                    changed_fields_by_row.setdefault(idx, []).append(field)

        # 主表更新条件：任何字段有变更或新行
        mask_write_main = is_new | changed_any
        delta_df = joined[mask_write_main].reset_index()[df_new.columns]

        # 日志更新条件：只监控字段变更
        log_df = pd.DataFrame()
        if self.enable_logging and self.log_table_model and changed_monitored.any():
            from datetime import datetime

            records = []
            for idx in joined.index[changed_monitored]:
                if isinstance(self.key_fields, list):
                    if isinstance(idx, tuple):
                        item_key = {k: v for k, v in zip(self.key_fields, idx)}
                    else:
                        item_key = {self.key_fields[0]: idx}
                else:
                    item_key = {self.key_fields: idx}
                for field in changed_fields_by_row.get(idx, []):
                    if field in monitored_fields:
                        records.append({
                            **item_key,
                            "FIELD_NAME": field,
                            "OLD_VALUE": joined.at[idx, f"{field}_old"],
                            "NEW_VALUE": joined.at[idx, field],
                            "CHANGE_DATE": datetime.now(),
                            "CHANGE_REASON": self.change_reason or "SmartWriter Update"
                        })

            if records:
                log_df = pd.DataFrame(records)

        return delta_df, log_df

