import datetime
import pandas as pd
from collections import defaultdict
from qms_core.infrastructure.db.models import DemandHistory, DemandHistoryRaw

def detect_and_mark_order_changes(config, fields_to_compare=None, dry_run: bool = False):
    """
    检测订单变更并回写 DemandHistoryRaw 表的标志字段。
    支持 dry_run 模式，用于审查变更内容而不写库。

    参数:
        config: MRPConfig 实例
        fields_to_compare: 可选，自定义需要比对的字段列表
        dry_run: 若为 True，则不写数据库，仅返回变更日志 DataFrame

    返回:
        df_log: 所有字段级别的变更记录
    """
    session = config.get_session()
    if fields_to_compare is None:
        fields_to_compare = ["LQORD", "Warehouse"]

    today = datetime.date.today()
    logs = []

    try:
        print("🔍 正在加载 CLEAN 与 RAW 订单数据...")
        clean_orders = session.query(DemandHistory).all()
        raw_orders = session.query(DemandHistoryRaw).filter(DemandHistoryRaw.IS_CHANGED == 0).all()

        clean_dict = {
            (o.LORD, o.LLINE, o.Warehouse): o
            for o in clean_orders
        }

        for raw in raw_orders:
            key = (raw.LORD, raw.LLINE, raw.Warehouse)
            clean = clean_dict.get(key)

            if clean is None:
                raw.IS_CHANGED = 1
                raw.CHANGE_DATE = today
                raw.CHANGE_REASON = "Order line missing (deleted or cancelled)"
                logs.append({
                    "LORD": raw.LORD,
                    "LLINE": raw.LLINE,
                    "Warehouse": raw.Warehouse,
                    "CHANGE_DATE": today,
                    "FIELD_NAME": "ROW",
                    "OLD_VALUE": None,
                    "NEW_VALUE": None,
                    "CHANGE_REASON": raw.CHANGE_REASON
                })
            else:
                field_changed = False
                for field in fields_to_compare:
                    raw_val = getattr(raw, field, None)
                    clean_val = getattr(clean, field, None)

                    if raw_val != clean_val:
                        field_changed = True
                        logs.append({
                            "LORD": raw.LORD,
                            "LLINE": raw.LLINE,
                            "Warehouse": raw.Warehouse,
                            "CHANGE_DATE": today,
                            "FIELD_NAME": field,
                            "OLD_VALUE": str(clean_val),
                            "NEW_VALUE": str(raw_val),
                            "CHANGE_REASON": f"{field} changed"
                        })

                if field_changed:
                    raw.IS_CHANGED = 1
                    raw.CHANGE_DATE = today
                    raw.CHANGE_REASON = "Field(s) changed"

        df_log = pd.DataFrame(logs)
        print(f"🧾 共检测出 {len(df_log)} 项字段级变更，涉及 {len(set((r['LORD'], r['LLINE']) for r in logs))} 条订单行。")

        if dry_run:
            print("🧪 Dry Run 模式：展示前 5 条变更记录：")
            print(df_log.head(5))
            print("✅ Dry Run 完成，未写入数据库。")
            return df_log

        print("✏️ 正在更新 RAW 表的状态字段...")
        grouped_changes = defaultdict(lambda: {"CHANGE_DATE": None, "FIELDS": set()})

        for _, row in df_log.iterrows():
            key = (row["LORD"], row["LLINE"], row["Warehouse"])
            change_date = pd.to_datetime(row["CHANGE_DATE"]).date()
            grouped_changes[key]["CHANGE_DATE"] = max(
                grouped_changes[key]["CHANGE_DATE"], change_date
            ) if grouped_changes[key]["CHANGE_DATE"] else change_date
            grouped_changes[key]["FIELDS"].add(row["FIELD_NAME"])

        for (lord, lline, warehouse), info in grouped_changes.items():
            raw_order = session.query(DemandHistoryRaw).filter_by(
                LORD=lord, LLINE=lline, Warehouse=warehouse
            ).first()
            if raw_order:
                raw_order.IS_CHANGED = 1
                raw_order.CHANGE_DATE = info["CHANGE_DATE"]
                raw_order.CHANGE_REASON = f"Field(s) changed: {', '.join(sorted(info['FIELDS']))}"

        session.commit()
        print(f"✅ RAW 表更新完成，共更新 {len(grouped_changes)} 条记录。")

        return df_log

    except Exception as e:
        session.rollback()
        print(f"❌ 检测或更新过程失败: {e}")
        raise

    finally:
        session.close()
