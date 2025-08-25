import datetime
from qms_core.infrastructure.db.models.safety_stock import SAFETY_TRANSFER_LOG
from qms_core.infrastructure.db.reader import run_sql

def transfer_safety_stock_with_log(config, operator="system", dry_run: bool = False):
    """
    子件继承母件安全库存，生成日志，并将母件安全库存归零。

    参数:
        operator: 操作人标记（如 'system'）
        dry_run: 是否为只读测试模式（不写入任何数据）
    """
    session = config.get_session()

    try:
        print("🔍 查询可继承的 DPS 替代记录...")
        result = run_sql(session, """
            SELECT
                P.Warehouse,
                D.ITEMNUM_PARENT,
                D.ITEMNUM_CHILD,
                P.WSAFE AS PARENT_WSAFE,
                C.WSAFE AS CHILD_WSAFE_BEFORE,
                ROUND(P.WSAFE / D.PSCQTY, 2) AS CHILD_WSAFE_AFTER,
                D.PSCQTY
            FROM DPS D
            JOIN IWI P ON P.ITEMNUM = D.ITEMNUM_PARENT AND P.Warehouse IS NOT NULL
            JOIN IWI C ON C.ITEMNUM = D.ITEMNUM_CHILD AND C.Warehouse = P.Warehouse
            WHERE D.TYPE = '1'
              AND D.PSCQTY > 0
              AND (C.WSAFE IS NULL OR C.WSAFE <= 0)
              AND P.WSAFE > 0
        """).fetchall()

        if not result:
            print("ℹ️ 无需继承，未找到符合条件的记录。")
            return

        print(f"✏️ 准备记录 {len(result)} 条转移日志...")
        if dry_run:
            print("🧪 [Dry Run] 以下为前 5 条示例：")
            for row in result[:5]:
                print(f"  📦 {row[1]} → {row[2]}（{row[3]} → {row[5]}）")
            print("✅ Dry Run 完成，未进行写库操作。")
            return

        # Step 1: 写入日志
        logs = []
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for row in result:
            logs.append(SAFETY_TRANSFER_LOG(
                TRANSFER_DATE=now_str,
                Warehouse=row[0],
                ITEMNUM_PARENT=row[1],
                ITEMNUM_CHILD=row[2],
                PARENT_WSAFE=row[3],
                CHILD_WSAFE_BEFORE=row[4],
                CHILD_WSAFE_AFTER=row[5],
                PSCQTY=row[6],
                OPERATOR=operator
            ))
        session.bulk_save_objects(logs)
        session.commit()

        print("🔁 更新子件安全库存...")
        run_sql(session, """
            UPDATE IWI
            SET WSAFE = (
                SELECT ROUND(P.WSAFE / D.PSCQTY, 2)
                FROM DPS D
                JOIN IWI P ON P.ITEMNUM = D.ITEMNUM_PARENT AND P.Warehouse = IWI.Warehouse
                WHERE D.ITEMNUM_CHILD = IWI.ITEMNUM
                  AND D.TYPE = '1'
                  AND D.PSCQTY > 0
            )
            WHERE EXISTS (
                SELECT 1
                FROM DPS D
                JOIN IWI P ON P.ITEMNUM = D.ITEMNUM_PARENT AND P.Warehouse = IWI.Warehouse
                WHERE D.ITEMNUM_CHILD = IWI.ITEMNUM
                  AND D.TYPE = '1'
                  AND D.PSCQTY > 0
                  AND (IWI.WSAFE IS NULL OR IWI.WSAFE <= 0)
                  AND P.WSAFE > 0
            )
        """)
        session.commit()

        print("🔁 清零母件安全库存...")
        run_sql(session, """
            UPDATE IWI
            SET WSAFE = 0
            WHERE EXISTS (
                SELECT 1
                FROM DPS D
                JOIN IWI C ON C.ITEMNUM = D.ITEMNUM_CHILD AND C.Warehouse = IWI.Warehouse
                WHERE D.ITEMNUM_PARENT = IWI.ITEMNUM
                  AND D.TYPE = '1'
                  AND D.PSCQTY > 0
                  AND (C.WSAFE IS NOT NULL AND C.WSAFE > 0)
                  AND IWI.WSAFE > 0
            )
        """)
        session.commit()

        print(f"✅ 替代继承+日志记录+母件归零完成，共写入 {len(logs)} 条转移日志。")

    except Exception as e:
        session.rollback()
        print(f"❌ 安全库存继承失败: {e}")

    finally:
        session.close()
