from qms_core.infrastructure.db.models import IIM,DPS

def update_rpflag(config, dry_run: bool = False):
    """
    更新 IIM 表中的 RPFLAG 字段：
    - 全部置为 NULL
    - 将出现在 DPS 表中 TYPE='1' 的 ITEMNUM_PARENT 设置为 'Y'

    参数:
        dry_run: 是否为测试模式，仅打印将被更新的记录，不执行写库
    """
    session = config.get_session()
    updated_count = 0

    try:
        if dry_run:
            print("🧪 [Dry Run] 清空 RPFLAG 跳过实际写入。")
        else:
            print("🔁 正在清空 IIM.RPFLAG 字段...")
            session.query(IIM).update({IIM.RPFLAG: None})
            session.commit()

        print("🔍 查询 DPS 中的 ITEMNUM_PARENT（TYPE='1'）...")
        itemnums = (
            session.query(DPS.ITEMNUM_PARENT)
            .filter(DPS.TYPE == '1')
            .distinct()
            .all()
        )
        itemnum_list = [row[0] for row in itemnums]

        if itemnum_list:
            print(f"✏️ 找到 {len(itemnum_list)} 个符合条件的物料。")
            if dry_run:
                print("🧪 [Dry Run] 以下是前 10 个将被设置为 RPFLAG='Y' 的物料：")
                for item in itemnum_list[:10]:
                    print(f" - {item}")
                print("✅ Dry Run 完成。未进行任何写入。")
            else:
                updated_count = (
                    session.query(IIM)
                    .filter(IIM.ITEMNUM.in_(itemnum_list))
                    .update({IIM.RPFLAG: 'Y'}, synchronize_session=False)
                )
                session.commit()
                print(f"✅ RPFLAG 更新完成，实际更新 {updated_count} 行。")
        else:
            print("ℹ️ 无需更新，未找到匹配的 DPS 条目。")

    except Exception as e:
        session.rollback()
        print(f"❌ 更新 RPFLAG 失败: {e}")

    finally:
        session.close()


