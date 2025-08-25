import pandas as pd

def generate_virtual_po_sublines(df: pd.DataFrame,
                                  po_col: str = 'PONUM',
                                  line_col: str = 'POLINE',
                                  sort_cols: list = None,
                                  new_col: str = 'POLINE') -> pd.DataFrame:
    """
    为重复的 PO + 行号记录生成虚拟分行编号，例如将 POLINE 从 10 拆分为 10-1、10-2 等。

    参数:
        df: 原始 DataFrame
        po_col: PO编号列名，默认 'PONUM'
        line_col: 行号列名，默认 'POLINE'
        sort_cols: 组内排序所用的列，默认按 ['InvoiceDate', 'ActualDeliveryDate']
        new_col: 新 POLINE 要替换的列名，默认覆盖原来的 'POLINE'

    返回:
        带虚拟行号的新 DataFrame
    """
    df = df.copy()

    if sort_cols is None:
        sort_cols = ['InvoiceDate', 'ActualDeliveryDate']

    df = df.sort_values([po_col, line_col] + sort_cols)

    # 组内数量
    df['__grp_size'] = df.groupby([po_col, line_col])[line_col].transform('size')
    df['__subline'] = df.groupby([po_col, line_col]).cumcount() + 1

    # 拼接新行号
    df[new_col] = df.apply(
        lambda row: f"{row[line_col]}-{row['__subline']}"
        if row['__grp_size'] > 1 else str(row[line_col]),
        axis=1
    )

    # 清理中间列
    df = df.drop(columns=['__grp_size', '__subline'])

    return df

def generate_tail_batch_poline(base_poline: str, batch_index: int, base_index: int = None) -> str:
    """
    为尾批模拟生成统一编号：
    - 保留原始编号（如 `1-2`）
    - 从已有最大编号继续递增（如已有到 `1-2`，从 `1-3` 开始）

    参数:
        base_poline: 原始 POLINE（可能是 "1-2"）
        batch_index: 当前是模拟的第几个 batch（从 1 开始）
        base_index: 已存在的最大后缀编号（如 "1-2" 中的 2）

    返回:
        生成的新 POLINE（如 "1-3"）
    """
    if base_index is None:
        match = re.match(r"^(.*)-(\d+)$", base_poline)
        if match:
            prefix, suffix = match.groups()
            base_index = int(suffix)
        else:
            prefix = base_poline
            base_index = 0
    else:
        prefix = base_poline.rsplit("-", 1)[0]

    new_index = base_index + batch_index
    return f"{prefix}-{new_index}"