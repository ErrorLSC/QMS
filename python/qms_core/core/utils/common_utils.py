def convert_currency(amount:float, rate:float, method: str):
    """
    将金额按照汇率换算
    :param amount: 原始金额
    :param rate: 汇率值
    :param method: 'M' 表示乘法，'D' 表示除法
    :return: 转换后的金额
    """
    if method == 'M':
        return amount * rate
    elif method == 'D':
        return amount / rate if rate != 0 else None
    else:
        raise ValueError(f"未知汇率方法: {method}")