import pandas as pd
from qms_core.core.common.base_transformer import BaseTransformer
from qms_core.adaptors.ETL.BPCS.common.utils import map_transport_mode
from qms_core.core.utils.po_utils import generate_virtual_po_sublines
from qms_core.core.common.params.enums import TransportMode

def compute_three_stage_leadtime(
    df: pd.DataFrame,
    po_date_col: str = "POEntryDate",
    invoice_date_col: str = "InvoiceDate",
    delivery_date_col: str = "ActualDeliveryDate"
) -> pd.DataFrame:
    """
    通用三段交期计算：
    - PrepareTime   = InvoiceDate - POEntryDate
    - TransportTime = ActualDeliveryDate - InvoiceDate
    - TotalLeadTime = ActualDeliveryDate - POEntryDate
    """
    df = df.copy()
    df["PrepareTime"] = (df[invoice_date_col] - df[po_date_col]).dt.days
    df["TransportTime"] = (df[delivery_date_col] - df[invoice_date_col]).dt.days
    df["TotalLeadTime"] = (df[delivery_date_col] - df[po_date_col]).dt.days
    return df

class POIntransitTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._standardize_fields(df)
        df = self._assign_transport_mode(df)
        df = self._correct_remaining_qty(df)
        df = self._insert_virtual_tail_lines(df)
        df = self._generate_virtual_lines(df)
        return df

    def _standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            'PONUM': 'PONUM',
            'POLINE': 'POLINE',
            'ITEMNUM': 'ITEMNUM',
            'PQORD': 'OrderedQty',
            'PQREM': 'RemainingQty',
            'IN_TRANSIT_QTY': 'InTransitQty',
            'INVOICE_DATE': 'InvoiceDate',
            'PO_ENTRY_DATE': 'POEntryDate',
            'POTPTC': 'RawTransportCode',
            'ORDER_TYPE': 'OrderType',
            'WAREHOUSE': 'Warehouse',
            'VENDORCODE': 'VendorCode',
            'COMMENT': 'Comment'
        }
        return df.rename(columns=rename_map)

    def _assign_transport_mode(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['TransportMode'] = df['RawTransportCode'].apply(map_transport_mode)
        return df

    def _correct_remaining_qty(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 1️⃣ 聚合出总 InTransitQty
        agg_df = df.groupby(["PONUM", "POLINE"], as_index=False)["InTransitQty"].sum()
        agg_df = agg_df.rename(columns={"InTransitQty": "InTransitQty_sum"})

        # 2️⃣ 合并聚合结果
        df = df.merge(agg_df, on=["PONUM", "POLINE"], how="left")

        # 3️⃣ 计算 error mask：实际剩余 + 总在途 > 订单数
        df["ExpectedRemaining"] = df["OrderedQty"] - df["InTransitQty_sum"]
        error_mask = df["RemainingQty"] > df["ExpectedRemaining"]

        # 4️⃣ 修正 RemainingQty
        df.loc[error_mask, "RemainingQty"] = df.loc[error_mask, "ExpectedRemaining"]

        # 5️⃣ 标记修正痕迹
        df["PQREM_Corrected"] = error_mask.astype(int)

        # 6️⃣ 清理临时列
        return df.drop(columns=["InTransitQty_sum", "ExpectedRemaining"])

    def _insert_virtual_tail_lines(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        插入尾批虚拟子行（[VIRTUAL_TAIL]），用于表示剩余尚未发货的部分。
        - 仅对 RemainingQty > 0 且 < OrderedQty 的 PO 行插入；
        - InvoiceDate 为空；
        - Comment 字段增加标记。
        """
        df = df.copy()

        # 1️⃣ 找出哪些 PO 行需要插入尾批
        group_df = (
            df.groupby(["PONUM", "POLINE"], as_index=False)
            .agg({
                "OrderedQty": "first",
                "RemainingQty": "first"
            })
        )

        tail_needed = group_df[
            (group_df["RemainingQty"] > 0) &
            (group_df["RemainingQty"] < group_df["OrderedQty"])
        ]

        tail_keys = set(zip(tail_needed["PONUM"], tail_needed["POLINE"]))
        df["__NEED_TAIL__"] = df.apply(lambda r: (r["PONUM"], r["POLINE"]) in tail_keys, axis=1)

        # 2️⃣ 构造虚拟尾批子行
        virtual_rows = []
        for (ponum, poline), group in df[df["__NEED_TAIL__"]].groupby(["PONUM", "POLINE"]):
            row = group.iloc[0].copy()
            remaining_qty = row["RemainingQty"]
            if remaining_qty <= 0:
                continue

            row["RemainingQty"] = remaining_qty
            row["InTransitQty"] = 0.0
            row["InvoiceDate"] = None
            row["PQREM_Corrected"] = 0
            comment = row.get("Comment") or ""
            row["Comment"] = comment + "[VIRTUAL_TAIL]"
            virtual_rows.append(row)

        # 3️⃣ 原分行清零 RemainingQty
        df.loc[df["__NEED_TAIL__"], "RemainingQty"] = 0.0

        # 4️⃣ 合并尾批子行
        if virtual_rows:
            df_virtual = pd.DataFrame(virtual_rows)
            df = pd.concat([df, df_virtual], ignore_index=True)

        return df.drop(columns=["__NEED_TAIL__"])

    def _generate_virtual_lines(self, df: pd.DataFrame) -> pd.DataFrame:
        return generate_virtual_po_sublines(
            df=df,
            po_col='PONUM',
            line_col='POLINE',
            sort_cols=['PONUM', 'POLINE', 'InvoiceDate'],
            new_col='POLINE'
        )

class OpenPOTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={
            'POENTRYDATE': 'POEntryDate',
            'DUEDATE': 'DueDate',
            'DELIVERYDATE': 'AcknowledgedDeliveryDate',
            'POTPTC': 'RawTransportCode',
            'VENDORCODE': 'VendorCode',
            'WAREHOUSE': 'Warehouse',
            'PCMT': 'Comment'
        })
        df['TransportMode'] = df['RawTransportCode'].apply(map_transport_mode)
        return df

class OverseaPOTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._standardize_fields(df)
        df = generate_virtual_po_sublines(
            df=df,
            po_col='PONUM',
            line_col='POLINE',
            sort_cols=['PONUM', 'POLINE', 'InvoiceDate', 'ActualDeliveryDate'],
            new_col='POLINE'
        )
        df = compute_three_stage_leadtime(df)
        df = self._assign_transport_mode(df)
        return df

    def _standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            'PORD': 'PONUM',
            'PLINE': 'POLINE',
            'PPROD': 'ITEMNUM',
            'PQORD': 'OrderedQty',
            'RECEIVED_QTY': 'ReceivedQty',
            'PO_ENTRY_DATE': 'POEntryDate',
            'OVERSEA_INVOICE_DATE': 'InvoiceDate',
            'OVERSEA_STOCK_IN_DATE': 'ActualDeliveryDate',
            'POTPTC': 'RawTransportCode',
            'PVEND': 'VendorCode',
            'PWHSE': 'Warehouse',
            'IS_PO_CLOSED': 'IsClosed'
        })

    def _assign_transport_mode(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['TransportMode'] = df['RawTransportCode'].apply(map_transport_mode)
        return df

class DomesticPOTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._standardize_fields(df)
        df = compute_three_stage_leadtime(df)
        df = self._assign_default_transport_mode(df)
        df = self._fix_transport_anomalies(df)
        return df

    def _standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            'PORD': 'PONUM',
            'PLINE': 'POLINE',
            'PWHSE': 'Warehouse',
            'PVEND': 'VendorCode',
            'PPROD': 'ITEMNUM',
            'PQORD': 'OrderedQty',
            'PQREC': 'ReceivedQty',
            'LOT_NUMBER': 'LotNumber',
            'PO_ENTRY_DATE': 'POEntryDate',
            'CONFIRMED_DELIVERY_DATE': 'InvoiceDate',
            'LOCAL_STOCK_IN_DATE': 'ActualDeliveryDate',
            'IS_PO_CLOSED': 'IsClosed'
        }
        return df.rename(columns=rename_map)

    def _assign_default_transport_mode(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['TransportMode'] = TransportMode.TRUCK.value
        return df

    def _fix_transport_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['TransportTimeFlag'] = 'OK'
        upper = TransportMode.TRUCK.lt_range[1]

        # 负值修正
        mask_negative = df['TransportTime'] < 0
        df.loc[mask_negative, 'TransportTime'] = 0
        df.loc[mask_negative, 'PrepareTime'] = df.loc[mask_negative, 'TotalLeadTime']
        df.loc[mask_negative, 'TransportTimeFlag'] = 'NEGATIVE_TRANSPORT_FIXED'

        # 超上限修正
        mask_truck_over = df['TransportTime'] > upper
        excess_days = df.loc[mask_truck_over, 'TransportTime'] - upper
        df.loc[mask_truck_over, 'PrepareTime'] += excess_days
        df.loc[mask_truck_over, 'TransportTime'] = upper
        df.loc[mask_truck_over, 'TransportTimeFlag'] = 'TRUCK_OVERSHOOT_CORRECTED'

        return df
    
class FreightChargeTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def _assign_transport_mode(self, df: pd.DataFrame) -> pd.DataFrame:
        df['SHTPTC'] = df['SHTPTC'].apply(map_transport_mode)
        return df
    
    def _standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            'IHINVD': 'InvoiceDate',
            'SHTPTC': 'TransportMode',
            'IHSHPN': 'ShipmentNum',
            'IHSPCD': 'SupplierGlobalCode',
            'IHPRCC': 'POCurrency',
            'IHPRCT': 'InvoiceTotal',
            'ITEMTOTAL': 'ItemTotal',
            'WAREHOUSE': 'Warehouse',
            'GROSSWEIGHT': 'GrossWeight',
            'FREIGHT_CHARGE': 'FreightCharge',
        })
    
    def transform(self, df:pd.DataFrame):
        df = self._assign_transport_mode(df)
        df = self._standardize_fields(df)
        return df
