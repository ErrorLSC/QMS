from qms_core.adaptors.ETL.BPCS.common.bpcs_extractor import BPCSExtractor, BPCSExtractorMeta
from qms_core.adaptors.ETL.BPCS.po_leadtime import SQL_templates
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_schema import DomesticPOParams,POIntransitParams,OpenPOParams,OverseaPOParams,FreightChargeParams

class DomesticPOExtractor(BPCSExtractor):
    TEMPLATE_FILE = "PO_LT_DOMESTIC.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = DomesticPOParams
    DEFAULT_DATE_COLUMNS = [
        "PO_ENTRY_DATE",
        "CONFIRMED_DELIVERY_DATE",
        "LOCAL_STOCK_IN_DATE"
    ]

    def __init__(self, meta: BPCSExtractorMeta, params: DomesticPOParams):
        super().__init__(meta, params)

class POIntransitExtractor(BPCSExtractor):
    TEMPLATE_FILE = "PO_INTRANSIT.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = POIntransitParams
    DEFAULT_DATE_COLUMNS = ["PO_ENTRY_DATE", "INVOICE_DATE"]

    def __init__(self, meta: BPCSExtractorMeta, params: POIntransitParams):
        super().__init__(meta, params)
    
class OverseaPOExtractor(BPCSExtractor):
    TEMPLATE_FILE = "PO_LT_OVERSEA.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = OverseaPOParams
    DEFAULT_DATE_COLUMNS = ["PO_ENTRY_DATE", "OVERSEA_INVOICE_DATE", "OVERSEA_STOCK_IN_DATE"]

    def __init__(self, meta: BPCSExtractorMeta, params: OverseaPOParams):
        super().__init__(meta, params)

class OpenPOExtractor(BPCSExtractor):
    TEMPLATE_FILE = "OPEN_PO.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = OpenPOParams
    DEFAULT_DATE_COLUMNS = ["POENTRYDATE", "DUEDATE", "DELIVERYDATE"]

    def __init__(self, meta: BPCSExtractorMeta, params: OpenPOParams):
        super().__init__(meta, params)

class FreightChargeExtractor(BPCSExtractor):
    TEMPLATE_FILE = "FREIGHT_COST.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = FreightChargeParams
    DEFAULT_DATE_COLUMNS = ["IHINVD"]

    def __init__(self, meta: BPCSExtractorMeta, params: FreightChargeParams):
        super().__init__(meta, params)
