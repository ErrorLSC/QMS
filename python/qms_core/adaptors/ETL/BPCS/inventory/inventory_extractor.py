from qms_core.adaptors.ETL.BPCS.common.bpcs_extractor import BPCSExtractor,BPCSExtractorMeta
from qms_core.adaptors.ETL.BPCS.inventory.inventory_schema import InventoryParams
from qms_core.adaptors.ETL.BPCS.inventory import SQL_templates

class ILIExtractor(BPCSExtractor):
    TEMPLATE_FILE = "ILI.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = InventoryParams

    def __init__(self, meta: BPCSExtractorMeta, params: InventoryParams):  
        super().__init__(meta, params)

class IWIAvailExtractor(BPCSExtractor):
    TEMPLATE_FILE = "IWI_Avail.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = InventoryParams

    def __init__(self, meta: BPCSExtractorMeta, params: InventoryParams):  
        super().__init__(meta, params)
