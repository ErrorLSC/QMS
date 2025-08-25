from qms_core.adaptors.ETL.BPCS.common.bpcs_extractor import BPCSExtractor,BPCSExtractorMeta
from qms_core.adaptors.ETL.BPCS.demand_history import SQL_templates
from qms_core.adaptors.ETL.BPCS.demand_history.demand_history_schema import DemandHistoryParams

class DemandHistoryExtractor(BPCSExtractor):
    TEMPLATE_FILE = "DEMAND_HISTORY_TEMPLATE.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = DemandHistoryParams
    DEFAULT_DATE_COLUMNS = ["LODTE", "LRDTE", "LSDTE"]

    def __init__(self, meta: BPCSExtractorMeta, params: DemandHistoryParams):  # ✅ 显式写出来
        super().__init__(meta, params)