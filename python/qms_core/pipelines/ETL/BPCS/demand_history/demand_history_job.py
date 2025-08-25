from qms_core.pipelines.ETL.common import GenericExtractorJob
from qms_core.adaptors.ETL.BPCS.demand_history.demand_history_extractor import DemandHistoryExtractor
from qms_core.adaptors.ETL.BPCS.demand_history.demand_history_transformer import DemandHistoryTransformer
from qms_core.infrastructure.db.models.demand_history import DemandHistory,DemandHistoryRaw

class DemandHistoryJob(GenericExtractorJob):
    EXTRACTOR_CLASS = DemandHistoryExtractor
    TARGET_TABLE = DemandHistory
    TRANSFORMER_CLASS = DemandHistoryTransformer

class DemandHistoryRawJob(GenericExtractorJob):
    EXTRACTOR_CLASS = DemandHistoryExtractor
    TARGET_TABLE = DemandHistoryRaw
    TRANSFORMER_CLASS = DemandHistoryTransformer

    DEFAULT_EXTRACT_PARAMS = {
        "exclude_deleted_orders": False,
    }
