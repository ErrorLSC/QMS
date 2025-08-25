from qms_core.pipelines.ETL.common import GenericExtractorJob
from qms_core.adaptors.ETL.BPCS.inventory.inventory_extractor import ILIExtractor,IWIAvailExtractor
from qms_core.adaptors.ETL.BPCS.inventory.inventory_transformer import ILITransformer,IWIAvailTransformer
from qms_core.infrastructure.db.models import STKOH,STKOHAvail

class ILIJob(GenericExtractorJob):
    EXTRACTOR_CLASS = ILIExtractor
    TRANSFORMER_CLASS = ILITransformer
    TARGET_TABLE = STKOH

class IWIAvailJob(GenericExtractorJob):
    EXTRACTOR_CLASS = IWIAvailExtractor
    TRANSFORMER_CLASS = IWIAvailTransformer
    TARGET_TABLE = STKOHAvail

