from qms_core.pipelines.ETL.common import GenericExtractorJob
from qms_core.adaptors.ETL.BPCS.metadata.metadata_extractor import ILMExtractor,IIMExtractor,IWIExtractor,AVMExtractor,DPSExtractor,GCCExtractor
from qms_core.adaptors.ETL.BPCS.metadata.metadata_transformer import ILMTransformer,IWITransformer,AVMTransformer,GCCTransformer
from qms_core.infrastructure.db.models import ILM,VendorMaster,IIM,IWI,DPS,MultiCurrency

class ILMJob(GenericExtractorJob):
    EXTRACTOR_CLASS = ILMExtractor
    TRANSFORMER_CLASS = ILMTransformer
    TARGET_TABLE = ILM

class IIMJob(GenericExtractorJob):
    EXTRACTOR_CLASS = IIMExtractor
    TARGET_TABLE = IIM


class IWIJob(GenericExtractorJob):
    EXTRACTOR_CLASS = IWIExtractor
    TRANSFORMER_CLASS = IWITransformer
    TARGET_TABLE = IWI

    
class DPSJob(GenericExtractorJob):
    EXTRACTOR_CLASS = DPSExtractor
    TARGET_TABLE = DPS


class AVMJob(GenericExtractorJob):
    EXTRACTOR_CLASS = AVMExtractor
    TRANSFORMER_CLASS = AVMTransformer
    TARGET_TABLE = VendorMaster

class GCCJob(GenericExtractorJob):
    EXTRACTOR_CLASS = GCCExtractor
    TRANSFORMER_CLASS = GCCTransformer
    TARGET_TABLE = MultiCurrency