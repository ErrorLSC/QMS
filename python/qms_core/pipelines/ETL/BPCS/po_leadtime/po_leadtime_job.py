from qms_core.pipelines.ETL.common import GenericExtractorJob
from qms_core.infrastructure.db.models.po import PO_DeliveryHistoryRaw,PO_LAST_SNAPSHOT,PO_IntransitRaw,PO_Freight_Charge
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_extractor import DomesticPOExtractor,OverseaPOExtractor,OpenPOExtractor,POIntransitExtractor,FreightChargeExtractor
from qms_core.adaptors.ETL.BPCS.po_leadtime.po_leadtime_transfomer import DomesticPOTransformer,OverseaPOTransformer,OpenPOTransformer,POIntransitTransformer,FreightChargeTransformer

class DomesticPOLeadtimeJob(GenericExtractorJob):
    EXTRACTOR_CLASS = DomesticPOExtractor
    TRANSFORMER_CLASS = DomesticPOTransformer
    TARGET_TABLE = PO_DeliveryHistoryRaw
    
class OverseaPOLeadtimeJob(GenericExtractorJob):
    EXTRACTOR_CLASS = OverseaPOExtractor
    TRANSFORMER_CLASS = OverseaPOTransformer
    TARGET_TABLE = PO_DeliveryHistoryRaw
    
class OpenPOJob(GenericExtractorJob):
    EXTRACTOR_CLASS = OpenPOExtractor
    TRANSFORMER_CLASS = OpenPOTransformer
    TARGET_TABLE = PO_LAST_SNAPSHOT
    
class POIntransitJob(GenericExtractorJob):
    EXTRACTOR_CLASS = POIntransitExtractor
    TRANSFORMER_CLASS = POIntransitTransformer
    TARGET_TABLE = PO_IntransitRaw

class FreightChargeJob(GenericExtractorJob):
    EXTRACTOR_CLASS = FreightChargeExtractor
    TRANSFORMER_CLASS = FreightChargeTransformer
    TARGET_TABLE = PO_Freight_Charge