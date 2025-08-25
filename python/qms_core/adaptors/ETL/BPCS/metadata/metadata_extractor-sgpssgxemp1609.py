from qms_core.adaptors.ETL.BPCS.common.bpcs_extractor import BPCSExtractor,BPCSExtractorMeta
from qms_core.adaptors.ETL.BPCS.metadata import SQL_templates
from qms_core.adaptors.ETL.BPCS.metadata.metadata_schema import IIMParams,IWIParams,ILMParams,DPSParams,AVMParams,GCCParams

class IIMExtractor(BPCSExtractor):
    TEMPLATE_FILE = "IIM.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = IIMParams

    def __init__(self, meta: BPCSExtractorMeta, params: IIMParams):  # ✅ 显式写出来
            super().__init__(meta, params)

class IWIExtractor(BPCSExtractor):
    TEMPLATE_FILE = "IWI.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = IWIParams

    def __init__(self, meta: BPCSExtractorMeta, params: IWIParams):  # ✅ 显式写出来
        super().__init__(meta, params)

class ILMExtractor(BPCSExtractor):
    TEMPLATE_FILE = "ILM.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = ILMParams

    def __init__(self, meta: BPCSExtractorMeta, params: ILMParams):  # ✅ 显式写出来
            super().__init__(meta, params)

class DPSExtractor(BPCSExtractor):
    TEMPLATE_FILE = "DPS.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = DPSParams

    def __init__(self, meta: BPCSExtractorMeta, params: DPSParams):  # ✅ 显式写出来
            super().__init__(meta, params)

class AVMExtractor(BPCSExtractor):
    TEMPLATE_FILE = "AVM.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = AVMParams

    def __init__(self, meta: BPCSExtractorMeta, params: AVMParams):  # ✅ 显式写出来
            super().__init__(meta, params)

class GCCExtractor(BPCSExtractor):
    TEMPLATE_FILE = "GCC.sql"
    TEMPLATE_NAMESPACE = SQL_templates
    PARAMS_CLASS = GCCParams

    def __init__(self, meta: BPCSExtractorMeta, params: GCCParams):
            super().__init__(meta, params)