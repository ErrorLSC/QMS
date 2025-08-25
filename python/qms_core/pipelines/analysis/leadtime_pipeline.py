from qms_core.pipelines.analysis.jobs.transport_leadtime_job import TransportLeadtimeJob
from qms_core.pipelines.analysis.jobs.prepare_leadtime_job import PrepareLeadtimeJob
from qms_core.pipelines.analysis.jobs.transport_preference_job import TransportPreferenceJob
from qms_core.pipelines.analysis.jobs.delivery_behavior_job import DeliveryBehaviorJob
from qms_core.pipelines.analysis.jobs.delivery_batch_profile_job import DeliveryBatchProfileJob
from qms_core.pipelines.analysis.jobs.smart_leadtime_job import SmartLeadtimeJob
from qms_core.pipelines.analysis.jobs.freight_charge_job import FreightChargeJob

from qms_core.pipelines.common.base_pipeline import BasePipeline


class LeadtimeAnalysisPipeline(BasePipeline):
    """
    ⛓️ 智能交期分析 Pipeline：
    - 分批调度或全量调度各个分析 Job
    - 每个 Job 都是 AnalysisJob 的实例
    """

    JOB_CLASSES = {
        "transport_leadtime": TransportLeadtimeJob,
        "prepare_leadtime": PrepareLeadtimeJob,
        "transport_preference": TransportPreferenceJob,
        "delivery_behavior": DeliveryBehaviorJob,
        "batch_profile": DeliveryBatchProfileJob,
        "smart_leadtime": SmartLeadtimeJob,
        "freight_charge": FreightChargeJob
    }

    JOB_INIT_ARGS = {
        job_name: ["config"]
        for job_name in JOB_CLASSES
    }

    DEFAULT_RUN_ORDER = [
        "transport_leadtime",
        "prepare_leadtime",
        "transport_preference",
        "delivery_behavior",
        "batch_profile",
        "smart_leadtime",
        "freight_charge",
    ]