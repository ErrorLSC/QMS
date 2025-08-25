from qms_core.pipelines.analysis.jobs.freight_charge_job import FreightChargeJob
from qms_core.infrastructure.config import MRPConfig

def test_run(config:MRPConfig,dry_run=True):
    job = FreightChargeJob(config)
    result = job.run(dry_run=dry_run)
    return result

if __name__ == "__main__":
    config = MRPConfig()
    result = test_run(config=config,dry_run=False)
    print(result)
