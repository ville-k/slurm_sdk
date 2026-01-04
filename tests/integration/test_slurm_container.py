from __future__ import annotations

import pytest

from slurm.examples.integration_test_task import simple_integration_task


@pytest.mark.integration_test
def test_slurm_services_running(slurm_testinfra):
    result = slurm_testinfra.run("sinfo")
    assert result.rc == 0, result.stderr
    assert "debug" in result.stdout

    procs = slurm_testinfra.check_output("pgrep -f slurmctld")
    assert procs.strip(), "slurmctld must be running"


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_submit_job_over_ssh(slurm_cluster):
    import logging

    logging.basicConfig(level=logging.DEBUG)

    job = slurm_cluster.submit(simple_integration_task)()
    result = job.wait(timeout=180, poll_interval=5)
    print(result)

    assert job.is_successful()
    assert job.get_result() == "integration-ok"
