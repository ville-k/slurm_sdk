from slurm.job import Job


class DummyBackend:
    def __init__(self, status):
        self._status = status

    def get_job_status(self, job_id: str):
        return self._status


class DummyCluster:
    def __init__(self, backend):
        self.backend = backend


def test_job_is_completed_and_successful():
    backend = DummyBackend({"JobState": "COMPLETED", "ExitCode": "0:0"})
    cluster = DummyCluster(backend)
    job = Job(id="1", cluster=cluster, target_job_dir="/tmp/x", pre_submission_id="abc")

    assert job.is_completed() is True
    assert job.is_successful() is True


def test_job_is_not_completed_running():
    backend = DummyBackend({"JobState": "RUNNING"})
    cluster = DummyCluster(backend)
    job = Job(id="2", cluster=cluster, target_job_dir="/tmp/x", pre_submission_id="abc")

    assert job.is_completed() is False
    assert job.is_successful() is False
