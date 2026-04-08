import pytest

from slurm.errors import BackendCommandError, BackendError
from slurm.job import Job


class DummyBackend:
    def __init__(self, status):
        self._status = status

    def get_job_status(self, job_id: str):
        return self._status

    def get_job_accounting(self, job_id: str):
        raise NotImplementedError


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


class AccountingFallbackBackend:
    """Backend where get_job_status fails but get_job_accounting succeeds."""

    def __init__(self, accounting_data):
        self._accounting = accounting_data

    def get_job_status(self, job_id: str):
        raise BackendCommandError("Job not found in SLURM queue")

    def get_job_accounting(self, job_id: str):
        return self._accounting


class BothFailBackend:
    """Backend where both status and accounting fail."""

    def get_job_status(self, job_id: str):
        raise BackendCommandError("Job not found in SLURM queue")

    def get_job_accounting(self, job_id: str):
        raise BackendCommandError("No accounting data for job")


def test_status_falls_back_to_accounting():
    """When job leaves scheduler queue, get_status() falls back to sacct."""
    acct_data = {"JobID": "123", "JobState": "COMPLETED", "ExitCode": "0:0"}
    backend = AccountingFallbackBackend(acct_data)
    cluster = DummyCluster(backend)
    job = Job(
        id="123", cluster=cluster, target_job_dir="/tmp/x", pre_submission_id="abc"
    )

    status = job.get_status()
    assert status["JobState"] == "COMPLETED"
    assert status["ExitCode"] == "0:0"


def test_status_fallback_reports_terminal():
    """Accounting fallback correctly marks completed jobs as terminal."""
    acct_data = {"JobID": "456", "JobState": "FAILED", "ExitCode": "1:0"}
    backend = AccountingFallbackBackend(acct_data)
    cluster = DummyCluster(backend)
    job = Job(
        id="456", cluster=cluster, target_job_dir="/tmp/x", pre_submission_id="abc"
    )

    assert job.is_completed() is True
    assert job.is_successful() is False


def test_status_raises_when_both_fail():
    """When both scheduler and accounting fail, raise BackendError."""
    backend = BothFailBackend()
    cluster = DummyCluster(backend)
    job = Job(
        id="789", cluster=cluster, target_job_dir="/tmp/x", pre_submission_id="abc"
    )

    with pytest.raises(
        BackendError, match="not found in scheduler queue or accounting"
    ):
        job.get_status()
