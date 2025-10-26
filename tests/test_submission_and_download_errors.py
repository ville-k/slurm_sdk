import os
import pickle
import pytest

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.errors import SubmissionError, DownloadError


@task(job_name="boom")
def echo(x):
    return x


class FailingBackend:
    def submit_job(self, *args, **kwargs):
        raise RuntimeError("sbatch failed")

    def get_queue(self):
        return []

    def get_cluster_info(self):
        return {}


def test_submission_error_bubbled():
    c = object.__new__(Cluster)
    c.backend_type = "FailingBackend"
    c.backend = FailingBackend()
    c.callbacks = []
    # Add new string-based API attributes
    c.default_packaging = None
    c.default_account = None
    c.default_partition = None
    with pytest.raises(SubmissionError):
        c.submit(echo, packaging="none")(1)


class FakeSSHBackend:
    def __init__(self, remote_path_exists: bool):
        self._exists = remote_path_exists

    def get_job_status(self, job_id: str):
        return {"JobState": "COMPLETED", "ExitCode": "0:0"}

    def download_file(self, remote_path: str, local_path: str):
        if not self._exists:
            raise FileNotFoundError("missing")
        with open(local_path, "wb") as f:
            pickle.dump(123, f)


class DummyCluster:
    def __init__(self, backend):
        self.backend = backend


def test_download_error_path(tmp_path, monkeypatch):
    from slurm import job as job_mod

    orig_fn = job_mod.Job.get_result

    def wrapped(self):
        try:
            return orig_fn(self)
        except FileNotFoundError as e:
            from slurm.errors import DownloadError

            raise DownloadError(str(e)) from e

    monkeypatch.setattr(job_mod.Job, "get_result", wrapped, raising=False)

    from slurm.job import Job

    backend = FakeSSHBackend(remote_path_exists=False)
    cluster = DummyCluster(backend)
    j = Job(
        id="5", cluster=cluster, target_job_dir=str(tmp_path), pre_submission_id="abc"
    )
    with pytest.raises(DownloadError):
        j.get_result()
