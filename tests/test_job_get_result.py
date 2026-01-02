import pickle
import pytest
from slurm.job import Job
from slurm.rendering import RESULT_FILENAME
from slurm.errors import DownloadError


class FakeSSHBackend:
    def __init__(self, remote_dir: str):
        self.remote_dir = remote_dir

    def get_job_status(self, job_id: str):
        return {"JobState": "COMPLETED", "ExitCode": "0:0"}

    def download_file(self, remote_path: str, local_path: str):
        # Simulate remote file existing by copying from a known local path
        with open(remote_path, "rb") as src, open(local_path, "wb") as dst:
            dst.write(src.read())


class DummyCluster:
    def __init__(self, backend):
        self.backend = backend


def test_job_get_result_downloads_and_unpickles(tmp_path):
    pre_id = "abc123"
    # Prepare a fake "remote" result file in tmp dir
    remote_dir = tmp_path / "remote"
    remote_dir.mkdir()
    result_path = remote_dir / f"slurm_job_{pre_id}_{RESULT_FILENAME}"
    with open(result_path, "wb") as f:
        pickle.dump({"ok": True}, f)

    backend = FakeSSHBackend(str(remote_dir))
    cluster = DummyCluster(backend)
    job = Job(
        id="7",
        cluster=cluster,
        target_job_dir=str(remote_dir),
        pre_submission_id=pre_id,
    )

    value = job.get_result()
    assert value == {"ok": True}


def test_job_get_result_failure_after_wait_reports_failure(monkeypatch, tmp_path):
    class StatusBackend:
        def get_job_status(self, job_id: str):
            return {
                "JobState": "FAILED",
                "ExitCode": "1:0",
                "Reason": "NonZeroExitCode",
            }

    cluster = DummyCluster(StatusBackend())
    job = Job(
        id="9",
        cluster=cluster,
        target_job_dir=str(tmp_path),
        pre_submission_id="pre",
        stdout_path=str(tmp_path / "stdout.txt"),
        stderr_path=str(tmp_path / "stderr.txt"),
    )

    completed_calls = {"count": 0}

    def fake_is_completed():
        completed_calls["count"] += 1
        return completed_calls["count"] > 1

    monkeypatch.setattr(job, "is_completed", fake_is_completed)
    monkeypatch.setattr(job, "wait", lambda timeout=None: False)
    monkeypatch.setattr(job, "get_stdout", lambda: "child stdout\nline2\n")
    monkeypatch.setattr(job, "get_stderr", lambda: "child stderr\nboom\n")

    with pytest.raises(DownloadError) as exc:
        job.get_result(timeout=1)

    message = str(exc.value)
    assert "did not succeed" in message
    assert "within timeout" not in message
    assert "child stderr" in message
