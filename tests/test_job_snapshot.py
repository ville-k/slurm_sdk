"""Tests for Job.snapshot() and JobSnapshot dataclass."""

import dataclasses
import time

import pytest

from slurm.job import Job, JobSnapshot


class FakeBackend:
    """Minimal backend for snapshot tests."""

    hostname = "test"

    def __init__(self, status=None):
        self._status = status or {"JobState": "COMPLETED", "ExitCode": "0:0"}

    def get_job_status(self, job_id):
        return dict(self._status)

    def is_remote(self):
        return False

    def tail_file(self, *args, **kwargs):
        raise NotImplementedError


def _make_job(
    status=None,
    stdout_path=None,
    stderr_path=None,
    started_at=None,
    finished_at=None,
):
    backend = FakeBackend(status=status)
    job = Job(
        id="12345",
        backend=backend,
        on_completed=lambda *a, **kw: None,
        target_job_dir="/tmp/test",
        pre_submission_id="abc",
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    if started_at is not None:
        job.started_at = started_at
    if finished_at is not None:
        job.finished_at = finished_at
    return job


def test_snapshot_completed_job(tmp_path):
    stdout = tmp_path / "out.log"
    stderr = tmp_path / "err.log"
    stdout.write_text("output line 1\noutput line 2\n")
    stderr.write_text("warning: something\n")

    job = _make_job(
        status={"JobState": "COMPLETED", "ExitCode": "0:0"},
        stdout_path=str(stdout),
        stderr_path=str(stderr),
        started_at=100.0,
        finished_at=200.0,
    )
    snap = job.snapshot()

    assert snap.job_id == "12345"
    assert snap.state == "COMPLETED"
    assert snap.exit_code == "0:0"
    assert snap.is_terminal is True
    assert snap.is_successful is True
    assert "output line 1" in snap.stdout_tail
    assert "warning: something" in snap.stderr_tail
    assert snap.elapsed_seconds == pytest.approx(100.0)


def test_snapshot_running_job():
    now = time.time()
    job = _make_job(
        status={"JobState": "RUNNING"},
        started_at=now - 60,
    )
    snap = job.snapshot()

    assert snap.state == "RUNNING"
    assert snap.is_terminal is False
    assert snap.is_successful is False
    assert snap.elapsed_seconds is not None
    assert snap.elapsed_seconds >= 59.0


def test_snapshot_pending_job():
    job = _make_job(status={"JobState": "PENDING"})
    snap = job.snapshot()

    assert snap.state == "PENDING"
    assert snap.is_terminal is False
    assert snap.is_successful is False
    assert snap.elapsed_seconds is None


def test_snapshot_failed_job(tmp_path):
    stderr = tmp_path / "err.log"
    stderr.write_text("Traceback:\n  File ...\nRuntimeError: boom\n")

    job = _make_job(
        status={"JobState": "FAILED", "ExitCode": "1:0", "Reason": "NonZeroExitCode"},
        stderr_path=str(stderr),
        started_at=100.0,
        finished_at=150.0,
    )
    snap = job.snapshot()

    assert snap.state == "FAILED"
    assert snap.is_terminal is True
    assert snap.is_successful is False
    assert snap.exit_code == "1:0"
    assert snap.reason == "NonZeroExitCode"
    assert "RuntimeError: boom" in snap.stderr_tail


def test_snapshot_missing_paths():
    job = _make_job(stdout_path=None, stderr_path=None)
    snap = job.snapshot()

    assert snap.stdout_tail == ""
    assert snap.stderr_tail == ""


def test_snapshot_nonexistent_files():
    job = _make_job(
        stdout_path="/nonexistent/out.log",
        stderr_path="/nonexistent/err.log",
    )
    snap = job.snapshot()

    assert snap.stdout_tail == ""
    assert snap.stderr_tail == ""


def test_snapshot_tail_lines(tmp_path):
    stdout = tmp_path / "out.log"
    stdout.write_text("\n".join(f"line {i}" for i in range(200)) + "\n")

    job = _make_job(stdout_path=str(stdout))
    snap = job.snapshot(tail_lines=10)

    lines = snap.stdout_tail.splitlines()
    assert len(lines) == 10
    assert lines[-1] == "line 199"


def test_snapshot_frozen():
    job = _make_job()
    snap = job.snapshot()

    with pytest.raises(dataclasses.FrozenInstanceError):
        snap.state = "MODIFIED"  # type: ignore[misc]


def test_snapshot_is_jobsnapshot_type():
    job = _make_job()
    snap = job.snapshot()
    assert isinstance(snap, JobSnapshot)
