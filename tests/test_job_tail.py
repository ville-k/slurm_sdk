"""Unit tests for Job.tail() method."""

import io
import time
from unittest.mock import MagicMock

import pytest

from slurm.job import Job


class TailBackend:
    """Fake backend that simulates tail_file."""

    hostname = "test"

    def __init__(self, lines_to_emit=None):
        self._lines = lines_to_emit or ["line1", "line2", "line3"]

    def tail_file(
        self,
        path,
        *,
        follow=True,
        lines=10,
        on_line=None,
        stop_event=None,
        poll_interval=1.0,
    ):
        for line in self._lines:
            if stop_event and stop_event.is_set():
                break
            on_line(line)
            if follow:
                time.sleep(0.05)
        if follow and stop_event:
            while not stop_event.is_set():
                time.sleep(0.05)

    def get_job_status(self, job_id):
        return {"JobState": "COMPLETED", "ExitCode": "0:0"}

    def is_remote(self):
        return False


def _make_job(backend=None, stdout_path="/tmp/test.out", stderr_path="/tmp/test.err"):
    backend = backend or TailBackend()
    return Job(
        id="12345",
        backend=backend,
        on_completed=lambda *a, **kw: None,
        target_job_dir="/tmp/test",
        pre_submission_id="abc",
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )


def test_tail_writes_to_stringio():
    """tail() with follow=False writes lines to a StringIO buffer."""
    buf = io.StringIO()
    job = _make_job()
    job.tail(follow=False, output=buf)
    assert "line1" in buf.getvalue()
    assert "line2" in buf.getvalue()


def test_tail_writes_to_file(tmp_path):
    """tail() with follow=False writes lines to a file."""
    log_file = tmp_path / "job.log"
    job = _make_job()
    with open(str(log_file), "w") as f:
        job.tail(follow=False, output=f)
    content = log_file.read_text()
    assert "line1" in content


def test_tail_stderr_uses_stderr_path():
    """tail(stderr=True) passes the stderr path to the backend."""
    backend = TailBackend()
    backend.tail_file = MagicMock()
    job = _make_job(backend=backend, stdout_path="/out.log", stderr_path="/err.log")

    job.tail(follow=False, stderr=True)
    backend.tail_file.assert_called_once()
    call_args = backend.tail_file.call_args
    assert call_args[0][0] == "/err.log"


def test_tail_no_follow_returns_immediately():
    """tail(follow=False) returns without blocking."""
    backend = TailBackend()
    job = _make_job(backend=backend)
    start = time.time()
    job.tail(follow=False)
    elapsed = time.time() - start
    assert elapsed < 2.0


def test_tail_raises_if_no_path():
    """tail() raises RuntimeError when output path is None."""
    job = _make_job(stdout_path=None)
    with pytest.raises(RuntimeError, match="not available"):
        job.tail()


def test_tail_follow_stops_when_job_completes():
    """tail(follow=True) stops streaming when job reaches terminal state."""
    backend = TailBackend()
    job = _make_job(backend=backend)

    buf = io.StringIO()
    start = time.time()
    job.tail(follow=True, output=buf, poll_interval=0.1)
    elapsed = time.time() - start

    # Should return in reasonable time since get_job_status returns COMPLETED
    assert elapsed < 5.0
    assert "line1" in buf.getvalue()


def test_tail_stderr_raises_if_no_stderr_path():
    """tail(stderr=True) raises RuntimeError when stderr_path is None."""
    job = _make_job(stderr_path=None)
    with pytest.raises(RuntimeError, match="not available"):
        job.tail(stderr=True)


def test_tail_defaults_to_stdout():
    """tail() uses stdout path by default (not stderr)."""
    backend = TailBackend()
    backend.tail_file = MagicMock()
    job = _make_job(backend=backend, stdout_path="/out.log", stderr_path="/err.log")

    job.tail(follow=False)
    call_args = backend.tail_file.call_args
    assert call_args[0][0] == "/out.log"
