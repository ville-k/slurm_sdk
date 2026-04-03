"""Unit tests for BackendBase.tail_file() implementations."""

import threading
import time

import pytest

from slurm.api.base import BackendBase
from slurm.api.local import LocalBackend


def test_tail_file_local_no_follow(tmp_path):
    """Tail last N lines without following."""
    f = tmp_path / "output.log"
    f.write_text("line1\nline2\nline3\nline4\nline5\n")

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    backend.tail_file(str(f), follow=False, lines=3, on_line=collected.append)
    assert collected == ["line3", "line4", "line5"]


def test_tail_file_local_follow_stops_on_event(tmp_path):
    """Follow mode stops when stop_event is set."""
    f = tmp_path / "output.log"
    f.write_text("initial\n")

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    stop = threading.Event()

    def run_tail():
        backend.tail_file(
            str(f),
            follow=True,
            lines=10,
            on_line=collected.append,
            stop_event=stop,
            poll_interval=0.1,
        )

    t = threading.Thread(target=run_tail)
    t.start()

    time.sleep(0.3)
    with open(str(f), "a") as fh:
        fh.write("appended\n")
    time.sleep(0.3)
    stop.set()
    t.join(timeout=5)

    assert "initial" in collected
    assert "appended" in collected


def test_tail_file_local_waits_for_file(tmp_path):
    """Tail waits for file to appear."""
    f = tmp_path / "future.log"

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    stop = threading.Event()

    def run_tail():
        backend.tail_file(
            str(f),
            follow=False,
            lines=10,
            on_line=collected.append,
            stop_event=stop,
            poll_interval=0.1,
        )

    t = threading.Thread(target=run_tail)
    t.start()

    time.sleep(0.3)
    f.write_text("appeared\n")
    t.join(timeout=5)

    assert "appeared" in collected


def test_tail_file_local_no_follow_all_lines(tmp_path):
    """No-follow with fewer lines than requested returns all available."""
    f = tmp_path / "short.log"
    f.write_text("only\n")

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    backend.tail_file(str(f), follow=False, lines=100, on_line=collected.append)
    assert collected == ["only"]


def test_tail_file_local_empty_file(tmp_path):
    """Tailing an empty file in no-follow mode returns nothing."""
    f = tmp_path / "empty.log"
    f.write_text("")

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    backend.tail_file(str(f), follow=False, lines=10, on_line=collected.append)
    assert collected == []


def test_tail_file_base_raises():
    """Base class raises NotImplementedError."""

    class MinimalBackend(BackendBase):
        hostname = "test"

        def submit_job(self, *a, **kw):
            pass

        def get_job_status(self, *a, **kw):
            pass

        def get_job_accounting(self, *a, **kw):
            pass

        def get_account_jobs(self, *a, **kw):
            pass

        def cancel_job(self, *a, **kw):
            pass

        def get_queue(self, *a, **kw):
            pass

        def get_cluster_info(self, *a, **kw):
            pass

        def is_remote(self):
            return False

    with pytest.raises(NotImplementedError):
        MinimalBackend().tail_file("path", on_line=lambda line: None)


def test_tail_file_local_stop_event_during_wait(tmp_path):
    """Stop event set while waiting for file to appear causes early exit."""
    f = tmp_path / "never_appears.log"

    backend = LocalBackend(job_base_dir=str(tmp_path))
    collected = []
    stop = threading.Event()

    def run_tail():
        backend.tail_file(
            str(f),
            follow=False,
            lines=10,
            on_line=collected.append,
            stop_event=stop,
            poll_interval=0.1,
        )

    t = threading.Thread(target=run_tail)
    t.start()

    time.sleep(0.3)
    stop.set()
    t.join(timeout=5)

    assert collected == []
