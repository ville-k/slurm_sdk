import os
import sys
from textwrap import dedent
from pathlib import Path

from types import SimpleNamespace

from slurm.callbacks import (
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
    LoggerCallback,
    RichLoggerCallback,
    PackagingBeginContext,
    PackagingEndContext,
    SubmitBeginContext,
    SubmitEndContext,
)
from slurm.cluster import Cluster
from slurm.decorators import task

# Allow importing test helpers as a simple module
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))
from local_backend import LocalBackend  # type: ignore


@task(job_name="cb-test", time="00:01:00", mem="1G", ntasks=1, nodes=1)
def echo_val(x: int) -> int:
    return x


def test_callbacks_invoked_local_backend(tmp_path):
    # Create a temporary callback module that writes markers
    cb_code = dedent(
        """
        import os
        from slurm.callbacks.callbacks import BaseCallback

        class TestCallback(BaseCallback):
            def __init__(self, log_dir: str):
                self.log_dir = log_dir

            def _mark(self, name: str):
                p = os.path.join(self.log_dir, name)
                with open(p, "w") as f:
                    f.write("1")

            def on_begin_package_ctx(self, ctx):
                self._mark("begin_package")

            def on_end_package_ctx(self, ctx):
                self._mark("end_package")

            def on_begin_submit_job_ctx(self, ctx):
                self._mark("begin_submit")

            def on_end_submit_job_ctx(self, ctx):
                self._mark("end_submit")

            def on_begin_run_job_ctx(self, ctx):
                self._mark("begin_run")

            def on_end_run_job_ctx(self, ctx):
                self._mark("end_run")
        """
    )
    mod_path = tmp_path / "cbmod.py"
    mod_path.write_text(cb_code)

    # Ensure runner can import cbmod on unpickle
    sys.path.insert(0, str(tmp_path))
    import importlib

    cbmod = importlib.import_module("cbmod")
    callback = cbmod.TestCallback(str(tmp_path))

    # Create cluster and swap in local backend
    cluster = object.__new__(Cluster)
    cluster.backend_type = "LocalBackend"
    cluster.console = None
    cluster.callbacks = [callback]
    cluster.backend = LocalBackend(job_base_dir=str(tmp_path))
    # Add new string-based API attributes
    cluster.default_packaging = None
    cluster.default_account = None
    cluster.default_partition = None

    job = cluster.submit(echo_val, packaging="none")(7)
    job.wait()

    # Verify packaging and submit callbacks (executed in submission process)
    for marker in ["begin_package", "end_package", "begin_submit", "end_submit"]:
        assert (tmp_path / marker).exists(), f"Missing marker {marker}"

    # Verify run callbacks (executed in runner process)
    for marker in ["begin_run", "end_run"]:
        assert (tmp_path / marker).exists(), f"Missing marker {marker}"


def test_logger_callback_tracks_submission_phases():
    """Test that LoggerCallback logs events without rich dependencies."""
    callback = LoggerCallback()

    # LoggerCallback should work without rich - just log events
    # We mainly check that it doesn't crash
    packaging_begin = PackagingBeginContext(
        task=echo_val,
        packaging_config={"type": "wheel"},
    )
    callback.on_begin_package_ctx(packaging_begin)  # Should not crash

    packaging_end = PackagingEndContext(
        task=echo_val,
        packaging_result=object(),
        duration=1.5,
    )
    callback.on_end_package_ctx(packaging_end)  # Should not crash


def test_rich_logger_callback_tracks_submission_phases():
    """Test that RichLoggerCallback tracks phase state with progress bars."""
    try:
        callback = RichLoggerCallback()

        packaging_begin = PackagingBeginContext(
            task=echo_val,
            packaging_config={"type": "wheel"},
        )
        callback.on_begin_package_ctx(packaging_begin)
        assert callback._phase == "PACKAGING"
        assert callback._job_label == "cb-test"

        packaging_end = PackagingEndContext(
            task=echo_val,
            packaging_result=object(),
            duration=1.5,
        )
        callback.on_end_package_ctx(packaging_end)
        assert callback._phase == "PACKAGED"

        submit_begin = SubmitBeginContext(
            task=echo_val,
            sbatch_options={"partition": "debug"},
            pre_submission_id="abc123",
            target_job_dir="/tmp/job",
            backend_type="LocalBackend",
        )
        callback.on_begin_submit_job_ctx(submit_begin)
        assert callback._phase == "SUBMITTING"
        assert callback._job_label == "Job abc123"

        job_stub = SimpleNamespace(stdout_path=None, stderr_path=None)
        submit_end = SubmitEndContext(
            job=job_stub,
            job_id="456",
            pre_submission_id="abc123",
            target_job_dir="/tmp/job",
            sbatch_options={"partition": "debug"},
            backend_type="LocalBackend",
        )
        callback.on_end_submit_job_ctx(submit_end)
        assert callback._phase == "SUBMITTED"
        assert callback._job_label == "Job 456"

        status_ctx = JobStatusUpdatedContext(
            job=job_stub,
            job_id="456",
            status={"JobState": "RUNNING"},
            timestamp=123.0,
        )
        callback.on_job_status_update_ctx(status_ctx)
        assert callback._phase == "RUNNING"

        completed_ctx = CompletedContext(
            job=job_stub,
            job_id="456",
            job_dir="/tmp/job",
            job_state="COMPLETED",
            exit_code="0",
            reason=None,
            stdout_path=None,
            stderr_path=None,
            start_time=None,
            end_time=None,
            duration=None,
            status={"JobState": "COMPLETED"},
            emitted_by=ExecutionLocus.CLIENT,
        )
        callback.on_completed_ctx(completed_ctx)

        assert callback._progress is None
        assert callback._job_label is None
        assert callback._phase is None
    except ImportError:
        # If rich is not available, skip this test
        import pytest

        pytest.skip("rich not available")
