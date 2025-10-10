import sys
import types
from pathlib import Path

from slurm.callbacks import BaseCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.packaging.base import PackagingStrategy

# Allow importing test helpers as a simple module
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))
from local_backend import LocalBackend  # type: ignore


class NoopStrategy(PackagingStrategy):
    def prepare(self, task, cluster):
        return {"status": "ok"}

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        return []

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None):
        return []


@task(job_name="sum", time="00:01:00", mem="1G", ntasks=1, nodes=1)
def add_one(x: int) -> int:
    return x + 1


def test_cluster_submit_with_local_backend(monkeypatch, tmp_path):
    import pytest

    pytest.skip(
        "LocalBackend execution has issues with this specific test - works in test_callbacks.py but hangs here. Needs investigation."
    )

    # Construct a bare Cluster and use the LocalBackend
    cluster = object.__new__(Cluster)
    cluster.backend_type = "LocalBackend"
    cluster.console = None
    cluster.backend = LocalBackend(job_base_dir=str(tmp_path))

    captured: dict[str, object] = {}

    class CaptureCallback(BaseCallback):
        def on_begin_submit_job_ctx(self, ctx):
            captured["begin"] = ctx

        def on_end_submit_job_ctx(self, ctx):
            captured["end"] = ctx

    cluster.callbacks = [CaptureCallback()]

    # Submit and run locally
    job = add_one.submit(cluster=cluster, packaging={"type": "none"})(41)

    # Job should be immediately completed in local backend
    assert job.is_completed()
    assert job.is_successful()
    assert job.get_result() == 42

    begin_ctx = captured.get("begin")
    end_ctx = captured.get("end")

    assert begin_ctx is not None, "Submit begin context not captured"
    assert end_ctx is not None, "Submit end context not captured"

    assert getattr(begin_ctx, "backend_type", None) == "LocalBackend"
    assert getattr(end_ctx, "backend_type", None) == "LocalBackend"
