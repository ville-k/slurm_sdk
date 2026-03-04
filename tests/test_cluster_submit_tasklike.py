"""Tests for Cluster.submit() accepting protocol-compatible task-like wrappers."""

from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.decorators import task
from local_backend import LocalBackend  # type: ignore


def create_mock_cluster(tmp_path: Path) -> Cluster:
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)
    cluster.backend = LocalBackend(job_base_dir=str(tmp_path))
    cluster.backend_type = "LocalBackend"
    cluster.packaging_defaults = {"type": "none"}
    cluster.callbacks = []
    cluster.console = None
    cluster.default_packaging = None
    cluster.default_packaging_kwargs = {}
    cluster.default_account = None
    cluster.default_partition = None
    cluster._job_pollers = {}
    return cluster


@task(time="00:01:00", mem="1G")
def double(x: int) -> int:
    return x * 2


class WrappedTask:
    """Protocol-compatible wrapper around an existing task handle."""

    def __init__(self, inner):
        self.inner = inner
        self.func = inner.func
        self.sbatch_options = inner.sbatch_options.copy()
        self.packaging = inner.packaging.copy() if inner.packaging else None
        self.slurm_options = dict(getattr(inner, "slurm_options", {}))
        self._pending_dependencies = []
        self._container_dependencies = []


def test_submit_accepts_tasklike_wrapper(tmp_path: Path):
    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    wrapper = WrappedTask(double)

    token = _set_active_context(cluster)
    try:
        submitter = cluster.submit(wrapper)
        job = submitter(3)
        assert job.id
    finally:
        _reset_active_context(token)
