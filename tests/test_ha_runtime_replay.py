"""Tests for replay-aware HA runtime behavior."""

from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.examples.high_availability_training.ha.fluent import (
    ResultRef,
    ha_runtime,
    ha_task,
)
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
    cluster.default_account = None
    cluster.default_partition = None
    return cluster


@ha_task(time="00:01:00", mem="1G")
def square(x: int) -> int:
    return x * x


def test_ha_runtime_reuses_result_ref_for_same_call(tmp_path: Path):
    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        with ha_runtime() as runtime:
            ref1 = square(3)
            ref2 = square(3)

            assert isinstance(ref1, ResultRef)
            assert ref1 is ref2
            assert len(runtime.__dict__.get("_cache", {})) >= 1
            assert ref1.get_result() == 9
    finally:
        _reset_active_context(token)
