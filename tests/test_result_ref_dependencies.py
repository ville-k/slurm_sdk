"""Tests for using ResultRef as dependency input."""

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


@ha_task(time="00:01:00")
def produce(n: int) -> int:
    return n + 1


@ha_task(time="00:01:00")
def consume(value: int) -> int:
    return value * 2


def test_result_ref_can_drive_dependency_submission(tmp_path: Path):
    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        with ha_runtime():
            produced = produce(4)
            assert isinstance(produced, ResultRef)
            consumed = consume(produced)
            assert isinstance(consumed, ResultRef)
            assert consumed.get_result() == 10
    finally:
        _reset_active_context(token)
