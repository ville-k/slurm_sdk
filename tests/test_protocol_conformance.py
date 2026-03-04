"""Protocol conformance checks for generalized APIs."""

from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.core import DependencyRef, InvocationRef, TaskLike, WorkflowLike
from slurm.decorators import task, workflow
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


@task(time="00:01:00")
def produce(value: int) -> int:
    return value + 1


@workflow(time="00:01:00")
def orchestrate(value: int, ctx) -> int:
    return produce(value)


def test_task_and_workflow_protocol_conformance():
    assert isinstance(produce, TaskLike)
    assert isinstance(orchestrate, WorkflowLike)


def test_job_conforms_to_reference_protocols(tmp_path: Path):
    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        job = produce(1)
        assert isinstance(job, InvocationRef)
        assert isinstance(job, DependencyRef)
        assert job.scheduler_dependencies() == [job.id]
    finally:
        _reset_active_context(token)
