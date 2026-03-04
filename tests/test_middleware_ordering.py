"""Tests for deterministic middleware ordering."""

from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _reset_active_runtime,
    _set_active_context,
    _set_active_runtime,
)
from slurm.core import ClusterRuntime, task_decorator
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


class OrderingMiddleware:
    def __init__(self, name: str, sink: list[str]) -> None:
        self._name = name
        self._sink = sink

    def before_invoke(self, ctx) -> None:
        self._sink.append(f"{self._name}:before_invoke")

    def transform_call(self, ctx) -> None:
        self._sink.append(f"{self._name}:transform_call")

    def before_submit(self, ctx) -> None:
        self._sink.append(f"{self._name}:before_submit")

    def after_submit(self, ctx, ref):
        self._sink.append(f"{self._name}:after_submit")
        return ref

    def on_result(self, ctx) -> None:
        self._sink.append(f"{self._name}:on_result")


def test_middleware_runs_in_stack_order(tmp_path: Path):
    events: list[str] = []
    first = OrderingMiddleware("first", events)
    second = OrderingMiddleware("second", events)
    decorate = task_decorator(middleware=(first, second))

    @decorate
    def sample(value: int) -> int:
        return value + 1

    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    ctx_token = _set_active_context(cluster)
    runtime_token = _set_active_runtime(ClusterRuntime())
    try:
        sample(5)
    finally:
        _reset_active_runtime(runtime_token)
        _reset_active_context(ctx_token)

    assert events == [
        "first:before_invoke",
        "second:before_invoke",
        "first:transform_call",
        "second:transform_call",
        "first:before_submit",
        "second:before_submit",
        "first:after_submit",
        "second:after_submit",
        "first:on_result",
        "second:on_result",
    ]
