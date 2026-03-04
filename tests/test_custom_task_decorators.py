"""Tests for custom task decorator composition helpers."""

from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _reset_active_runtime,
    _set_active_context,
    _set_active_runtime,
)
from slurm.core import ClusterRuntime, TaskHandle, task_decorator
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


class RecordingMiddleware:
    def __init__(self) -> None:
        self.events: list[str] = []

    def before_invoke(self, ctx) -> None:
        self.events.append("before_invoke")

    def transform_call(self, ctx) -> None:
        self.events.append("transform_call")

    def before_submit(self, ctx) -> None:
        self.events.append("before_submit")

    def after_submit(self, ctx, ref):
        self.events.append("after_submit")
        return ref

    def on_result(self, ctx) -> None:
        self.events.append("on_result")


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


class DependencyRefStub:
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id

    def scheduler_dependencies(self) -> list[str]:
        return [self.job_id]


def test_custom_task_decorator_applies_defaults_and_middleware(tmp_path: Path):
    middleware = RecordingMiddleware()
    gpu_task = task_decorator(
        default_options={"partition": "gpu", "time": "00:03:00"},
        middleware=(middleware,),
    )

    @gpu_task
    def train(x: int) -> int:
        return x + 1

    assert isinstance(train, TaskHandle)
    assert train.sbatch_options["partition"] == "gpu"
    assert train.sbatch_options["time"] == "00:03:00"

    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    ctx_token = _set_active_context(cluster)
    runtime_token = _set_active_runtime(ClusterRuntime())
    try:
        job = train(1)
    finally:
        _reset_active_runtime(runtime_token)
        _reset_active_context(ctx_token)

    assert job is not None
    assert middleware.events == [
        "before_invoke",
        "transform_call",
        "before_submit",
        "after_submit",
        "on_result",
    ]


def test_task_decorator_can_wrap_existing_task_handle():
    mark_cpu = task_decorator(default_options={"partition": "cpu"})

    @mark_cpu
    def preprocess(x: int) -> int:
        return x

    assert isinstance(preprocess, TaskHandle)
    assert preprocess.sbatch_options["partition"] == "cpu"

    tuned = mark_cpu(preprocess.with_options(mem="2G"))
    assert isinstance(tuned, TaskHandle)
    assert tuned.sbatch_options["partition"] == "cpu"
    assert tuned.sbatch_options["mem"] == "2G"


def test_stacked_task_decorators_have_deterministic_order(tmp_path: Path):
    events: list[str] = []
    first = task_decorator(
        default_options={"partition": "cpu"},
        middleware=(OrderingMiddleware("first", events),),
    )
    second = task_decorator(
        default_options={"mem": "2G"},
        middleware=(OrderingMiddleware("second", events),),
    )

    @second
    @first
    def stacked(x: int) -> int:
        return x + 1

    assert stacked.sbatch_options["partition"] == "cpu"
    assert stacked.sbatch_options["mem"] == "2G"

    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    ctx_token = _set_active_context(cluster)
    runtime_token = _set_active_runtime(ClusterRuntime())
    try:
        stacked(1)
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


def test_after_and_map_keep_custom_task_handle_metadata(tmp_path: Path):
    custom = task_decorator(default_options={"partition": "cpu"})

    @custom
    def producer(x: int) -> int:
        return x + 1

    @custom
    def consumer(x: int) -> int:
        return x * 2

    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    ctx_token = _set_active_context(cluster)
    runtime_token = _set_active_runtime(ClusterRuntime())
    try:
        producer_job = producer(2)
        dep = DependencyRefStub(producer_job.id)
        chained = consumer.after(dep).with_options(mem="1G")
        assert isinstance(chained.task, TaskHandle)
        array_job = chained.map([1, 2, 3])
    finally:
        _reset_active_runtime(runtime_token)
        _reset_active_context(ctx_token)

    assert isinstance(array_job.task, TaskHandle)
    assert array_job.task.sbatch_options["partition"] == "cpu"
    assert array_job.task.sbatch_options["mem"] == "1G"
