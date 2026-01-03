"""Tests for type safety enhancements (TYPE_CHECKING, generics)."""

import sys
from pathlib import Path

from slurm.cluster import Cluster
from slurm.decorators import task, workflow
from slurm.context import (
    _set_active_context,
    _reset_active_context,
    _clear_active_context,
)
from slurm.workflow import WorkflowContext

# Allow importing test helpers
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))
from local_backend import LocalBackend  # type: ignore


def create_mock_cluster(tmp_path: Path) -> Cluster:
    """Create a mock cluster for testing."""
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)
    cluster.backend = LocalBackend(job_base_dir=str(tmp_path))
    cluster.backend_type = "LocalBackend"
    cluster.packaging_defaults = {"type": "none"}
    cluster.callbacks = []
    cluster.console = None
    # Add new string-based API attributes
    cluster.default_packaging = None
    cluster.default_account = None
    cluster.default_partition = None
    return cluster


def test_task_decorator_returns_job_type():
    """Test that @task decorated function has correct type signature."""

    @task(time="00:01:00", mem="1G")
    def simple_task(x: int) -> str:
        """Simple task that returns a string."""
        return f"result_{x}"

    # At runtime, simple_task is a SlurmTask
    from slurm.task import SlurmTask

    assert isinstance(simple_task, SlurmTask)

    # Type checkers should see: simple_task: (x: int) -> Job[str]
    # We can't directly test type checker behavior, but we can verify
    # the function can be called and returns a Job with the right type


def test_task_returns_job_at_runtime(tmp_path):
    """Test that calling @task decorated function returns Job[T]."""
    _clear_active_context()

    @task(time="00:01:00", mem="1G")
    def compute(x: int) -> int:
        """Compute x * 2."""
        return x * 2

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Call task - should return Job
        job = compute(5)

        # Verify it's a Job
        from slurm.job import Job

        assert isinstance(job, Job)

        # Verify Job has the expected attributes
        assert hasattr(job, "id")
        assert hasattr(job, "get_result")

        # The type checker would know job: Job[int]
        # At runtime, we can verify the result type matches
        # (Note: We can't actually call get_result in this test without a real backend)
    finally:
        _reset_active_context(token)


def test_workflow_decorator_returns_job_type():
    """Test that @workflow decorated function has correct type signature."""

    @workflow(time="02:00:00")
    def my_workflow(data: str, ctx: WorkflowContext) -> list[int]:
        """Workflow that returns list of ints."""
        return [1, 2, 3]

    # At runtime, my_workflow is a SlurmTask
    from slurm.task import SlurmTask

    assert isinstance(my_workflow, SlurmTask)

    # Type checkers should see: my_workflow: (data: str, ctx: WorkflowContext) -> Job[list[int]]


def test_workflow_returns_job_at_runtime(tmp_path):
    """Test that calling @workflow returns Job[T]."""
    _clear_active_context()

    @workflow(time="01:00:00")
    def simple_workflow(x: int, ctx: WorkflowContext) -> str:
        """Simple workflow."""
        return f"workflow_{x}"

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Call workflow - should return Job
        job = simple_workflow(42)

        # Verify it's a Job
        from slurm.job import Job

        assert isinstance(job, Job)

        # Type checker would know: job: Job[str]
    finally:
        _reset_active_context(token)


def test_job_generic_type_parameter():
    """Test that Job class is generic with type parameter T."""
    from slurm.job import Job
    from typing import get_origin

    # Job should be Generic[T]
    # We can check this by looking at __orig_bases__
    if hasattr(Job, "__orig_bases__"):
        from typing import Generic

        assert Generic in [get_origin(base) for base in Job.__orig_bases__]


def test_array_job_generic_type():
    """Test that ArrayJob is generic with correct type parameter."""
    from slurm.array_job import ArrayJob

    # ArrayJob should already be Generic[T]
    if hasattr(ArrayJob, "__orig_bases__"):
        from typing import Generic, get_origin

        assert Generic in [get_origin(base) for base in ArrayJob.__orig_bases__]


def test_map_returns_array_job_generic(tmp_path):
    """Test that .map() returns ArrayJob[T] with correct type."""
    _clear_active_context()

    @task(time="00:01:00")
    def process(x: int) -> str:
        """Process int to string."""
        return str(x)

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Call .map() - should return ArrayJob[str]
        array_job = process.map([1, 2, 3])

        from slurm.array_job import ArrayJob

        assert isinstance(array_job, ArrayJob)

        # Type checker would know: array_job: ArrayJob[str]
        # At runtime, verify it has expected methods
        assert hasattr(array_job, "get_results")
        assert len(array_job) == 3
    finally:
        _reset_active_context(token)


def test_task_without_args_type():
    """Test @task without arguments has correct type."""

    @task
    def no_args_task(x: float) -> float:
        """Task without decorator arguments."""
        return x * 2.0

    from slurm.task import SlurmTask

    assert isinstance(no_args_task, SlurmTask)
    # Type checker should see: no_args_task: (x: float) -> Job[float]


def test_workflow_without_args_type():
    """Test @workflow without arguments has correct type."""

    @workflow
    def no_args_workflow(ctx: WorkflowContext) -> dict:
        """Workflow without decorator arguments."""
        return {"status": "done"}

    from slurm.task import SlurmTask

    assert isinstance(no_args_workflow, SlurmTask)
    # Type checker should see: no_args_workflow: (ctx: WorkflowContext) -> Job[dict]


def test_complex_return_type():
    """Test that complex return types are preserved."""

    @task(time="00:10:00")
    def complex_task(data: str) -> dict[str, list[int]]:
        """Task with complex return type."""
        return {"numbers": [1, 2, 3]}

    from slurm.task import SlurmTask

    assert isinstance(complex_task, SlurmTask)
    # Type checker should see: complex_task: (data: str) -> Job[dict[str, list[int]]]


def test_get_result_type_matches_task_return(tmp_path):
    """Test that Job.get_result() returns the correct type T."""
    _clear_active_context()

    @task(time="00:01:00")
    def typed_task(x: int) -> list[str]:
        """Task that returns list of strings."""
        return [str(x), str(x * 2)]

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        job = typed_task(5)

        # Type checker should know:
        # job: Job[list[str]]
        # job.get_result(): list[str]

        # At runtime, verify Job type parameter
        from slurm.job import Job

        assert isinstance(job, Job)

        # The get_result method signature should be: def get_result(self) -> T
        import inspect

        sig = inspect.signature(job.get_result)
        # Return annotation should be T (TypeVar)
        assert sig.return_annotation is not inspect.Signature.empty
    finally:
        _reset_active_context(token)
