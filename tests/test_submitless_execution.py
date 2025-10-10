"""Unit tests for submitless execution (task calling returns Jobs)."""

import sys
import pytest
from pathlib import Path

from slurm.cluster import Cluster
from slurm.decorators import task, workflow
from slurm.context import set_active_context, reset_active_context, clear_active_context
from slurm.workflow import WorkflowContext
from slurm.job import Job

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
    return cluster


@task(time="00:01:00", mem="1G")
def simple_task(x: int) -> int:
    """Simple task for testing."""
    return x + 1


@task(time="00:01:00", mem="1G")
def task_with_args(x: int, y: int) -> int:
    """Task with multiple arguments."""
    return x + y


@task(time="00:01:00", mem="1G")
def task_with_kwargs(x: int, y: int = 10) -> int:
    """Task with keyword arguments."""
    return x + y


def test_task_outside_context_raises():
    """Test that calling task outside context raises RuntimeError."""
    clear_active_context()

    with pytest.raises(RuntimeError) as exc_info:
        simple_task(5)

    assert "@task decorated function" in str(exc_info.value)
    assert "must be called within a Cluster context" in str(exc_info.value)
    assert "unwrapped" in str(exc_info.value)


def test_task_with_cluster_context_returns_job(tmp_path):
    """Test that calling task within cluster context returns Job."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Call task - should return Job (immediately submitted in current implementation)
        result = simple_task(5)
        assert isinstance(result, Job)
        # Current implementation submits immediately, so job has an ID
        assert hasattr(result, "id")
    finally:
        reset_active_context(token)


def test_task_unwrapped_works_without_context():
    """Test that .unwrapped allows local execution without context."""
    clear_active_context()

    # Call unwrapped - should execute immediately
    result = simple_task.unwrapped(5)
    assert result == 6


def test_task_with_multiple_args(tmp_path):
    """Test task with multiple positional arguments."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        job = task_with_args(3, 4)
        assert isinstance(job, Job)
        assert job.args == (3, 4)
        assert job.kwargs == {}
    finally:
        reset_active_context(token)


def test_task_with_kwargs(tmp_path):
    """Test task with keyword arguments."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # With default kwargs
        job1 = task_with_kwargs(5)
        assert isinstance(job1, Job)
        assert job1.args == (5,)
        assert job1.kwargs == {}

        # With explicit kwargs
        job2 = task_with_kwargs(5, y=20)
        assert isinstance(job2, Job)
        assert job2.args == (5,)
        assert job2.kwargs == {"y": 20}
    finally:
        reset_active_context(token)


def test_task_with_workflow_context(tmp_path):
    """Test that task works within workflow context."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=tmp_path / "workflow",
        shared_dir=tmp_path / "workflow" / "shared",
        local_mode=False,
    )

    token = set_active_context(workflow_ctx)

    try:
        # Call task - should use cluster from workflow context
        job = simple_task(5)
        assert isinstance(job, Job)
        assert job.cluster is cluster
    finally:
        reset_active_context(token)


def test_task_with_options_override(tmp_path):
    """Test .with_options() for runtime parameter override."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Create task with overridden options
        gpu_task = simple_task.with_options(gpus=1, partition="gpu")

        # Call with overridden options
        job = gpu_task(5)
        assert isinstance(job, Job)

        # Check that options were applied
        # (The actual sbatch_options will be merged during submission)
        assert hasattr(simple_task, "with_options")

        # Original task unchanged
        job2 = simple_task(5)
        assert isinstance(job2, Job)
    finally:
        reset_active_context(token)


def test_multiple_tasks_in_sequence(tmp_path):
    """Test calling multiple tasks in sequence."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Call multiple tasks
        job1 = simple_task(1)
        job2 = simple_task(2)
        job3 = task_with_args(3, 4)

        # All should be Jobs
        assert isinstance(job1, Job)
        assert isinstance(job2, Job)
        assert isinstance(job3, Job)

        # All should use same cluster
        assert job1.cluster is cluster
        assert job2.cluster is cluster
        assert job3.cluster is cluster
    finally:
        reset_active_context(token)


def test_cluster_context_manager(tmp_path):
    """Test using Cluster as context manager for submitless execution."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)

    # Mock __enter__ and __exit__
    cluster._context_token = None

    def __enter__(self):
        self._context_token = set_active_context(self)
        return self

    def __exit__(self, *args):
        if self._context_token:
            reset_active_context(self._context_token)
        return False

    cluster.__enter__ = __enter__.__get__(cluster, Cluster)
    cluster.__exit__ = __exit__.__get__(cluster, Cluster)

    # Use as context manager
    with cluster:
        job = simple_task(5)
        assert isinstance(job, Job)

    # Context should be cleared after exiting
    assert get_active_context() is None

    # Should raise outside context
    with pytest.raises(RuntimeError):
        simple_task(5)


def test_task_preserves_function_attributes():
    """Test that decorated task preserves original function attributes."""
    assert simple_task.__name__ == "simple_task"
    assert "Simple task for testing" in (simple_task.__doc__ or "")
    assert hasattr(simple_task, "unwrapped")
    assert callable(simple_task.unwrapped)


def test_task_callable_protocol():
    """Test that task follows callable protocol."""
    assert callable(simple_task)
    assert callable(simple_task.unwrapped)
    assert hasattr(simple_task, "__call__")


def get_active_context():
    """Helper to get active context for testing."""
    from slurm.context import get_active_context as _get

    return _get()
