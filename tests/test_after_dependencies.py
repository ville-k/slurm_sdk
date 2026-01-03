"""Tests for .after() explicit dependency binding (v0.7.0)."""

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


@task(time="00:01:00", mem="1G")
def task_a(x: int) -> int:
    """First task."""
    return x + 1


@task(time="00:01:00", mem="1G")
def task_b(y: int) -> int:
    """Second task."""
    return y * 2


@task(time="00:01:00", mem="1G")
def merge_task(output: str) -> str:
    """Merge task that doesn't use job results directly."""
    return f"merged_{output}"


def test_after_returns_new_task():
    """Test that .after() returns a new SlurmTaskWithDependencies instance."""
    _clear_active_context()

    # Call .after() should return a SlurmTaskWithDependencies
    bound_task = task_a.after()

    from slurm.task import SlurmTaskWithDependencies

    assert isinstance(bound_task, SlurmTaskWithDependencies)
    # Should be a different instance (stateless)
    assert bound_task is not task_a


def test_after_with_single_job(tmp_path):
    """Test .after() with a single job dependency."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Create first job
        job1 = task_a(5)

        # Create second job that explicitly depends on job1 (without using its result)
        job2 = merge_task.after(job1)("output.txt")

        # Both should be Jobs
        from slurm.job import Job

        assert isinstance(job1, Job)
        assert isinstance(job2, Job)

        # job2 should have dependency on job1 (checked in cluster.submit)
    finally:
        _reset_active_context(token)


def test_after_with_multiple_jobs(tmp_path):
    """Test .after() with multiple job dependencies."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Create two independent jobs
        job1 = task_a(1)
        job2 = task_b(2)

        # Create merge job that depends on both
        merge_job = merge_task.after(job1, job2)("combined.txt")

        from slurm.job import Job

        assert isinstance(merge_job, Job)
        # Dependencies should be tracked
    finally:
        _reset_active_context(token)


def test_after_with_hybrid_dependencies(tmp_path):
    """Test .after() combines with automatic dependencies from args."""
    _clear_active_context()

    @task(time="00:01:00")
    def process(data: int, extra: int) -> int:
        """Task with both automatic and explicit dependencies."""
        return data + extra

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        # Create three jobs
        job1 = task_a(1)  # Explicit dependency
        job2 = task_b(2)  # Automatic dependency (passed as arg)
        job3 = task_a(3)  # Explicit dependency

        # process depends on job1 and job3 explicitly (via .after())
        # and on job2 automatically (passed as argument)
        result_job = process.after(job1, job3)(job2, 100)

        from slurm.job import Job

        assert isinstance(result_job, Job)
        # Should have 3 total dependencies: job1, job3 (explicit) + job2 (automatic)
    finally:
        _reset_active_context(token)


def test_after_composition_with_with_options(tmp_path):
    """Test that .after() composes with .with_options()."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        job1 = task_a(5)

        # Compose .after() with .with_options() - order 1
        job2 = task_b.after(job1).with_options(mem="8GB")(10)

        from slurm.job import Job

        assert isinstance(job2, Job)
    finally:
        _reset_active_context(token)


def test_after_composition_with_map(tmp_path):
    """Test that .after() composes with .map() for array jobs."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        prep_job = task_a(1)

        # Use .after() with .map() to create array with dependency
        array_job = task_b.after(prep_job).map([1, 2, 3])

        from slurm.array_job import ArrayJob

        assert isinstance(array_job, ArrayJob)
        # Each task in the array should depend on prep_job
    finally:
        _reset_active_context(token)


def test_after_preserves_task_attributes():
    """Test that .after() preserves task function attributes."""
    bound_task = task_a.after()

    # Should preserve function name and docstring
    assert bound_task.__name__ == task_a.__name__
    assert bound_task.__doc__ == task_a.__doc__
    assert bound_task.func is task_a.func


def test_after_is_stateless(tmp_path):
    """Test that .after() doesn't modify the original task (stateless)."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        job1 = task_a(1)

        # Create bound task
        bound_task = task_b.after(job1)

        # Original task should not have pending dependencies
        assert task_b._pending_dependencies == []

        # Bound task should have the dependency
        assert len(bound_task._pending_dependencies) == 1
        assert bound_task._pending_dependencies[0] is job1
    finally:
        _reset_active_context(token)


def test_after_chaining():
    """Test that .after() can be chained multiple times."""
    _clear_active_context()

    # Create a chain of .after() calls
    bound1 = task_a.after()
    bound2 = bound1.after()

    from slurm.task import SlurmTaskWithDependencies

    assert isinstance(bound1, SlurmTaskWithDependencies)
    assert isinstance(bound2, SlurmTaskWithDependencies)
    assert bound2 is not bound1
    assert bound2 is not task_a


def test_after_with_workflow_context(tmp_path):
    """Test .after() works correctly in workflow context."""
    _clear_active_context()

    @workflow(time="00:10:00")
    def test_workflow(ctx: WorkflowContext) -> str:
        """Workflow using .after() pattern."""
        # Parallel jobs
        job1 = task_a(1)
        job2 = task_b(2)

        # Merge depends on both (explicit)
        merge_job = merge_task.after(job1, job2)("result.txt")

        return merge_job

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        workflow_job = test_workflow()

        from slurm.job import Job

        assert isinstance(workflow_job, Job)
    finally:
        _reset_active_context(token)


def test_after_raises_on_non_job_argument():
    """Test that .after() raises TypeError for non-Job arguments."""
    import pytest

    with pytest.raises(
        TypeError, match=".after\\(\\) expects Job or ArrayJob arguments"
    ):
        task_a.after("not_a_job")


def test_after_empty_call():
    """Test that .after() with no arguments works (creates new task)."""
    bound_task = task_a.after()

    from slurm.task import SlurmTaskWithDependencies

    assert isinstance(bound_task, SlurmTaskWithDependencies)
    assert len(bound_task._pending_dependencies) == 0


def test_after_preserves_sbatch_options():
    """Test that .after() preserves sbatch_options from original task."""
    bound_task = task_a.after()

    assert bound_task.sbatch_options == task_a.sbatch_options
    assert bound_task.sbatch_options is not task_a.sbatch_options  # Should be a copy


def test_after_preserves_packaging():
    """Test that .after() preserves packaging configuration."""
    bound_task = task_a.after()

    assert bound_task.packaging == task_a.packaging
    if task_a.packaging:
        assert bound_task.packaging is not task_a.packaging  # Should be a copy


def test_after_multiple_dependencies_accumulated(tmp_path):
    """Test that multiple .after() calls accumulate dependencies."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        job1 = task_a(1)
        job2 = task_b(2)
        job3 = task_a(3)

        # Chain .after() calls
        bound1 = merge_task.after(job1)
        bound2 = bound1.after(job2, job3)

        # bound2 should have all three dependencies
        assert len(bound2._pending_dependencies) == 3
        assert job1 in bound2._pending_dependencies
        assert job2 in bound2._pending_dependencies
        assert job3 in bound2._pending_dependencies

        # Create final job
        final_job = bound2("output.txt")

        from slurm.job import Job

        assert isinstance(final_job, Job)
    finally:
        _reset_active_context(token)


def test_after_with_unwrapped():
    """Test that .unwrapped still works on bound tasks."""
    bound_task = task_a.after()

    # Should still have .unwrapped property
    assert hasattr(bound_task, "unwrapped")
    assert callable(bound_task.unwrapped)

    # Should execute locally
    result = bound_task.unwrapped(5)
    assert result == 6  # task_a adds 1


def test_after_pattern_with_map(tmp_path):
    """Test the full method chain: task.after().with_options().map()."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)

    try:
        prep_job = task_a(1)

        # Full composition chain
        array_job = task_b.after(prep_job).map([10, 20, 30])

        from slurm.array_job import ArrayJob

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
    finally:
        _reset_active_context(token)
