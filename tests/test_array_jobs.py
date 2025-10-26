"""Unit tests for array jobs and .map() method."""

import sys
import pytest
from pathlib import Path

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.context import set_active_context, reset_active_context, clear_active_context
from slurm.array_job import ArrayJob

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
def process_item(item: str) -> str:
    """Process a single item."""
    return item.upper()


@task(time="00:01:00", mem="1G")
def process_tuple(x: int, y: int) -> int:
    """Process tuple arguments."""
    return x + y


@task(time="00:01:00", mem="1G")
def process_with_kwargs(x: int, y: int = 10) -> int:
    """Process with keyword arguments."""
    return x * y


def test_map_without_context_raises():
    """Test that .map() outside context raises RuntimeError."""
    clear_active_context()

    items = ["a", "b", "c"]

    with pytest.raises(RuntimeError) as exc_info:
        process_item.map(items)

    assert "must be called within a Cluster context" in str(exc_info.value)


def test_map_with_single_values(tmp_path):
    """Test .map() with single values (passed as first arg)."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = ["a", "b", "c"]
        array_job = process_item.map(items)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
        assert array_job.task is process_item
    finally:
        reset_active_context(token)


def test_map_with_tuples(tmp_path):
    """Test .map() with tuples (unpacked as positional args)."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = [(1, 2), (3, 4), (5, 6)]
        array_job = process_tuple.map(items)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
    finally:
        reset_active_context(token)


def test_map_with_dicts(tmp_path):
    """Test .map() with dicts (unpacked as keyword args)."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = [
            {"x": 1, "y": 10},
            {"x": 2, "y": 20},
            {"x": 3, "y": 30},
        ]
        array_job = process_with_kwargs.map(items)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
    finally:
        reset_active_context(token)


def test_array_job_length(tmp_path):
    """Test ArrayJob __len__ method."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = list(range(100))
        array_job = process_item.map([str(i) for i in items])

        assert len(array_job) == 100
    finally:
        reset_active_context(token)


def test_array_job_getitem(tmp_path):
    """Test ArrayJob __getitem__ method."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = ["a", "b", "c"]
        array_job = process_item.map(items)

        # Access individual jobs by index
        job0 = array_job[0]
        job1 = array_job[1]
        job2 = array_job[2]

        # All should be Job objects
        from slurm.job import Job

        assert isinstance(job0, Job)
        assert isinstance(job1, Job)
        assert isinstance(job2, Job)
    finally:
        reset_active_context(token)


def test_array_job_after_dependency(tmp_path):
    """Test ArrayJob.after() for explicit dependencies."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Create prerequisite job
        prep_job = process_item("setup")

        # Create array job that depends on prep_job
        items = ["a", "b", "c"]
        array_job = process_item.map(items).after(prep_job)

        assert isinstance(array_job, ArrayJob)
        # Should have dependency tracked
    finally:
        reset_active_context(token)


def test_array_job_with_max_concurrent(tmp_path):
    """Test ArrayJob with max_concurrent parameter."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = list(range(100))
        array_job = process_item.map([str(i) for i in items], max_concurrent=10)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 100
    finally:
        reset_active_context(token)


def test_array_job_get_results_dir(tmp_path):
    """Test ArrayJob.get_results_dir() method.

    NOTE: ArrayJobs are not immediately submitted in current implementation,
    so get_results_dir() raises until the array job is submitted.
    """
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = ["a", "b", "c"]
        array_job = process_item.map(items)

        # Array job not yet submitted, so get_results_dir() will raise
        with pytest.raises(RuntimeError, match="array job has not been submitted yet"):
            array_job.get_results_dir()

        # Verify array job was created
        assert isinstance(array_job, ArrayJob)
    finally:
        reset_active_context(token)


def test_array_job_directory_structure(tmp_path):
    """Test that ArrayJob creates correct directory structure.

    NOTE: ArrayJobs are not immediately submitted, so array_dir is None
    until submission.
    """
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = ["a", "b", "c"]
        array_job = process_item.map(items)

        # Array job has array_dir attribute, but it's None until submitted
        assert hasattr(array_job, "array_dir")
        assert array_job.array_dir is None  # Not yet submitted

        # Verify the ArrayJob was created correctly
        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
    finally:
        reset_active_context(token)


def test_map_empty_list(tmp_path):
    """Test .map() with empty list."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = []
        array_job = process_item.map(items)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 0
    finally:
        reset_active_context(token)


def test_map_preserves_order(tmp_path):
    """Test that .map() preserves item order."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        items = ["first", "second", "third", "fourth"]
        array_job = process_item.map(items)

        # Items should be stored in order
        assert hasattr(array_job, "items")
        assert array_job.items == items
    finally:
        reset_active_context(token)


def test_array_job_fluent_api(tmp_path):
    """Test fluent API with array jobs."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        prep_job = process_item("setup")

        # Fluent API: chain .map() and .after()
        array_job = process_item.map(["a", "b", "c"]).after(prep_job)

        assert isinstance(array_job, ArrayJob)
    finally:
        reset_active_context(token)


def test_array_job_with_job_dependencies(tmp_path):
    """Test array job where items include Job objects."""
    clear_active_context()

    @task(time="00:01:00", mem="1G")
    def merge_task(a: int, b: str) -> str:
        return f"{a}_{b}"

    cluster = create_mock_cluster(tmp_path)
    token = set_active_context(cluster)

    try:
        # Create some jobs
        job1 = process_item("x")
        job2 = process_item("y")
        job3 = process_item("z")

        # Map over items that include jobs
        # Each array task should depend on its corresponding job
        items = [
            (job1, "suffix1"),
            (job2, "suffix2"),
            (job3, "suffix3"),
        ]
        array_job = merge_task.map(items)

        assert isinstance(array_job, ArrayJob)
        assert len(array_job) == 3
    finally:
        reset_active_context(token)
