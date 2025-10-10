"""Integration tests for workflow orchestration with nested tasks."""

import sys
import pytest
from pathlib import Path
from typing import Optional

from slurm.cluster import Cluster
from slurm.decorators import task, workflow
from slurm.workflow import WorkflowContext
from slurm.runtime import JobContext
from slurm.context import clear_active_context

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
def prep_data(input_file: str) -> dict:
    """Prepare data for processing."""
    return {"file": input_file, "rows": 100}


@task(time="00:01:00", mem="2G")
def process_data(data: dict, ctx: Optional[JobContext] = None) -> dict:
    """Process the data."""
    return {"file": data["file"], "processed": True, "result": data["rows"] * 2}


@task(time="00:01:00", mem="1G")
def aggregate(results: list) -> int:
    """Aggregate results."""
    return sum(r["result"] for r in results)


@workflow(time="01:00:00", job_name="simple_pipeline")
def simple_workflow(input_files: list[str], ctx: WorkflowContext) -> int:
    """Simple workflow with sequential tasks."""
    # Stage 1: Prep data
    prep_job = prep_data(input_files[0])

    # Stage 2: Process (depends on prep)
    process_job = process_data(prep_job)

    # Get result
    result = process_job.get_result()
    return result["result"]


@workflow(time="01:00:00", job_name="parallel_pipeline")
def parallel_workflow(input_files: list[str], ctx: WorkflowContext) -> int:
    """Workflow with parallel tasks."""
    # Stage 1: Prep all files in parallel
    prep_jobs = [prep_data(f) for f in input_files]

    # Stage 2: Process all (each depends on its prep job)
    process_jobs = [process_data(job) for job in prep_jobs]

    # Stage 3: Aggregate
    results = [job.get_result() for job in process_jobs]
    total = aggregate(results)

    return total.get_result()


@workflow(time="01:00:00", job_name="array_pipeline")
def array_workflow(input_files: list[str], ctx: WorkflowContext) -> int:
    """Workflow using array jobs."""
    # Use .map() for parallel execution
    prep_jobs = prep_data.map(input_files)
    process_jobs = process_data.map(prep_jobs.get_results())

    # Aggregate
    results = process_jobs.get_results()
    return sum(r["result"] for r in results)


def test_workflow_has_context_parameter():
    """Test that workflow functions can have WorkflowContext parameter."""
    import inspect

    sig = inspect.signature(simple_workflow.unwrapped)
    params = list(sig.parameters.values())

    # Should have input_files and ctx parameters
    assert len(params) == 2
    assert params[0].name == "input_files"
    assert params[1].name == "ctx"


def test_workflow_outside_context_raises():
    """Test that calling workflow outside context raises."""
    clear_active_context()

    with pytest.raises(RuntimeError) as exc_info:
        simple_workflow(["file1.csv"])

    assert "@workflow decorated function" in str(
        exc_info.value
    ) or "@task decorated function" in str(exc_info.value)


def test_workflow_unwrapped_works():
    """Test that workflow.unwrapped can be tested locally."""
    clear_active_context()

    # Create mock cluster and context
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = "/tmp/test"

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=Path("/tmp/workflow"),
        shared_dir=Path("/tmp/workflow/shared"),
        local_mode=True,
    )

    # Can't actually execute the workflow locally without proper setup,
    # but we can verify it has the right structure
    assert hasattr(simple_workflow, "unwrapped")
    assert callable(simple_workflow.unwrapped)


def test_workflow_creates_task_directories(tmp_path):
    """Test that workflow creates proper directory structure for tasks."""
    clear_active_context()

    cluster = create_mock_cluster(tmp_path)

    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    # Verify directory helpers
    task_dir = workflow_ctx.get_task_output_dir("my_task")
    assert task_dir == workflow_dir / "tasks" / "my_task"

    # Verify tasks_dir property
    assert workflow_ctx.tasks_dir == workflow_dir / "tasks"


def test_workflow_context_shared_directory(tmp_path):
    """Test that workflow context provides shared directory."""
    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"
    shared_dir.mkdir(parents=True)

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )

    # Shared dir should be accessible
    assert workflow_ctx.shared_dir == shared_dir
    assert workflow_ctx.shared_dir.exists()

    # Can write to shared dir
    test_file = workflow_ctx.shared_dir / "test.txt"
    test_file.write_text("test data")
    assert test_file.exists()


def test_nested_task_calls_within_workflow(tmp_path):
    """Test that tasks can be called within workflow context."""
    clear_active_context()

    from slurm.context import set_active_context, reset_active_context

    cluster = create_mock_cluster(tmp_path)

    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    # Set workflow context as active
    token = set_active_context(workflow_ctx)

    try:
        # Tasks should be callable within workflow context
        from slurm.job import Job

        job = prep_data("test.csv")
        assert isinstance(job, Job)
        assert job.cluster is cluster
    finally:
        reset_active_context(token)


def test_workflow_with_explicit_dependencies(tmp_path):
    """Test workflow with explicit .after() dependencies."""
    clear_active_context()

    @workflow(time="01:00:00")
    def workflow_with_after(files: list[str], ctx: WorkflowContext) -> int:
        # Parallel prep
        prep_jobs = [prep_data(f) for f in files]

        # Process that waits for all prep jobs
        # Even though it doesn't use their results directly
        process_job = process_data({"file": "merged.csv", "rows": 1000}).after(
            *prep_jobs
        )

        return process_job.get_result()["result"]

    # Verify workflow structure
    assert hasattr(workflow_with_after, "unwrapped")
    assert workflow_with_after.__name__ == "workflow_with_after"


def test_workflow_result_path(tmp_path):
    """Test that workflow context has proper result path."""
    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    # Result path should be in workflow directory
    assert workflow_ctx.result_path == workflow_dir / "result.pkl"
    assert workflow_ctx.metadata_path == workflow_dir / "metadata.json"


def test_workflow_list_task_runs_sorting(tmp_path):
    """Test that list_task_runs returns newest first."""
    workflow_dir = tmp_path / "workflow"
    task_dir = workflow_dir / "tasks" / "my_task"
    task_dir.mkdir(parents=True)

    # Create run directories with different timestamps
    (task_dir / "20250101_100000_abc").mkdir()
    (task_dir / "20250102_100000_def").mkdir()
    (task_dir / "20250103_100000_ghi").mkdir()

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    runs = workflow_ctx.list_task_runs("my_task")

    # Should be sorted newest first
    assert len(runs) == 3
    assert runs[0].name == "20250103_100000_ghi"
    assert runs[1].name == "20250102_100000_def"
    assert runs[2].name == "20250101_100000_abc"


def test_workflow_decorator_preserves_metadata():
    """Test that @workflow preserves function metadata."""

    @workflow(time="02:00:00", mem="8G", job_name="test_workflow")
    def my_workflow(x: int, ctx: WorkflowContext) -> int:
        """Workflow documentation."""
        return x + 1

    assert my_workflow.__name__ == "my_workflow"
    assert "Workflow documentation" in (my_workflow.__doc__ or "")
    assert hasattr(my_workflow, "sbatch_options")
    assert my_workflow.sbatch_options["time"] == "02:00:00"
    assert my_workflow.sbatch_options["mem"] == "8G"
    assert my_workflow.sbatch_options["job_name"] == "test_workflow"


def test_workflow_with_conditional_logic(tmp_path):
    """Test workflow with conditional task execution."""
    clear_active_context()

    @workflow(time="01:00:00")
    def conditional_workflow(threshold: int, ctx: WorkflowContext) -> str:
        # Initial task
        prep_job = prep_data("data.csv")
        prep_result = prep_job.get_result()

        # Conditional execution
        if prep_result["rows"] > threshold:
            process_job = process_data(prep_result)
            return "processed"
        else:
            return "skipped"

    # Verify workflow structure
    assert hasattr(conditional_workflow, "unwrapped")
    assert callable(conditional_workflow.unwrapped)
