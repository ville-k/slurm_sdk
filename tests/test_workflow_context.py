"""Unit tests for workflow context injection and runner integration."""

from pathlib import Path

from slurm.decorators import workflow
from slurm.workflow import WorkflowContext
from slurm.cluster import Cluster
from slurm.runtime import JobContext


def test_workflow_context_creation(tmp_path):
    """Test creating WorkflowContext."""
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)

    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )

    assert ctx.cluster is cluster
    assert ctx.workflow_job_id == "test_123"
    assert ctx.workflow_job_dir == workflow_dir
    assert ctx.shared_dir == shared_dir
    assert ctx.local_mode is False


def test_workflow_context_properties(tmp_path):
    """Test WorkflowContext properties."""
    cluster = object.__new__(Cluster)
    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )

    # Test result_path property
    assert ctx.result_path == workflow_dir / "result.pkl"

    # Test metadata_path property
    assert ctx.metadata_path == workflow_dir / "metadata.json"

    # Test tasks_dir property
    assert ctx.tasks_dir == workflow_dir / "tasks"


def test_workflow_context_get_task_output_dir(tmp_path):
    """Test WorkflowContext.get_task_output_dir()."""
    cluster = object.__new__(Cluster)
    workflow_dir = tmp_path / "workflow"

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    task_dir = ctx.get_task_output_dir("my_task")
    assert task_dir == workflow_dir / "tasks" / "my_task"


def test_workflow_context_list_task_runs(tmp_path):
    """Test WorkflowContext.list_task_runs()."""
    cluster = object.__new__(Cluster)
    workflow_dir = tmp_path / "workflow"

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    # Create some task run directories
    task_dir = workflow_dir / "tasks" / "my_task"
    task_dir.mkdir(parents=True, exist_ok=True)

    (task_dir / "20250107_140000_abc123").mkdir()
    (task_dir / "20250107_140100_def456").mkdir()
    (task_dir / "20250107_140200_ghi789").mkdir()

    # List runs (should be sorted newest first)
    runs = ctx.list_task_runs("my_task")
    assert len(runs) == 3
    assert runs[0].name == "20250107_140200_ghi789"  # Newest
    assert runs[1].name == "20250107_140100_def456"
    assert runs[2].name == "20250107_140000_abc123"  # Oldest


def test_workflow_context_list_task_runs_empty(tmp_path):
    """Test WorkflowContext.list_task_runs() with no runs."""
    cluster = object.__new__(Cluster)
    workflow_dir = tmp_path / "workflow"

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    # Non-existent task
    runs = ctx.list_task_runs("nonexistent_task")
    assert runs == []


def test_workflow_function_wants_context():
    """Test detecting if workflow function wants WorkflowContext."""
    from slurm.runner import function_wants_workflow_context

    @workflow
    def workflow_with_ctx(ctx: WorkflowContext) -> int:
        return 42

    @workflow
    def workflow_with_ctx_annotation(ctx: "WorkflowContext") -> int:
        return 42

    @workflow
    def workflow_with_context_param(context: WorkflowContext) -> int:
        return 42

    @workflow
    def workflow_with_workflow_context_param(workflow_context: WorkflowContext) -> int:
        return 42

    @workflow
    def workflow_without_ctx(x: int) -> int:
        return x + 1

    # These should want context
    assert function_wants_workflow_context(workflow_with_ctx.unwrapped)
    assert function_wants_workflow_context(workflow_with_ctx_annotation.unwrapped)
    assert function_wants_workflow_context(workflow_with_context_param.unwrapped)
    assert function_wants_workflow_context(
        workflow_with_workflow_context_param.unwrapped
    )

    # This should not want context
    assert not function_wants_workflow_context(workflow_without_ctx.unwrapped)


def test_workflow_context_injection():
    """Test WorkflowContext injection into workflow function."""
    from slurm.runner import bind_workflow_context

    def workflow_func(x: int, ctx: WorkflowContext) -> int:
        return x + 1

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=Path("/tmp/workflow"),
        shared_dir=Path("/tmp/workflow/shared"),
        local_mode=False,
    )

    # Inject context
    args, kwargs, injected = bind_workflow_context(
        workflow_func, (10,), {}, workflow_ctx
    )

    assert injected is True
    assert args == (10,)
    assert "ctx" in kwargs
    assert kwargs["ctx"] is workflow_ctx


def test_workflow_context_injection_already_provided():
    """Test that context is not injected if already provided."""
    from slurm.runner import bind_workflow_context

    def workflow_func(x: int, ctx: WorkflowContext) -> int:
        return x + 1

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=Path("/tmp/workflow"),
        shared_dir=Path("/tmp/workflow/shared"),
        local_mode=False,
    )

    another_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="another_123",
        workflow_job_dir=Path("/tmp/another"),
        shared_dir=Path("/tmp/another/shared"),
        local_mode=False,
    )

    # Context already provided in kwargs
    args, kwargs, injected = bind_workflow_context(
        workflow_func, (10,), {"ctx": another_ctx}, workflow_ctx
    )

    assert injected is False
    assert kwargs["ctx"] is another_ctx  # Original context preserved


def test_workflow_context_injection_by_name():
    """Test context injection works with different parameter names."""
    from slurm.runner import bind_workflow_context

    cluster = object.__new__(Cluster)
    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=Path("/tmp/workflow"),
        shared_dir=Path("/tmp/workflow/shared"),
        local_mode=False,
    )

    # Test with 'context' parameter name
    def workflow_with_context(x: int, context: WorkflowContext) -> int:
        return x + 1

    args, kwargs, injected = bind_workflow_context(
        workflow_with_context, (10,), {}, workflow_ctx
    )
    assert injected is True
    assert "context" in kwargs

    # Test with 'workflow_context' parameter name
    def workflow_with_workflow_context(
        x: int, workflow_context: WorkflowContext
    ) -> int:
        return x + 1

    args, kwargs, injected = bind_workflow_context(
        workflow_with_workflow_context, (10,), {}, workflow_ctx
    )
    assert injected is True
    assert "workflow_context" in kwargs


def test_workflow_context_symmetry_with_job_context():
    """Test that WorkflowContext has similar structure to JobContext.

    NOTE: WorkflowContext has additional workflow-specific properties
    (result_path, metadata_path, tasks_dir) that JobContext doesn't have.
    Both share the output_dir concept but use it differently.
    """
    cluster = object.__new__(Cluster)
    workflow_dir = Path("/tmp/workflow")

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=workflow_dir,
        shared_dir=workflow_dir / "shared",
        local_mode=False,
    )

    job_ctx = JobContext(
        job_id="456",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
        hostnames=(),
        output_dir=Path("/tmp/job"),
        environment={},
    )

    # WorkflowContext has workflow-specific properties
    assert hasattr(workflow_ctx, "result_path")
    assert hasattr(workflow_ctx, "metadata_path")
    assert hasattr(workflow_ctx, "tasks_dir")
    assert hasattr(workflow_ctx, "get_task_output_dir")
    assert hasattr(workflow_ctx, "list_task_runs")

    # JobContext has output_dir (similar concept to workflow_job_dir)
    assert hasattr(job_ctx, "output_dir")
    assert hasattr(workflow_ctx, "workflow_job_dir")


def test_workflow_decorator_preserves_attributes():
    """Test that @workflow decorator preserves function attributes."""

    @workflow(time="01:00:00")
    def my_workflow(x: int) -> int:
        """My workflow docstring."""
        return x + 1

    assert my_workflow.__name__ == "my_workflow"
    assert "My workflow docstring" in (my_workflow.__doc__ or "")
    assert hasattr(my_workflow, "unwrapped")
    assert callable(my_workflow.unwrapped)
