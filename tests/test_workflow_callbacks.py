"""Tests for workflow-specific callback methods (Phase 1)."""

from pathlib import Path

from slurm.callbacks.callbacks import (
    BaseCallback,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
    ExecutionLocus,
)


def test_workflow_callback_context_creation():
    """Test WorkflowCallbackContext dataclass creation."""
    ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path/to/workflow"),
        workflow_name="my_workflow",
        workflow_context=None,
        timestamp=1234567890.0,
        cluster=None,
    )

    assert ctx.workflow_job_id == "12345"
    assert ctx.workflow_job_dir == Path("/path/to/workflow")
    assert ctx.workflow_name == "my_workflow"
    assert ctx.workflow_context is None
    assert ctx.timestamp == 1234567890.0
    assert ctx.result is None
    assert ctx.exception is None
    assert ctx.cluster is None


def test_workflow_callback_context_with_result():
    """Test WorkflowCallbackContext with result."""
    ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path/to/workflow"),
        workflow_name="my_workflow",
        workflow_context=None,
        timestamp=1234567890.0,
        result={"status": "success"},
        cluster=None,
    )

    assert ctx.result == {"status": "success"}
    assert ctx.exception is None


def test_workflow_callback_context_with_exception():
    """Test WorkflowCallbackContext with exception."""
    exc = RuntimeError("Test error")
    ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path/to/workflow"),
        workflow_name="my_workflow",
        workflow_context=None,
        timestamp=1234567890.0,
        exception=exc,
        cluster=None,
    )

    assert ctx.result is None
    assert ctx.exception is exc
    assert str(ctx.exception) == "Test error"


def test_workflow_task_submit_context_creation():
    """Test WorkflowTaskSubmitContext dataclass creation."""
    ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="12345",
        parent_workflow_dir=Path("/path/to/workflow"),
        parent_workflow_name="my_workflow",
        child_job_id="67890",
        child_job_dir=Path("/path/to/child"),
        child_task_name="child_task",
        child_is_workflow=False,
        timestamp=1234567890.0,
        cluster=None,  # type: ignore
    )

    assert ctx.parent_workflow_id == "12345"
    assert ctx.parent_workflow_dir == Path("/path/to/workflow")
    assert ctx.parent_workflow_name == "my_workflow"
    assert ctx.child_job_id == "67890"
    assert ctx.child_job_dir == Path("/path/to/child")
    assert ctx.child_task_name == "child_task"
    assert ctx.child_is_workflow is False
    assert ctx.timestamp == 1234567890.0


def test_workflow_task_submit_context_nested_workflow():
    """Test WorkflowTaskSubmitContext for nested workflow."""
    ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="12345",
        parent_workflow_dir=Path("/path/to/outer"),
        parent_workflow_name="outer_workflow",
        child_job_id="67890",
        child_job_dir=Path("/path/to/inner"),
        child_task_name="inner_workflow",
        child_is_workflow=True,  # Child is also a workflow
        timestamp=1234567890.0,
        cluster=None,  # type: ignore
    )

    assert ctx.child_is_workflow is True
    assert ctx.child_task_name == "inner_workflow"


def test_base_callback_has_workflow_methods():
    """Test that BaseCallback has workflow-specific methods."""
    callback = BaseCallback()

    # Check methods exist
    assert hasattr(callback, "on_workflow_begin_ctx")
    assert hasattr(callback, "on_workflow_end_ctx")
    assert hasattr(callback, "on_workflow_task_submitted_ctx")

    # Check they're callable
    assert callable(callback.on_workflow_begin_ctx)
    assert callable(callback.on_workflow_end_ctx)
    assert callable(callback.on_workflow_task_submitted_ctx)


def test_workflow_callback_methods_are_no_op():
    """Test that default workflow callback methods are no-ops."""
    callback = BaseCallback()

    # Create dummy contexts
    workflow_ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path"),
        workflow_name="test",
        workflow_context=None,
        timestamp=123.0,
        cluster=None,
    )

    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="12345",
        parent_workflow_dir=Path("/path"),
        parent_workflow_name="test",
        child_job_id="67890",
        child_job_dir=Path("/child"),
        child_task_name="child",
        child_is_workflow=False,
        timestamp=123.0,
        cluster=None,  # type: ignore
    )

    # Should not raise any exceptions (no-op)
    callback.on_workflow_begin_ctx(workflow_ctx)
    callback.on_workflow_end_ctx(workflow_ctx)
    callback.on_workflow_task_submitted_ctx(submit_ctx)


def test_workflow_callback_execution_loci():
    """Test that workflow callbacks have correct execution loci."""
    callback = BaseCallback()

    # Workflow lifecycle events run on RUNNER
    assert (
        callback.get_execution_locus("on_workflow_begin_ctx") == ExecutionLocus.RUNNER
    )
    assert callback.get_execution_locus("on_workflow_end_ctx") == ExecutionLocus.RUNNER

    # Child task submission runs on CLIENT
    assert (
        callback.get_execution_locus("on_workflow_task_submitted_ctx")
        == ExecutionLocus.CLIENT
    )


def test_workflow_callbacks_should_run_on_runner():
    """Test workflow lifecycle callbacks should run on runner."""
    callback = BaseCallback()

    assert callback.should_run_on_runner("on_workflow_begin_ctx") is True
    assert callback.should_run_on_runner("on_workflow_end_ctx") is True
    assert callback.should_run_on_client("on_workflow_begin_ctx") is False
    assert callback.should_run_on_client("on_workflow_end_ctx") is False


def test_workflow_task_submit_should_run_on_client():
    """Test child task submission callback should run on client."""
    callback = BaseCallback()

    assert callback.should_run_on_client("on_workflow_task_submitted_ctx") is True
    assert callback.should_run_on_runner("on_workflow_task_submitted_ctx") is False


class WorkflowTrackingCallback(BaseCallback):
    """Test callback that tracks workflow events."""

    def __init__(self):
        super().__init__()
        self.workflow_begins = []
        self.workflow_ends = []
        self.task_submissions = []

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext):
        self.workflow_begins.append(ctx)

    def on_workflow_end_ctx(self, ctx: WorkflowCallbackContext):
        self.workflow_ends.append(ctx)

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext):
        self.task_submissions.append(ctx)


def test_custom_workflow_callback_tracking():
    """Test custom callback can track workflow events."""
    callback = WorkflowTrackingCallback()

    # Create and emit workflow begin
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    assert len(callback.workflow_begins) == 1
    assert callback.workflow_begins[0].workflow_name == "test_workflow"

    # Create and emit child task submission
    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="12345",
        parent_workflow_dir=Path("/path"),
        parent_workflow_name="test_workflow",
        child_job_id="67890",
        child_job_dir=Path("/child"),
        child_task_name="child_task",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx)

    assert len(callback.task_submissions) == 1
    assert callback.task_submissions[0].child_task_name == "child_task"

    # Create and emit workflow end
    end_ctx = WorkflowCallbackContext(
        workflow_job_id="12345",
        workflow_job_dir=Path("/path"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=120.0,
        result={"status": "success"},
        cluster=None,
    )
    callback.on_workflow_end_ctx(end_ctx)

    assert len(callback.workflow_ends) == 1
    assert callback.workflow_ends[0].result == {"status": "success"}


def test_callback_requires_runner_transport_includes_workflow():
    """Test that workflow callbacks are included in runner transport check."""
    callback = WorkflowTrackingCallback()

    # Should require runner transport since workflow callbacks run on runner
    assert callback.requires_runner_transport() is True


def test_workflow_decorator_sets_is_workflow_flag():
    """Test that @workflow decorator sets _is_workflow flag."""
    from slurm.decorators import task, workflow

    @task(time="00:01:00")
    def my_task(x: int) -> int:
        return x + 1

    @workflow(time="00:10:00")
    def my_workflow(x: int):
        return my_task(x)

    # Regular tasks should not have _is_workflow flag (or it should be falsy)
    assert not getattr(my_task, "_is_workflow", False)

    # Workflows should have _is_workflow=True
    assert hasattr(my_workflow, "_is_workflow")
    assert my_workflow._is_workflow is True
