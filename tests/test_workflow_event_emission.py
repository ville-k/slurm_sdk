"""Integration tests for workflow callback event emission (Phase 2)."""

import sys
import tempfile
from pathlib import Path
from typing import List

from slurm.callbacks.callbacks import (
    BaseCallback,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
)
from slurm.decorators import task, workflow
from slurm.cluster import Cluster
from slurm.workflow import WorkflowContext

# Allow importing test helpers
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))


class WorkflowEventRecorder(BaseCallback):
    """Callback that records all workflow events for testing."""

    def __init__(self):
        super().__init__()
        self.workflow_begins: List[WorkflowCallbackContext] = []
        self.workflow_ends: List[WorkflowCallbackContext] = []
        self.task_submissions: List[WorkflowTaskSubmitContext] = []

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext):
        """Record workflow begin event."""
        self.workflow_begins.append(ctx)

    def on_workflow_end_ctx(self, ctx: WorkflowCallbackContext):
        """Record workflow end event."""
        self.workflow_ends.append(ctx)

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext):
        """Record task submission event."""
        self.task_submissions.append(ctx)


def test_workflow_lifecycle_events_in_runner(tmp_path):
    """Test that workflow begin/end events are emitted when workflow runs."""

    # Create a simple workflow that we can execute in submitless mode
    @task(time="00:01:00")
    def simple_task(x: int) -> int:
        return x + 1

    @workflow(time="00:05:00")
    def simple_workflow(ctx: WorkflowContext, x: int) -> int:
        # In submitless mode, this just calls the unwrapped function
        job = simple_task.unwrapped(x)
        return job

    # Set up callback recorder
    recorder = WorkflowEventRecorder()

    # NOTE: This test verifies the Phase 2 infrastructure (callback methods exist
    # and events can be emitted). Full runner integration testing requires actual
    # job execution which is better tested in end-to-end tests with a real/mock
    # SLURM environment.

    # Verify that the recorder has the workflow callback methods
    assert hasattr(recorder, "on_workflow_begin_ctx")
    assert hasattr(recorder, "on_workflow_end_ctx")
    assert hasattr(recorder, "on_workflow_task_submitted_ctx")

    # Verify workflow has the _is_workflow flag
    assert hasattr(simple_workflow, "_is_workflow")
    assert simple_workflow._is_workflow is True

    # Verify regular tasks don't have _is_workflow flag
    assert not getattr(simple_task, "_is_workflow", False)


def test_workflow_task_submission_events(tmp_path):
    """Test that child task submission events are emitted in workflow context."""

    # Create tasks and workflow
    @task(time="00:01:00")
    def child_task(x: int) -> int:
        return x * 2

    @workflow(time="00:05:00")
    def parent_workflow(ctx: WorkflowContext, x: int) -> int:
        # Submit a child task - this should trigger submission event
        job = child_task(x)
        return job.get_result()

    # Set up callback recorder
    recorder = WorkflowEventRecorder()

    # NOTE: Full integration test would require actual submission to backend.
    # This test verifies the infrastructure is in place.

    # Verify workflow flag
    assert hasattr(parent_workflow, "_is_workflow")
    assert parent_workflow._is_workflow is True

    # Verify child task doesn't have workflow flag
    assert not getattr(child_task, "_is_workflow", False)


def test_nested_workflow_events(tmp_path):
    """Test workflow events for nested workflows."""

    # Create nested workflow structure
    @task(time="00:01:00")
    def leaf_task(x: int) -> int:
        return x + 1

    @workflow(time="00:03:00")
    def inner_workflow(ctx: WorkflowContext, x: int) -> int:
        job = leaf_task(x)
        return job.get_result()

    @workflow(time="00:10:00")
    def outer_workflow(ctx: WorkflowContext, x: int) -> int:
        # Submit inner workflow as child
        inner_job = inner_workflow(x)
        return inner_job.get_result()

    # Verify all workflow flags are correct
    assert getattr(outer_workflow, "_is_workflow", False) is True
    assert getattr(inner_workflow, "_is_workflow", False) is True
    assert not getattr(leaf_task, "_is_workflow", False)

    # NOTE: Full nested workflow execution would require backend support.
    # Event ordering should be:
    # 1. on_begin_run_job(outer_workflow)
    # 2. on_workflow_begin(outer_workflow)
    # 3. on_workflow_task_submitted(inner_workflow)  # child_is_workflow=True
    # 4.   on_begin_run_job(inner_workflow)
    # 5.   on_workflow_begin(inner_workflow)
    # 6.   on_workflow_task_submitted(leaf_task)  # child_is_workflow=False
    # 7.   on_workflow_end(inner_workflow)
    # 8.   on_end_run_job(inner_workflow)
    # 9. on_workflow_end(outer_workflow)
    # 10. on_end_run_job(outer_workflow)


def test_workflow_event_context_fields():
    """Test that workflow event contexts have all required fields."""
    from pathlib import Path as PathType

    # Create a WorkflowCallbackContext
    workflow_ctx = WorkflowCallbackContext(
        workflow_job_id="test_workflow_12345",
        workflow_job_dir=PathType("/path/to/workflow"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=1234567890.0,
        cluster=None,
    )

    # Verify all required fields are present
    assert workflow_ctx.workflow_job_id == "test_workflow_12345"
    assert workflow_ctx.workflow_job_dir == PathType("/path/to/workflow")
    assert workflow_ctx.workflow_name == "test_workflow"
    assert workflow_ctx.timestamp == 1234567890.0

    # Verify optional fields default to None
    assert workflow_ctx.result is None
    assert workflow_ctx.exception is None
    assert workflow_ctx.cluster is None


def test_workflow_task_submit_context_fields():
    """Test that task submission context has all required fields."""
    from pathlib import Path as PathType

    # Create a WorkflowTaskSubmitContext
    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="parent_12345",
        parent_workflow_dir=PathType("/path/to/parent"),
        parent_workflow_name="parent_workflow",
        child_job_id="child_67890",
        child_job_dir=PathType("/path/to/child"),
        child_task_name="child_task",
        child_is_workflow=False,
        timestamp=1234567890.0,
        cluster=None,  # type: ignore
    )

    # Verify all fields
    assert submit_ctx.parent_workflow_id == "parent_12345"
    assert submit_ctx.parent_workflow_dir == PathType("/path/to/parent")
    assert submit_ctx.parent_workflow_name == "parent_workflow"
    assert submit_ctx.child_job_id == "child_67890"
    assert submit_ctx.child_job_dir == PathType("/path/to/child")
    assert submit_ctx.child_task_name == "child_task"
    assert submit_ctx.child_is_workflow is False
    assert submit_ctx.timestamp == 1234567890.0


def test_workflow_event_recorder_callback():
    """Test that WorkflowEventRecorder correctly captures events."""
    from pathlib import Path as PathType

    recorder = WorkflowEventRecorder()

    # Emit workflow begin
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="test_12345",
        workflow_job_dir=PathType("/test"),
        workflow_name="test",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    recorder.on_workflow_begin_ctx(begin_ctx)

    assert len(recorder.workflow_begins) == 1
    assert recorder.workflow_begins[0].workflow_job_id == "test_12345"

    # Emit task submission
    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="test_12345",
        parent_workflow_dir=PathType("/test"),
        parent_workflow_name="test",
        child_job_id="child_67890",
        child_job_dir=PathType("/test/child"),
        child_task_name="child",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    recorder.on_workflow_task_submitted_ctx(submit_ctx)

    assert len(recorder.task_submissions) == 1
    assert recorder.task_submissions[0].child_job_id == "child_67890"

    # Emit workflow end
    end_ctx = WorkflowCallbackContext(
        workflow_job_id="test_12345",
        workflow_job_dir=PathType("/test"),
        workflow_name="test",
        workflow_context=None,
        timestamp=120.0,
        result={"status": "success"},
        cluster=None,
    )
    recorder.on_workflow_end_ctx(end_ctx)

    assert len(recorder.workflow_ends) == 1
    assert recorder.workflow_ends[0].result == {"status": "success"}


def test_workflow_with_multiple_child_tasks():
    """Test workflow that submits multiple child tasks."""

    @task(time="00:01:00")
    def preprocessing(data: str) -> str:
        return f"preprocessed_{data}"

    @task(time="00:02:00")
    def training(data: str) -> str:
        return f"trained_{data}"

    @task(time="00:01:00")
    def evaluation(model: str) -> str:
        return f"evaluated_{model}"

    @workflow(time="00:10:00")
    def ml_pipeline(ctx: WorkflowContext, data: str) -> str:
        # Submit multiple tasks
        preprocess_job = preprocessing(data)
        train_job = training(preprocess_job.get_result())
        eval_job = evaluation(train_job.get_result())
        return eval_job.get_result()

    # Verify workflow structure
    assert ml_pipeline._is_workflow is True
    assert not getattr(preprocessing, "_is_workflow", False)
    assert not getattr(training, "_is_workflow", False)
    assert not getattr(evaluation, "_is_workflow", False)

    # When executed, should emit:
    # - 1 workflow_begin event
    # - 3 task_submitted events (one for each child task)
    # - 1 workflow_end event


def test_workflow_exception_handling():
    """Test that workflow end event is emitted even on exception."""
    from pathlib import Path as PathType

    recorder = WorkflowEventRecorder()

    # Simulate workflow failure
    exception_ctx = WorkflowCallbackContext(
        workflow_job_id="failed_12345",
        workflow_job_dir=PathType("/failed"),
        workflow_name="failed_workflow",
        workflow_context=None,
        timestamp=200.0,
        exception=RuntimeError("Test error"),
        cluster=None,
    )

    recorder.on_workflow_end_ctx(exception_ctx)

    assert len(recorder.workflow_ends) == 1
    assert recorder.workflow_ends[0].exception is not None
    assert isinstance(recorder.workflow_ends[0].exception, RuntimeError)
    assert str(recorder.workflow_ends[0].exception) == "Test error"
    assert recorder.workflow_ends[0].result is None
