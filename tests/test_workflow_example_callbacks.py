"""Tests for BenchmarkCallback workflow features."""

import sys
from pathlib import Path

from slurm.callbacks import (
    BenchmarkCallback,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
    CompletedContext,
)

# Allow importing example
EXAMPLES_DIR = Path(__file__).parent.parent / "src" / "slurm" / "examples"
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_DIR))

from workflow_graph_visualization import WorkflowGraphCallback

# Allow importing test helpers
HELPERS_DIR = Path(__file__).parent / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))


def test_benchmark_callback_workflow_tracking():
    """Test BenchmarkCallback tracks workflow metrics."""
    callback = BenchmarkCallback()

    # Start workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    assert "wf_12345" in callback._workflows
    assert callback._workflows["wf_12345"]["name"] == "test_workflow"
    assert callback._workflows["wf_12345"]["child_count"] == 0


def test_benchmark_callback_tracks_child_submissions():
    """Test BenchmarkCallback tracks child task submissions."""
    callback = BenchmarkCallback()

    # Start workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    # Submit children
    submit_ctx1 = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="test_workflow",
        child_job_id="child_1",
        child_job_dir=Path("/test/child1"),
        child_task_name="task1",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx1)

    submit_ctx2 = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="test_workflow",
        child_job_id="child_2",
        child_job_dir=Path("/test/child2"),
        child_task_name="task2",
        child_is_workflow=False,
        timestamp=120.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx2)

    assert callback._workflows["wf_12345"]["child_count"] == 2
    assert len(callback._workflows["wf_12345"]["submission_times"]) == 2
    assert callback._child_to_parent["child_1"] == "wf_12345"
    assert callback._child_to_parent["child_2"] == "wf_12345"


def test_benchmark_callback_calculates_metrics():
    """Test BenchmarkCallback calculates workflow metrics."""
    callback = BenchmarkCallback()

    # Setup workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    # Submit two children
    for i in range(2):
        submit_ctx = WorkflowTaskSubmitContext(
            parent_workflow_id="wf_12345",
            parent_workflow_dir=Path("/test/wf"),
            parent_workflow_name="test_workflow",
            child_job_id=f"child_{i}",
            child_job_dir=Path(f"/test/child{i}"),
            child_task_name=f"task{i}",
            child_is_workflow=False,
            timestamp=100.0 + i * 10.0,
            cluster=None,  # type: ignore
        )
        callback.on_workflow_task_submitted_ctx(submit_ctx)

    # Complete workflow
    end_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=150.0,
        cluster=None,
    )
    callback.on_workflow_end_ctx(end_ctx)

    # Get metrics
    metrics = callback.get_workflow_metrics("wf_12345")
    assert metrics is not None
    assert metrics["name"] == "test_workflow"
    assert metrics["child_count"] == 2
    assert metrics["duration_seconds"] == 50.0
    assert "orchestration_overhead_ms" in metrics
    assert metrics["orchestration_overhead_ms"] == 10000.0  # 10 seconds in ms


def test_benchmark_callback_get_metrics():
    """Test BenchmarkCallback get_workflow_metrics method."""
    callback = BenchmarkCallback()

    # Start and end workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    end_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=200.0,
        cluster=None,
    )
    callback.on_workflow_end_ctx(end_ctx)

    metrics = callback.get_workflow_metrics("wf_12345")
    assert metrics is not None
    assert metrics["name"] == "test_workflow"
    assert metrics["duration_seconds"] == 100.0

    # Non-existent workflow
    assert callback.get_workflow_metrics("nonexistent") is None


def test_benchmark_callback_child_duration_tracking():
    """Test BenchmarkCallback tracks child task durations."""
    callback = BenchmarkCallback()

    # Setup workflow with child
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="test_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="test_workflow",
        child_job_id="child_1",
        child_job_dir=Path("/test/child1"),
        child_task_name="task1",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx)

    # Complete child
    completed_ctx = CompletedContext(
        job=None,
        job_id="child_1",
        job_dir="/test/child1",
        job_state="COMPLETED",
        exit_code="0:0",
        reason=None,
        stdout_path=None,
        stderr_path=None,
        start_time=110.0,
        end_time=120.0,
        duration=10.0,
        status={"JobState": "COMPLETED"},
    )
    callback.on_completed_ctx(completed_ctx)

    assert callback._workflows["wf_12345"]["completed_count"] == 1
    assert len(callback._workflows["wf_12345"]["child_durations"]) == 1
    assert callback._workflows["wf_12345"]["child_durations"][0] == 10.0


def test_workflow_graph_callback_initialization():
    """Test WorkflowGraphCallback can be initialized."""
    callback = WorkflowGraphCallback()
    assert callback.graph == {}
    assert callback.submission_order == []


def test_workflow_graph_callback_builds_graph():
    """Test WorkflowGraphCallback builds dependency graph."""
    callback = WorkflowGraphCallback()

    # Start workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="pipeline",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    assert "wf_12345" in callback.graph
    assert callback.graph["wf_12345"]["name"] == "pipeline"
    assert callback.graph["wf_12345"]["type"] == "workflow"

    # Submit child tasks
    submit_ctx1 = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="pipeline",
        child_job_id="child_1",
        child_job_dir=Path("/test/child1"),
        child_task_name="preprocess",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx1)

    assert len(callback.graph["wf_12345"]["children"]) == 1
    assert "child_1" in callback.graph
    assert callback.graph["child_1"]["type"] == "task"

    submit_ctx2 = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="pipeline",
        child_job_id="child_2",
        child_job_dir=Path("/test/child2"),
        child_task_name="train",
        child_is_workflow=False,
        timestamp=120.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx2)

    assert len(callback.graph["wf_12345"]["children"]) == 2


def test_workflow_graph_callback_export_dot():
    """Test WorkflowGraphCallback exports to DOT format."""
    callback = WorkflowGraphCallback()

    # Build simple graph
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="pipeline",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    submit_ctx = WorkflowTaskSubmitContext(
        parent_workflow_id="wf_12345",
        parent_workflow_dir=Path("/test/wf"),
        parent_workflow_name="pipeline",
        child_job_id="child_1",
        child_job_dir=Path("/test/child1"),
        child_task_name="task1",
        child_is_workflow=False,
        timestamp=110.0,
        cluster=None,  # type: ignore
    )
    callback.on_workflow_task_submitted_ctx(submit_ctx)

    # Export to DOT
    dot = callback.export_dot()
    assert "digraph workflow" in dot
    assert "pipeline" in dot
    assert "task1" in dot
    assert "->" in dot  # Has edges


def test_workflow_graph_callback_export_json():
    """Test WorkflowGraphCallback exports to JSON format."""
    callback = WorkflowGraphCallback()

    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_12345",
        workflow_job_dir=Path("/test/wf"),
        workflow_name="pipeline",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    # Export to JSON
    json_str = callback.export_json()
    assert "graph" in json_str
    assert "submission_order" in json_str
    assert "pipeline" in json_str


def test_workflow_graph_callback_get_roots():
    """Test WorkflowGraphCallback identifies root workflows."""
    callback = WorkflowGraphCallback()

    # Create root workflow
    begin_ctx = WorkflowCallbackContext(
        workflow_job_id="wf_root",
        workflow_job_dir=Path("/test/root"),
        workflow_name="root_workflow",
        workflow_context=None,
        timestamp=100.0,
        cluster=None,
    )
    callback.on_workflow_begin_ctx(begin_ctx)

    roots = callback.get_workflow_roots()
    assert len(roots) == 1
    assert "wf_root" in roots


def test_all_example_callbacks_have_required_methods():
    """Test that all example callbacks implement required methods."""
    callbacks = [
        WorkflowGraphCallback(),
        BenchmarkCallback(),
    ]

    for callback in callbacks:
        assert hasattr(callback, "on_workflow_begin_ctx")
        assert hasattr(callback, "on_workflow_end_ctx")
        assert hasattr(callback, "on_workflow_task_submitted_ctx")
        assert callable(callback.on_workflow_begin_ctx)
        assert callable(callback.on_workflow_end_ctx)
        assert callable(callback.on_workflow_task_submitted_ctx)
