"""Unit tests for context management (contextvars-based)."""

from slurm.context import (
    get_active_context,
    set_active_context,
    reset_active_context,
    clear_active_context,
)
from slurm.cluster import Cluster
from slurm.workflow import WorkflowContext


def test_no_active_context():
    """Test that no context is active by default."""
    clear_active_context()
    assert get_active_context() is None


def test_set_and_get_context(tmp_path):
    """Test setting and getting active context."""
    clear_active_context()

    # Create a mock cluster
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)

    # Set context
    token = set_active_context(cluster)

    # Get context
    ctx = get_active_context()
    assert ctx is cluster

    # Reset context
    reset_active_context(token)
    assert get_active_context() is None


def test_nested_contexts(tmp_path):
    """Test nested context scoping."""
    clear_active_context()

    # Create two mock clusters
    cluster1 = object.__new__(Cluster)
    cluster1.job_base_dir = str(tmp_path / "cluster1")

    cluster2 = object.__new__(Cluster)
    cluster2.job_base_dir = str(tmp_path / "cluster2")

    # Set outer context
    token1 = set_active_context(cluster1)
    assert get_active_context() is cluster1

    # Set inner context
    token2 = set_active_context(cluster2)
    assert get_active_context() is cluster2

    # Reset inner context - should restore outer
    reset_active_context(token2)
    assert get_active_context() is cluster1

    # Reset outer context
    reset_active_context(token1)
    assert get_active_context() is None


def test_workflow_context_as_active_context(tmp_path):
    """Test that WorkflowContext can be set as active context."""
    clear_active_context()

    # Create mock cluster and workflow context
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=tmp_path / "workflow",
        shared_dir=tmp_path / "workflow" / "shared",
        local_mode=False,
    )

    # Set workflow context as active
    token = set_active_context(workflow_ctx)

    # Get context
    ctx = get_active_context()
    assert ctx is workflow_ctx
    assert hasattr(ctx, "cluster")
    assert ctx.cluster is cluster

    # Reset
    reset_active_context(token)
    assert get_active_context() is None


def test_clear_context(tmp_path):
    """Test clearing context."""
    clear_active_context()

    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)

    # Set context
    set_active_context(cluster)
    assert get_active_context() is not None

    # Clear
    clear_active_context()
    assert get_active_context() is None


def test_context_isolation_across_threads(tmp_path):
    """Test that contexts are isolated across threads.

    Note: contextvars don't automatically inherit to new threads.
    Threads start with a clean context unless explicitly set.
    """
    import threading

    clear_active_context()

    cluster_main = object.__new__(Cluster)
    cluster_main.job_base_dir = str(tmp_path / "main")

    cluster_thread = object.__new__(Cluster)
    cluster_thread.job_base_dir = str(tmp_path / "thread")

    # Set context in main thread
    token_main = set_active_context(cluster_main)

    results = {}

    def thread_func():
        # Thread starts with no context (contextvars don't auto-inherit to new threads)
        results["inherited"] = get_active_context()

        # Set different context in thread
        token = set_active_context(cluster_thread)
        results["thread"] = get_active_context()

        reset_active_context(token)
        results["after_reset"] = get_active_context()

    thread = threading.Thread(target=thread_func)
    thread.start()
    thread.join()

    # Main thread context unchanged
    assert get_active_context() is cluster_main

    # Thread starts with no context
    assert results["inherited"] is None

    # Thread set its own context
    assert results["thread"] is cluster_thread

    # Thread reset restored no context
    assert results["after_reset"] is None

    # Cleanup
    reset_active_context(token_main)
