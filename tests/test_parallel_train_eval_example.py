import json
from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.workflow import WorkflowContext

from slurm.examples.parallel_train_eval.workflow import parallel_train_eval_workflow
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
    cluster.default_packaging = None
    cluster.default_account = None
    cluster.default_partition = None
    return cluster


def run_workflow(tmp_path: Path, epochs: int, epoch_steps: int, cap: int) -> Path:
    """Run the workflow in a local/dry-run context and return the state path."""
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_workflow",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )

    token = _set_active_context(ctx)
    try:
        state_path = parallel_train_eval_workflow.unwrapped(
            epochs=epochs,
            epoch_steps=epoch_steps,
            steps_per_job_cap=cap,
            partition_train=None,
            partition_eval=None,
            ctx=ctx,
        )
    finally:
        _reset_active_context(token)

    state_path = Path(state_path)
    assert state_path.parent == shared_dir
    return state_path


def test_parallel_train_eval_workflow_artifacts(tmp_path: Path) -> None:
    """Validate workflow progress, artifacts, and non-blocking eval behavior."""
    epochs = 2
    epoch_steps = 5
    cap = 3

    state_path = run_workflow(tmp_path, epochs=epochs, epoch_steps=epoch_steps, cap=cap)
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert state["config"]["epochs"] == epochs
    assert state["config"]["epoch_steps"] == epoch_steps
    assert state["config"]["steps_per_job_cap"] == cap

    assert len(state["epochs"]) == epochs

    multiple_train_jobs = False
    for epoch_key, epoch_state in state["epochs"].items():
        assert epoch_state["steps_completed"] == epoch_steps
        assert epoch_state["checkpoint_path"] is not None
        assert epoch_state["metrics_path"] is not None
        assert epoch_state["eval_job_id"] is not None

        train_job_ids = epoch_state["train_job_ids"]
        if len(train_job_ids) > 1:
            multiple_train_jobs = True

        checkpoint_path = Path(epoch_state["checkpoint_path"])
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        assert checkpoint["steps_completed"] == epoch_steps

        metrics_path = Path(epoch_state["metrics_path"])
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert metrics["checkpoint_path"] == str(checkpoint_path)
        assert metrics["epoch"] == int(epoch_key)

    assert multiple_train_jobs is True

    for epoch in range(1, epochs):
        prev_state = state["epochs"][str(epoch - 1)]
        next_state = state["epochs"][str(epoch)]
        assert prev_state["eval_submitted_order"] is not None
        assert next_state["epoch_started_order"] is not None
        assert prev_state["eval_submitted_order"] < next_state["epoch_started_order"]
        if prev_state["eval_completed_order"] is not None:
            assert (
                next_state["epoch_started_order"] < prev_state["eval_completed_order"]
            )
