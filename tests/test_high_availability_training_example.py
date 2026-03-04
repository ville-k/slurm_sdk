import json
from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.workflow import WorkflowContext

from slurm.examples.high_availability_training.workflow import (
    high_availability_training_workflow,
)
from local_backend import LocalBackend  # type: ignore


def create_mock_cluster(tmp_path: Path) -> Cluster:
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


def run_workflow(tmp_path: Path, *, run_dir: Path) -> Path:
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
        state_path = high_availability_training_workflow.unwrapped(
            run_id="test_run",
            run_dir=str(run_dir),
            epochs=2,
            epoch_steps=5,
            max_steps_per_chunk=3,
            max_train_attempts=3,
            max_eval_attempts=2,
            poll_interval_s=0.0,
            partition_train=None,
            partition_eval=None,
            resiliency_config={"enabled": False, "implementation": "none"},
            ctx=ctx,
        )
    finally:
        _reset_active_context(token)

    return Path(state_path)


def test_high_availability_workflow_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    state_path = run_workflow(tmp_path, run_dir=run_dir)

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["completed"] is True
    assert state["failed"] is False

    assert (run_dir / "config" / "run_config.json").exists()
    assert (run_dir / "exports" / "latest_checkpoint.json").exists()

    train_results = list(run_dir.glob("train/epoch_*/chunk_*/attempt_*/result.json"))
    eval_results = list(run_dir.glob("eval/epoch_*/attempt_*/result.json"))

    assert len(train_results) > 1
    assert len(eval_results) == 2

    for result_path in train_results:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["status_code"] == "SUCCESS"
        checkpoint_out = payload["result_details"]["checkpoint_out"]
        assert Path(checkpoint_out).exists()

    for result_path in eval_results:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["status_code"] == "SUCCESS"
        metrics_path = payload["result_details"]["metrics_path"]
        assert Path(metrics_path).exists()

    # Re-running should be a no-op once the run is complete.
    train_count_before = len(
        list(run_dir.glob("train/epoch_*/chunk_*/attempt_*/result.json"))
    )
    state_path_2 = run_workflow(tmp_path, run_dir=run_dir)
    assert state_path_2 == state_path
    train_count_after = len(
        list(run_dir.glob("train/epoch_*/chunk_*/attempt_*/result.json"))
    )
    assert train_count_after == train_count_before
