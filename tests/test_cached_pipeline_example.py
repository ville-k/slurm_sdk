"""Unit tests for the cache-aware custom decorator example."""

import json
from pathlib import Path

from slurm.cluster import Cluster
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.examples.cached_pipeline.decorators import cache_runtime
from slurm.examples.cached_pipeline.task import build_feature_summary
from slurm.examples.cached_pipeline.workflow import cached_replay_workflow
from slurm.workflow import WorkflowContext
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
    cluster.default_packaging_kwargs = {}
    cluster.default_account = None
    cluster.default_partition = None
    cluster._job_pollers = {}
    return cluster


def test_cache_runtime_reuses_submission_and_replays_from_cache(tmp_path: Path):
    _clear_active_context()
    cluster = create_mock_cluster(tmp_path)
    token = _set_active_context(cluster)
    cache_dir = tmp_path / "example-cache"

    try:
        with cache_runtime(cache_dir):
            first_ref = build_feature_summary(dataset="tiny-imagenet", epoch=3, scale=2)
            second_ref = build_feature_summary(
                dataset="tiny-imagenet",
                epoch=3,
                scale=2,
            )

            assert first_ref is second_ref
            assert first_ref.cache_hit is False
            assert first_ref.id is not None

            first_result = first_ref.get_result()
            second_result = second_ref.get_result()
            assert first_result == second_result

        with cache_runtime(cache_dir):
            replay_ref = build_feature_summary(
                dataset="tiny-imagenet", epoch=3, scale=2
            )
            changed_ref = build_feature_summary(
                dataset="tiny-imagenet", epoch=4, scale=2
            )

            assert replay_ref.cache_hit is True
            assert replay_ref.id is None
            assert changed_ref.cache_hit is False
            assert changed_ref.id is not None

            replay_result = replay_ref.get_result()
            changed_result = changed_ref.get_result()
            assert replay_result == first_result
            assert changed_result != first_result
    finally:
        _reset_active_context(token)


def run_cached_workflow(tmp_path: Path) -> Path:
    _clear_active_context()

    cluster = create_mock_cluster(tmp_path)
    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)

    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_cached_workflow",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )

    token = _set_active_context(ctx)
    try:
        state_path = cached_replay_workflow.unwrapped(
            dataset="tiny-imagenet",
            epoch=3,
            scale=2,
            partition_task=None,
            ctx=ctx,
        )
    finally:
        _reset_active_context(token)

    return Path(state_path)


def test_cached_workflow_example_generates_replay_state(tmp_path: Path) -> None:
    state_path = run_cached_workflow(tmp_path)
    summary = json.loads(state_path.read_text(encoding="utf-8"))

    assert state_path.name == "cache_state.json"
    assert summary["same_ref_within_runtime"] is True
    assert summary["replay_loaded_from_cache"] is True
    assert summary["results_equal"] is True
    assert summary["changed_result_differs"] is True

    assert summary["first_job_id"] is not None
    assert summary["second_job_id"] == summary["first_job_id"]
    assert summary["replay_job_id"] is None
    assert summary["changed_job_id"] is not None
    assert summary["changed_job_id"] != summary["first_job_id"]

    cache_files = summary["cache_files"]
    assert isinstance(cache_files, list)
    assert len(cache_files) == 2
