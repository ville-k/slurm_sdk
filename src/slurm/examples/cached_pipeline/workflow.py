from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from slurm.callbacks.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.logging import configure_logging
from slurm.workflow import WorkflowContext

from slurm.examples.cached_pipeline.decorators import (
    cache_runtime,
    checkpoint_workflow,
)
from slurm.examples.cached_pipeline.task import build_feature_summary


@checkpoint_workflow()
def cached_replay_workflow(
    dataset: str,
    epoch: int,
    scale: int,
    partition_task: Optional[str],
    ctx: WorkflowContext,
) -> str:
    """Demonstrate cache-aware task replay through custom decorators and runtime."""
    configure_logging()
    logger = logging.getLogger(__name__)

    workdir = Path(ctx.shared_dir)
    cache_dir = workdir / "cache"
    state_path = workdir / "cache_state.json"
    workdir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    task_impl = build_feature_summary
    if partition_task:
        task_impl = task_impl.with_options(partition=partition_task)

    with cache_runtime(cache_dir):
        first_ref = task_impl(dataset=dataset, epoch=epoch, scale=scale)
        second_ref = task_impl(dataset=dataset, epoch=epoch, scale=scale)
        first_result = first_ref.get_result()
        second_result = second_ref.get_result()

    with cache_runtime(cache_dir):
        replay_ref = task_impl(dataset=dataset, epoch=epoch, scale=scale)
        replay_result = replay_ref.get_result()

    with cache_runtime(cache_dir):
        changed_ref = task_impl(dataset=dataset, epoch=epoch + 1, scale=scale)
        changed_result = changed_ref.get_result()

    summary: Dict[str, Any] = {
        "workflow_job_id": ctx.workflow_job_id,
        "dataset": dataset,
        "epoch": epoch,
        "scale": scale,
        "cache_dir": str(cache_dir),
        "first_job_id": first_ref.id,
        "second_job_id": second_ref.id,
        "replay_job_id": replay_ref.id,
        "changed_job_id": changed_ref.id,
        "first_cache_hit": first_ref.cache_hit,
        "second_cache_hit": second_ref.cache_hit,
        "replay_cache_hit": replay_ref.cache_hit,
        "changed_cache_hit": changed_ref.cache_hit,
        "same_ref_within_runtime": (
            first_ref.key == second_ref.key and first_ref.id == second_ref.id
        ),
        "replay_loaded_from_cache": replay_ref.cache_hit and replay_ref.id is None,
        "results_equal": first_result == second_result == replay_result,
        "changed_result_differs": changed_result != first_result,
        "cache_files": sorted(path.name for path in cache_dir.glob("*.pkl")),
    }

    state_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    logger.info("Cache-aware workflow summary written to %s", state_path)
    return str(state_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Cache-aware custom decorator example that demonstrates "
            "idempotent replay behavior."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)
    parser.add_argument(
        "--dataset",
        default="tiny-imagenet",
        help="Dataset identifier used for cache key generation",
    )
    parser.add_argument(
        "--epoch",
        type=int,
        default=3,
        help="Epoch value used for deterministic feature summary",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=2,
        help="Scaling factor for the deterministic score",
    )
    parser.add_argument(
        "--partition-workflow",
        help="Optional partition override for the workflow orchestrator",
    )
    parser.add_argument(
        "--partition-task",
        help="Optional partition override for decorated tasks",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging()

    cluster = Cluster.from_args(args, callbacks=[LoggerCallback()])
    submit_overrides: dict[str, Any] = {}
    if args.partition_workflow:
        submit_overrides["partition"] = args.partition_workflow

    logger = logging.getLogger(__name__)
    submitter = cluster.submit(cached_replay_workflow, **submit_overrides)
    workflow_job = submitter(
        dataset=args.dataset,
        epoch=args.epoch,
        scale=args.scale,
        partition_task=args.partition_task,
    )
    logger.info("Workflow job submitted: %s", workflow_job.id)

    state_path = Path(workflow_job.get_result())
    state = json.loads(state_path.read_text(encoding="utf-8"))
    logger.info("Workflow complete. Cache state file: %s", state_path)
    logger.info("Replay cache hit: %s", state.get("replay_loaded_from_cache"))
    logger.info(
        "Single submission reused in runtime: %s", state.get("same_ref_within_runtime")
    )


if __name__ == "__main__":
    main()
