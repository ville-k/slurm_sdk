from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, List, Optional

from slurm.callbacks.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import workflow
from slurm.logging import configure_logging
from slurm.workflow import WorkflowContext

from slurm.examples.parallel_train_eval.eval_task import evaluate_epoch
from slurm.examples.parallel_train_eval.state import (
    ensure_epoch_state,
    init_state,
    write_json,
)
from slurm.examples.parallel_train_eval.train_task import train_epoch_segment


@workflow(time="00:10:00", mem="512M", cpus_per_task=1)
def parallel_train_eval_workflow(
    epochs: int,
    epoch_steps: int,
    steps_per_job_cap: int,
    partition_train: Optional[str],
    partition_eval: Optional[str],
    ctx: WorkflowContext,
) -> str:
    """Run a parallel train + eval workflow with capped train jobs.

    Args:
        epochs: Total number of epochs to run.
        epoch_steps: Total steps per epoch.
        steps_per_job_cap: Maximum steps per train job.
        partition_train: Optional partition override for training jobs.
        partition_eval: Optional partition override for eval jobs.
        ctx: WorkflowContext injected by the runner.

    Returns:
        Path to the final state JSON (stored under ctx.shared_dir).
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    if epochs < 1:
        raise ValueError("epochs must be >= 1")
    if epoch_steps < 1:
        raise ValueError("epoch_steps must be >= 1")
    if steps_per_job_cap < 1:
        raise ValueError("steps_per_job_cap must be >= 1")

    workdir_path = Path(ctx.shared_dir)
    workdir_path.mkdir(parents=True, exist_ok=True)
    (workdir_path / "checkpoints").mkdir(parents=True, exist_ok=True)
    (workdir_path / "metrics").mkdir(parents=True, exist_ok=True)
    logger.info("Using shared workdir at %s", workdir_path)

    state_path = workdir_path / "state.json"
    state = init_state(
        epochs=epochs,
        epoch_steps=epoch_steps,
        steps_per_job_cap=steps_per_job_cap,
        workdir=str(workdir_path),
        workflow_job_id=ctx.workflow_job_id,
    )
    write_json(state_path, state)
    logger.info("State initialized at %s", state_path)

    eval_jobs: List[tuple[int, Any]] = []
    order_counter = 0

    for epoch in range(epochs):
        order_counter += 1
        state["current_epoch"] = epoch
        state["steps_completed"] = 0

        epoch_state = ensure_epoch_state(state, epoch)
        epoch_state["epoch_started_order"] = order_counter
        write_json(state_path, state)

        steps_completed = 0
        job_index = 0
        last_train_job = None
        checkpoint_path = None

        while steps_completed < epoch_steps:
            job_index += 1
            steps_this_job = min(steps_per_job_cap, epoch_steps - steps_completed)

            train_task = train_epoch_segment
            if partition_train:
                train_task = train_task.with_options(partition=partition_train)

            train_job = train_task(
                epoch=epoch,
                start_step=steps_completed,
                steps_to_run=steps_this_job,
                epoch_steps=epoch_steps,
                workdir=str(workdir_path),
                job_index=job_index,
            )
            last_train_job = train_job
            epoch_state["train_job_ids"].append(train_job.id)

            logger.info(
                "Submitted train job %s for epoch %s (job %s)",
                train_job.id,
                epoch,
                job_index,
            )
            write_json(state_path, state)

            train_result = train_job.get_result()
            steps_completed = int(train_result["steps_completed"])
            checkpoint_path = str(train_result["checkpoint_path"])

            epoch_state["steps_completed"] = steps_completed
            epoch_state["checkpoint_path"] = checkpoint_path
            state["steps_completed"] = steps_completed
            write_json(state_path, state)

            logger.info(
                "Epoch %s progress: %s/%s steps", epoch, steps_completed, epoch_steps
            )

        if last_train_job is None or checkpoint_path is None:
            raise RuntimeError("Training produced no checkpoint for epoch")

        eval_task = evaluate_epoch
        if partition_eval:
            eval_task = eval_task.with_options(partition=partition_eval)

        eval_job = eval_task.after(last_train_job)(
            epoch=epoch,
            checkpoint_path=checkpoint_path,
            workdir=str(workdir_path),
        )
        order_counter += 1
        epoch_state["eval_job_id"] = eval_job.id
        epoch_state["eval_submitted_order"] = order_counter
        epoch_state["metrics_path"] = str(
            workdir_path / "metrics" / f"epoch_{epoch:03d}.json"
        )
        write_json(state_path, state)

        logger.info("Submitted eval job %s for epoch %s", eval_job.id, epoch)
        eval_jobs.append((epoch, eval_job))

    write_json(state_path, state)

    for epoch, eval_job in eval_jobs:
        logger.info("Waiting for eval job %s (epoch %s)", eval_job.id, epoch)
        eval_job.wait()
        epoch_state = ensure_epoch_state(state, epoch)
        epoch_state["eval_completed"] = True
        order_counter += 1
        epoch_state["eval_completed_order"] = order_counter
        write_json(state_path, state)

    write_json(state_path, state)
    logger.info("Workflow completed. State saved at %s", state_path)

    return str(state_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parallel train + eval workflow with capped train jobs and "
            "fire-and-forget evaluation."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)

    parser.add_argument(
        "--epochs",
        type=int,
        default=3,
        help="Number of epochs to run",
    )
    parser.add_argument(
        "--epoch-steps",
        type=int,
        default=10,
        help="Total steps per epoch",
    )
    parser.add_argument(
        "--steps-per-job-cap",
        type=int,
        default=4,
        help="Maximum steps per train job",
    )
    parser.add_argument(
        "--partition-workflow",
        help="Partition override for the workflow orchestrator",
    )
    parser.add_argument(
        "--partition-train",
        help="Partition override for train jobs",
    )
    parser.add_argument(
        "--partition-eval",
        help="Partition override for eval jobs",
    )

    return parser


def main() -> None:
    """Run the parallel train + eval workflow example."""
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging()

    cluster = Cluster.from_args(args, callbacks=[LoggerCallback()])

    submit_overrides = {}
    if args.partition_workflow:
        submit_overrides["partition"] = args.partition_workflow

    logger = logging.getLogger(__name__)
    logger.info("Submitting parallel train + eval workflow")

    submitter = cluster.submit(parallel_train_eval_workflow, **submit_overrides)
    job = submitter(
        epochs=args.epochs,
        epoch_steps=args.epoch_steps,
        steps_per_job_cap=args.steps_per_job_cap,
        partition_train=args.partition_train,
        partition_eval=args.partition_eval,
    )

    logger.info("Workflow job submitted: %s", job.id)
    result_path = job.get_result()

    logger.info("Workflow complete. State file: %s", result_path)


if __name__ == "__main__":
    main()
