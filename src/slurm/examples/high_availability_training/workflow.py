from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from slurm.callbacks.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.logging import configure_logging
from slurm.workflow import WorkflowContext

from slurm.examples.high_availability_training.eval_task import evaluate_epoch
from slurm.examples.high_availability_training.ha.fluent import ha_runtime, ha_workflow
from slurm.examples.high_availability_training.ha.train_eval import (
    TrainEvalSupervisor,
    TrainEvalSupervisorConfig,
)
from slurm.examples.high_availability_training.train_task import train_chunk


@ha_workflow(time="00:15:00", mem="1G", cpus_per_task=1)
def high_availability_training_workflow(
    *,
    run_id: str,
    run_dir: str,
    epochs: int,
    epoch_steps: int,
    max_steps_per_chunk: int,
    max_train_attempts: int,
    max_eval_attempts: int,
    poll_interval_s: float,
    partition_train: Optional[str],
    partition_eval: Optional[str],
    resiliency_config: Optional[Mapping[str, Any]],
    ctx: WorkflowContext,
) -> str:
    """Run a high-availability train/eval supervisor loop against a stable run dir."""
    configure_logging()

    supervisor = TrainEvalSupervisor(
        config=TrainEvalSupervisorConfig(
            run_id=run_id,
            run_dir=Path(run_dir),
            epochs=epochs,
            epoch_steps=epoch_steps,
            max_steps_per_chunk=max_steps_per_chunk,
            max_train_attempts=max_train_attempts,
            max_eval_attempts=max_eval_attempts,
            poll_interval_s=poll_interval_s,
            partition_train=partition_train,
            partition_eval=partition_eval,
            resiliency_config=resiliency_config,
        ),
        train_task=train_chunk,
        eval_task=evaluate_epoch,
        ctx=ctx,
    )
    with ha_runtime():
        return str(supervisor.run())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="High-availability training + evaluation workflow example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    Cluster.add_argparse_args(parser)

    parser.add_argument("--run-id", default="ha_train_eval", help="Run identifier")
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Stable run directory on the cluster filesystem",
    )
    parser.add_argument("--epochs", type=int, default=3, help="Number of epochs")
    parser.add_argument(
        "--epoch-steps", type=int, default=10, help="Steps per epoch (mocked)"
    )
    parser.add_argument(
        "--max-steps-per-chunk",
        type=int,
        default=4,
        help="Maximum steps per train chunk job",
    )
    parser.add_argument(
        "--max-train-attempts",
        type=int,
        default=3,
        help="Max attempts per train chunk",
    )
    parser.add_argument(
        "--max-eval-attempts",
        type=int,
        default=2,
        help="Max attempts per eval epoch",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=0.2,
        help="Supervisor poll interval (seconds)",
    )
    parser.add_argument("--partition-workflow", help="Partition override for workflow")
    parser.add_argument("--partition-train", help="Partition override for train jobs")
    parser.add_argument("--partition-eval", help="Partition override for eval jobs")

    parser.add_argument(
        "--resiliency-enabled",
        action="store_true",
        help="Enable in-job resiliency (mock by default)",
    )
    parser.add_argument(
        "--resiliency-implementation",
        choices=["mock", "nvrx", "none"],
        default="none",
        help="In-job resiliency implementation",
    )
    parser.add_argument(
        "--resiliency-max-restarts",
        type=int,
        default=0,
        help="Max in-job restarts per allocation (mock implementation)",
    )
    parser.add_argument(
        "--resiliency-module",
        default="nvidia_resiliency_ext",
        help="NVRx Python module name (when using implementation=nvrx)",
    )
    parser.add_argument(
        "--resiliency-adapter",
        help=(
            "Project-specific resiliency adapter in 'module:function' form "
            "(when using implementation=nvrx)"
        ),
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging()
    logger = logging.getLogger(__name__)

    cluster = Cluster.from_args(args, callbacks=[LoggerCallback()])

    workflow_overrides: Dict[str, Any] = {}
    if args.partition_workflow:
        workflow_overrides["partition"] = args.partition_workflow

    resiliency_config: Dict[str, Any] = {
        "enabled": bool(args.resiliency_enabled),
        "implementation": args.resiliency_implementation,
        "max_restarts": int(args.resiliency_max_restarts),
    }
    if args.resiliency_module:
        resiliency_config["module"] = str(args.resiliency_module)
    if args.resiliency_adapter:
        resiliency_config["adapter"] = str(args.resiliency_adapter)

    submitter = cluster.submit(
        high_availability_training_workflow, **workflow_overrides
    )
    job = submitter(
        run_id=args.run_id,
        run_dir=args.run_dir,
        epochs=args.epochs,
        epoch_steps=args.epoch_steps,
        max_steps_per_chunk=args.max_steps_per_chunk,
        max_train_attempts=args.max_train_attempts,
        max_eval_attempts=args.max_eval_attempts,
        poll_interval_s=args.poll_interval_s,
        partition_train=args.partition_train,
        partition_eval=args.partition_eval,
        resiliency_config=resiliency_config,
    )

    logger.info("Workflow job submitted: %s", job.id)
    final_state_path = job.get_result()
    logger.info("Workflow complete. State: %s", final_state_path)


if __name__ == "__main__":
    main()
