from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from slurm.logging import configure_logging

from slurm.examples.high_availability_training.ha.fluent import ha_task
from slurm.examples.high_availability_training.ha.io import (
    now_iso,
    read_json,
    write_json_atomic,
)
from slurm.examples.high_availability_training.ha.paths import AttemptPaths
from slurm.examples.high_availability_training.ha.task_attempt import (
    StopRequestedError,
    run_task_attempt,
)


class UserError(ValueError):
    """Non-retryable error caused by user input/configuration."""


class TransientError(RuntimeError):
    """Retryable error that can be restarted inside a job attempt."""


@ha_task(time="00:05:00", mem="1G", cpus_per_task=1, signal="B:USR1@60")
def evaluate_epoch(
    *,
    run_id: str,
    run_dir: str,
    epoch: int,
    checkpoint_in: str,
    output_dir: str,
    eval_config: Optional[Mapping[str, Any]] = None,
    signal_config: Optional[Mapping[str, Any]] = None,
    resiliency_config: Optional[Mapping[str, Any]] = None,
    failure_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate a checkpoint and write metrics under output_dir (mocked)."""
    configure_logging()
    logger = logging.getLogger(__name__)

    output_path = Path(output_dir).expanduser()
    paths = AttemptPaths(output_dir=output_path)

    fail_cfg: Dict[str, Any] = dict(failure_config or {})
    fail_mode = str(fail_cfg.get("mode") or "").lower()

    input_payload = {
        "run_id": run_id,
        "run_dir": run_dir,
        "epoch": epoch,
        "checkpoint_in": checkpoint_in,
        "output_dir": str(output_path),
        "eval_config": dict(eval_config or {}),
        "signal_config": dict(signal_config or {}),
        "resiliency_config": dict(resiliency_config or {}),
        "failure_config": fail_cfg,
        "started_at": now_iso(),
    }

    def is_retryable_in_job(exc: BaseException) -> bool:
        return isinstance(exc, TransientError)

    def run_once(stop_requested: Callable[[], bool]) -> Dict[str, Any]:
        if stop_requested():
            raise StopRequestedError()

        if epoch < 0:
            raise UserError("epoch must be >= 0")

        if fail_mode == "user":
            raise UserError("Injected user error during evaluation")
        if fail_mode == "transient":
            raise TransientError("Injected transient error during evaluation")

        checkpoint_payload = read_json(Path(checkpoint_in))
        steps_completed = int(checkpoint_payload.get("steps_completed", 0))
        metrics_payload = {
            "run_id": run_id,
            "epoch": epoch,
            "checkpoint_path": checkpoint_in,
            "steps_completed": steps_completed,
            "accuracy": round(0.70 + (epoch * 0.01), 4),
            "loss": round(1.0 / (1 + epoch + 1), 6),
            "evaluated_at": now_iso(),
        }
        write_json_atomic(
            paths.progress_path, {"stage": "complete", "updated_at": now_iso()}
        )
        write_json_atomic(paths.metrics_json_path, metrics_payload)
        return metrics_payload

    result = run_task_attempt(
        output_dir=output_path,
        input_payload=input_payload,
        checkpoint_in=checkpoint_in,
        run_once=run_once,
        is_retryable_in_job=is_retryable_in_job,
        user_error_types=(UserError,),
        resiliency_config=resiliency_config,
        metrics_path=paths.metrics_json_path,
        success_summary=f"Completed evaluation for epoch {epoch}",
        preempted_summary="Preemption/timeout signal received during evaluation",
        user_error_summary="Non-retryable evaluation error",
        retryable_error_summary="Retryable evaluation error",
    )
    logger.info("%s", result.get("status_summary"))
    return result


__all__ = [
    "TransientError",
    "UserError",
    "evaluate_epoch",
]
