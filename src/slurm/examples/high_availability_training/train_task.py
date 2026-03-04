from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from slurm.logging import configure_logging

from slurm.examples.high_availability_training.ha.fluent import ha_task
from slurm.examples.high_availability_training.ha.io import (
    append_jsonl,
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


def _load_checkpoint_steps(checkpoint_path: Optional[str]) -> Optional[int]:
    if not checkpoint_path:
        return None
    try:
        payload = read_json(Path(checkpoint_path))
        steps = payload.get("steps_completed")
        return int(steps) if steps is not None else None
    except Exception:
        return None


@ha_task(time="00:10:00", mem="1G", cpus_per_task=1, signal="B:USR1@60")
def train_chunk(
    *,
    run_id: str,
    run_dir: str,
    epoch: int,
    chunk: int,
    start_step: int,
    max_steps: int,
    epoch_steps: int,
    checkpoint_in: Optional[str],
    output_dir: str,
    train_config: Optional[Mapping[str, Any]] = None,
    signal_config: Optional[Mapping[str, Any]] = None,
    resiliency_config: Optional[Mapping[str, Any]] = None,
    failure_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Run a checkpointable training chunk and write artifacts under output_dir.

    This is a mocked training loop designed to demonstrate fault-tolerance wiring.
    A real implementation would run a framework training step loop (PyTorch, etc.)
    and write real checkpoints/metrics.
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    output_path = Path(output_dir).expanduser()
    paths = AttemptPaths(output_dir=output_path)

    fail_cfg: Dict[str, Any] = dict(failure_config or {})
    fail_mode = str(fail_cfg.get("mode") or "").lower()
    fail_at_step = fail_cfg.get("at_step")
    if fail_at_step is not None:
        try:
            fail_at_step = int(fail_at_step)
        except Exception:
            fail_at_step = None

    checkpoint_interval_steps = int(
        (train_config or {}).get("checkpoint_interval_steps", 1)
    )
    checkpoint_interval_steps = max(checkpoint_interval_steps, 1)

    initial_resume = _load_checkpoint_steps(checkpoint_in)
    resume_step = max(start_step, int(initial_resume or start_step))

    input_payload = {
        "run_id": run_id,
        "run_dir": run_dir,
        "epoch": epoch,
        "chunk": chunk,
        "start_step": start_step,
        "max_steps": max_steps,
        "epoch_steps": epoch_steps,
        "checkpoint_in": checkpoint_in,
        "output_dir": str(output_path),
        "train_config": dict(train_config or {}),
        "signal_config": dict(signal_config or {}),
        "resiliency_config": dict(resiliency_config or {}),
        "failure_config": fail_cfg,
        "started_at": now_iso(),
    }

    end_step_target = min(start_step + max_steps, epoch_steps)
    metrics_path = paths.metrics_jsonl_path

    def is_retryable_in_job(exc: BaseException) -> bool:
        return isinstance(exc, TransientError)

    def run_once(stop_requested: Callable[[], bool]) -> int:
        nonlocal resume_step

        if epoch < 0 or chunk < 0:
            raise UserError("epoch and chunk must be >= 0")
        if epoch_steps <= 0:
            raise UserError("epoch_steps must be > 0")
        if max_steps <= 0:
            raise UserError("max_steps must be > 0")

        progress_steps = resume_step
        if paths.progress_path.exists():
            try:
                progress_steps = int(
                    read_json(paths.progress_path).get("steps_completed")
                )
            except Exception:
                progress_steps = resume_step

        current_step = max(resume_step, progress_steps)
        if current_step > end_step_target:
            return current_step

        for step in range(current_step, end_step_target):
            if stop_requested():
                raise StopRequestedError()

            next_step = step + 1

            if fail_at_step is not None and next_step >= fail_at_step:
                if fail_mode == "crash":
                    os._exit(2)  # noqa: S606
                if fail_mode == "user":
                    raise UserError(f"Injected user error at step {next_step}")
                if fail_mode == "transient":
                    raise TransientError(
                        f"Injected transient error at step {next_step}"
                    )

            write_json_atomic(
                paths.progress_path,
                {
                    "epoch": epoch,
                    "chunk": chunk,
                    "steps_completed": next_step,
                    "updated_at": now_iso(),
                },
            )

            append_jsonl(
                metrics_path,
                {
                    "time": now_iso(),
                    "epoch": epoch,
                    "step": next_step,
                    "loss": round(1.0 / (1 + next_step), 6),
                },
            )

            if (
                next_step % checkpoint_interval_steps == 0
                or next_step == end_step_target
            ):
                write_json_atomic(
                    paths.checkpoint_out_path,
                    {
                        "run_id": run_id,
                        "epoch": epoch,
                        "chunk": chunk,
                        "steps_completed": next_step,
                        "epoch_steps": epoch_steps,
                        "checkpoint_in": checkpoint_in,
                        "written_at": now_iso(),
                    },
                )

            resume_step = next_step
            time.sleep(0.01)

        return resume_step

    result = run_task_attempt(
        output_dir=output_path,
        input_payload=input_payload,
        checkpoint_in=checkpoint_in,
        run_once=run_once,
        is_retryable_in_job=is_retryable_in_job,
        user_error_types=(UserError,),
        resiliency_config=resiliency_config,
        checkpoint_out_path=paths.checkpoint_out_path,
        metrics_path=paths.metrics_jsonl_path,
        success_summary=lambda final_step: (
            f"Completed train chunk {chunk} (epoch {epoch}) to step {final_step}"
        ),
        preempted_summary=lambda exc: (
            f"Preemption/timeout signal received; checkpointed at step {resume_step}"
        ),
        user_error_summary="Non-retryable training error",
        retryable_error_summary="Retryable training error",
    )
    logger.info("%s", result.get("status_summary"))
    return result


__all__ = [
    "TransientError",
    "UserError",
    "train_chunk",
]
