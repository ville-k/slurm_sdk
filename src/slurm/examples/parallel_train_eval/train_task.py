from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

from slurm.decorators import task
from slurm.logging import configure_logging

from slurm.examples.parallel_train_eval.state import write_json


@task(time="00:05:00", mem="256M", cpus_per_task=1)
def train_epoch_segment(
    epoch: int,
    start_step: int,
    steps_to_run: int,
    epoch_steps: int,
    workdir: str,
    job_index: int,
) -> Dict[str, Any]:
    """Run a capped training segment and write the epoch checkpoint.

    Args:
        epoch: Epoch index (0-based).
        start_step: Step offset at the start of this job.
        steps_to_run: Number of steps to execute in this job.
        epoch_steps: Total steps required for the epoch.
        workdir: Shared filesystem path for artifacts.
        job_index: Sequential train job index within the epoch.

    Returns:
        Dict containing the updated step count and checkpoint path.
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    if steps_to_run <= 0:
        raise ValueError("steps_to_run must be positive")

    remaining_steps = max(epoch_steps - start_step, 0)
    steps_this_job = min(steps_to_run, remaining_steps)
    end_step = start_step + steps_this_job

    workdir_path = Path(workdir).expanduser()
    checkpoint_path = workdir_path / "checkpoints" / f"epoch_{epoch:03d}.json"

    checkpoint_payload = {
        "epoch": epoch,
        "job_index": job_index,
        "start_step": start_step,
        "steps_this_job": steps_this_job,
        "steps_completed": end_step,
        "epoch_steps": epoch_steps,
    }
    write_json(checkpoint_path, checkpoint_payload)

    logger.info(
        "Epoch %s train job %s completed steps %s-%s",
        epoch,
        job_index,
        start_step,
        end_step,
    )
    logger.info("Checkpoint written to %s", checkpoint_path)

    return {
        "epoch": epoch,
        "steps_completed": end_step,
        "checkpoint_path": str(checkpoint_path),
    }
