from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

from slurm.decorators import task
from slurm.logging import configure_logging

from slurm.examples.parallel_train_eval.state import read_json, write_json


@task(time="00:03:00", mem="256M", cpus_per_task=1)
def evaluate_epoch(epoch: int, checkpoint_path: str, workdir: str) -> Dict[str, Any]:
    """Evaluate a checkpoint and write metrics for the epoch.

    Args:
        epoch: Epoch index (0-based).
        checkpoint_path: Path to the checkpoint JSON.
        workdir: Shared filesystem path for artifacts.

    Returns:
        Dict containing the metrics path.
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    checkpoint_file = Path(checkpoint_path)
    checkpoint_payload = read_json(checkpoint_file)

    metrics_payload = {
        "epoch": epoch,
        "checkpoint_path": str(checkpoint_file),
        "steps_completed": checkpoint_payload.get("steps_completed"),
        "accuracy": round(0.75 + (epoch * 0.01), 4),
        "loss": round(1.0 / (1 + epoch + 1), 4),
    }

    metrics_path = (
        Path(workdir).expanduser() / "metrics" / f"epoch_{epoch:03d}.json"
    )
    write_json(metrics_path, metrics_payload)

    logger.info("Epoch %s eval metrics written to %s", epoch, metrics_path)

    return {"metrics_path": str(metrics_path)}


__all__ = ["evaluate_epoch"]
