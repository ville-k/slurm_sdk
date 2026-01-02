from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def init_state(
    epochs: int,
    epoch_steps: int,
    steps_per_job_cap: int,
    workdir: str,
    workflow_job_id: str,
) -> Dict[str, Any]:
    return {
        "config": {
            "epochs": epochs,
            "epoch_steps": epoch_steps,
            "steps_per_job_cap": steps_per_job_cap,
            "workdir": workdir,
        },
        "workflow_job_id": workflow_job_id,
        "current_epoch": 0,
        "steps_completed": 0,
        "epochs": {},
    }


def ensure_epoch_state(state: Dict[str, Any], epoch: int) -> Dict[str, Any]:
    key = str(epoch)
    if key not in state["epochs"]:
        state["epochs"][key] = {
            "epoch": epoch,
            "steps_completed": 0,
            "checkpoint_path": None,
            "metrics_path": None,
            "train_job_ids": [],
            "eval_job_id": None,
            "eval_completed": False,
            "epoch_started_order": None,
            "eval_submitted_order": None,
            "eval_completed_order": None,
        }
    return state["epochs"][key]


__all__ = [
    "write_json",
    "read_json",
    "init_state",
    "ensure_epoch_state",
]
