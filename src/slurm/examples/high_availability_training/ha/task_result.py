from __future__ import annotations

import os
import socket
import traceback
from enum import Enum
from typing import Any, Dict, Mapping, Optional

from slurm.examples.high_availability_training.ha.io import now_iso


class StatusCode(str, Enum):
    SUCCESS = "SUCCESS"
    RETRYABLE_ERROR = "RETRYABLE_ERROR"
    NONRETRYABLE_ERROR = "NONRETRYABLE_ERROR"
    PREEMPTED = "PREEMPTED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


def slurm_metadata_from_env() -> Dict[str, Optional[str]]:
    return {
        "job_id": os.environ.get("SLURM_JOB_ID"),
        "array_job_id": os.environ.get("SLURM_ARRAY_JOB_ID"),
        "array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
        "hostname": os.environ.get("HOSTNAME") or socket.gethostname(),
    }


def exception_details(exc: BaseException) -> Dict[str, str]:
    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "traceback": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ),
    }


def make_task_result(
    *,
    status_code: str,
    status_summary: str,
    output_dir: str,
    checkpoint_out: Optional[str] = None,
    metrics_path: Optional[str] = None,
    events_path: Optional[str] = None,
    error_class: Optional[str] = None,
    error: Optional[Mapping[str, str]] = None,
    nvrx: Optional[Mapping[str, Any]] = None,
    debug: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the standard JSON-serializable TaskResult payload."""
    result_details: Dict[str, Any] = {
        "output_dir": output_dir,
        "checkpoint_out": checkpoint_out,
        "metrics_path": metrics_path,
        "events_path": events_path,
        "slurm": slurm_metadata_from_env(),
        "nvrx": dict(nvrx) if nvrx is not None else {"enabled": False},
    }

    if error_class is not None:
        result_details["error_class"] = error_class
    if error is not None:
        result_details.update(dict(error))
    if debug is not None:
        result_details["debug"] = dict(debug)

    return {
        "schema_version": 1,
        "status_code": status_code,
        "status_summary": status_summary,
        "created_at": now_iso(),
        "result_details": result_details,
    }


__all__ = [
    "StatusCode",
    "exception_details",
    "make_task_result",
    "slurm_metadata_from_env",
]
