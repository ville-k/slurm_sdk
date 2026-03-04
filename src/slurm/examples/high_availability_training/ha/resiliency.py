from __future__ import annotations

import importlib
import importlib.util
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from slurm.examples.high_availability_training.ha.io import (
    append_jsonl,
    now_iso,
    write_json_atomic,
)


class ResiliencyUnavailableError(RuntimeError):
    """Raised when resiliency is requested but the implementation is unavailable."""


class InJobStopRequestedError(RuntimeError):
    """Raised when the outer supervisor requests a stop (signals/time budget)."""


@dataclass
class ResiliencySummary:
    enabled: bool
    implementation: str
    restarts: int
    max_restarts: int
    last_error_type: Optional[str] = None
    last_error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "implementation": self.implementation,
            "restarts": self.restarts,
            "max_restarts": self.max_restarts,
            "last_error_type": self.last_error_type,
            "last_error_message": self.last_error_message,
            "updated_at": now_iso(),
        }


def _bool(value: Any) -> bool:
    return bool(value) and str(value).lower() not in {"0", "false", "no", "off"}


def run_with_resiliency(
    *,
    output_dir: Path,
    resiliency_config: Optional[Mapping[str, Any]],
    run_once: Callable[[], Any],
    is_retryable_in_job: Callable[[BaseException], bool],
    stop_requested: Callable[[], bool],
) -> tuple[Any, Mapping[str, Any]]:
    """Run `run_once` with an optional in-job restart loop.

    This is a lightweight, dependency-free stand-in for NVRx wiring:
    - `implementation=mock` retries retryable exceptions within the same allocation.
    - `implementation=nvrx` validates the NVRx import and then raises until a concrete
      adapter is provided (keeps the hook explicit and safe).
    """
    cfg: Dict[str, Any] = dict(resiliency_config or {})
    enabled = _bool(cfg.get("enabled"))
    implementation = str(cfg.get("implementation") or "none").strip().lower()

    nvrx_dir = output_dir / "nvrx"
    events_path = nvrx_dir / "events.jsonl"
    summary_path = nvrx_dir / "summary.json"
    nvrx_dir.mkdir(parents=True, exist_ok=True)

    if not enabled or implementation in {"none", "noop"}:
        summary = ResiliencySummary(
            enabled=False,
            implementation="none",
            restarts=0,
            max_restarts=0,
        )
        write_json_atomic(summary_path, summary.to_dict())
        return run_once(), {
            "enabled": False,
            "implementation": "none",
            "restarts": 0,
            "events_path": str(events_path),
            "summary_path": str(summary_path),
        }

    max_restarts = int(cfg.get("max_restarts", 0))
    backoff_s = float(cfg.get("restart_backoff_s", 1.0))

    if implementation == "nvrx":
        adapter_spec = cfg.get("adapter")
        if isinstance(adapter_spec, str) and adapter_spec.strip():
            module_name, _, attr = adapter_spec.strip().partition(":")
            if not module_name or not attr:
                raise ResiliencyUnavailableError(
                    "resiliency_config.adapter must be in 'module:function' form"
                )
            try:
                adapter = getattr(importlib.import_module(module_name), attr)
            except Exception as exc:
                raise ResiliencyUnavailableError(
                    f"Failed to import resiliency adapter {adapter_spec!r}: {exc}"
                ) from exc
            if not callable(adapter):
                raise ResiliencyUnavailableError(
                    f"Resiliency adapter {adapter_spec!r} is not callable"
                )

            result, summary = adapter(
                run_once=run_once,
                output_dir=output_dir,
                nvrx_dir=nvrx_dir,
                events_path=events_path,
                summary_path=summary_path,
                resiliency_config=cfg,
                is_retryable_in_job=is_retryable_in_job,
                stop_requested=stop_requested,
            )
            if isinstance(summary, dict):
                summary.setdefault("enabled", True)
                summary.setdefault("implementation", "nvrx")
            return result, summary

        module_name = str(cfg.get("module") or "nvidia_resiliency_ext")
        if importlib.util.find_spec(module_name) is None:
            raise ResiliencyUnavailableError(
                "resiliency_config requested NVRx but module "
                f"'{module_name}' is not available"
            )
        raise ResiliencyUnavailableError(
            "NVRx is enabled but no adapter is wired.\n"
            "Provide `resiliency_config.adapter='module:function'` to integrate the library, "
            "or use `implementation=mock` for the dependency-free example."
        )

    if implementation != "mock":
        raise ResiliencyUnavailableError(
            f"Unknown resiliency implementation: {implementation!r}"
        )

    restarts = 0
    last_error_type: Optional[str] = None
    last_error_message: Optional[str] = None

    while True:
        if stop_requested():
            raise InJobStopRequestedError("Stop requested; aborting in-job restarts")
        try:
            result = run_once()
            summary = ResiliencySummary(
                enabled=True,
                implementation="mock",
                restarts=restarts,
                max_restarts=max_restarts,
                last_error_type=last_error_type,
                last_error_message=last_error_message,
            )
            write_json_atomic(summary_path, summary.to_dict())
            return result, {
                "enabled": True,
                "implementation": "mock",
                "restarts": restarts,
                "events_path": str(events_path),
                "summary_path": str(summary_path),
            }
        except BaseException as exc:
            if not is_retryable_in_job(exc):
                raise
            restarts += 1
            last_error_type = type(exc).__name__
            last_error_message = str(exc)
            append_jsonl(
                events_path,
                {
                    "time": now_iso(),
                    "event": "restart",
                    "restart": restarts,
                    "error_type": last_error_type,
                    "error_message": last_error_message,
                },
            )
            summary = ResiliencySummary(
                enabled=True,
                implementation="mock",
                restarts=restarts,
                max_restarts=max_restarts,
                last_error_type=last_error_type,
                last_error_message=last_error_message,
            )
            write_json_atomic(summary_path, summary.to_dict())
            if restarts > max_restarts:
                raise
            time.sleep(backoff_s)


__all__ = [
    "InJobStopRequestedError",
    "ResiliencyUnavailableError",
    "run_with_resiliency",
]
