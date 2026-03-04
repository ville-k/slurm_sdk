from __future__ import annotations

import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, TypeVar

from slurm.examples.high_availability_training.ha.io import write_json_atomic
from slurm.examples.high_availability_training.ha.paths import AttemptPaths
from slurm.examples.high_availability_training.ha.resiliency import (
    InJobStopRequestedError,
    ResiliencyUnavailableError,
    run_with_resiliency,
)
from slurm.examples.high_availability_training.ha.task_result import (
    StatusCode,
    exception_details,
    make_task_result,
)

T = TypeVar("T")


class StopRequestedError(RuntimeError):
    """Raised by user code when a SIGTERM/SIGUSR1 stop has been requested."""


@dataclass
class _SignalState:
    received: Optional[int] = None
    previous_handlers: Optional[Dict[int, Any]] = None

    def stop_requested(self) -> bool:
        return self.received is not None


def _install_signal_handlers(signals: Tuple[int, ...]) -> _SignalState:
    state = _SignalState(received=None, previous_handlers={})

    def handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        state.received = signum

    for signum in signals:
        state.previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, handler)

    return state


def _restore_signal_handlers(state: _SignalState) -> None:
    if not state.previous_handlers:
        return
    for signum, previous in state.previous_handlers.items():
        try:
            signal.signal(signum, previous)
        except Exception:
            continue


def run_task_attempt(
    *,
    output_dir: Path,
    input_payload: Mapping[str, Any],
    checkpoint_in: Optional[str],
    run_once: Callable[[Callable[[], bool]], T],
    is_retryable_in_job: Callable[[BaseException], bool],
    user_error_types: Tuple[type[BaseException], ...],
    resiliency_config: Optional[Mapping[str, Any]] = None,
    checkpoint_out_path: Optional[Path] = None,
    metrics_path: Optional[Path] = None,
    events_path: Optional[Path] = None,
    success_summary: str | Callable[[T], str] = "Completed",
    preempted_summary: str | Callable[[BaseException], str] = (
        "Preemption/timeout signal received"
    ),
    user_error_summary: str = "Non-retryable error",
    retryable_error_summary: str = "Retryable error",
) -> Dict[str, Any]:
    """Run a task attempt with standardized HA artifacts and result recording.

    This helper centralizes common high-availability mechanics:
    - Create `output_dir` and standard subdirs
    - Write `input.json` and `checkpoint_in.json`
    - Install SIGUSR1/SIGTERM handlers
    - Optionally wrap business logic with an in-job restart loop
    - Write `result.json` as the commit record (last)

    The provided `run_once` should contain application logic (training/eval) and
    may raise exceptions to be mapped into a `TaskResult` status code.
    """
    output_path = output_dir.expanduser()
    paths = AttemptPaths(output_dir=output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)

    write_json_atomic(paths.input_path, dict(input_payload))
    write_json_atomic(paths.checkpoint_in_path, {"checkpoint_in": checkpoint_in})

    signal_state = _install_signal_handlers((signal.SIGUSR1, signal.SIGTERM))
    try:
        result_value, nvrx = run_with_resiliency(
            output_dir=output_path,
            resiliency_config=resiliency_config,
            run_once=lambda: run_once(signal_state.stop_requested),
            is_retryable_in_job=is_retryable_in_job,
            stop_requested=signal_state.stop_requested,
        )
        summary = (
            success_summary(result_value)
            if callable(success_summary)
            else str(success_summary)
        )
        result = make_task_result(
            status_code=StatusCode.SUCCESS.value,
            status_summary=summary,
            output_dir=str(output_path),
            checkpoint_out=str(checkpoint_out_path) if checkpoint_out_path else None,
            metrics_path=str(metrics_path) if metrics_path else None,
            events_path=str(events_path) if events_path else None,
            nvrx=nvrx,
        )
        write_json_atomic(paths.result_path, result)
        return result
    except (InJobStopRequestedError, StopRequestedError) as exc:
        summary = (
            preempted_summary(exc)
            if callable(preempted_summary)
            else str(preempted_summary)
        )
        result = make_task_result(
            status_code=StatusCode.PREEMPTED.value,
            status_summary=summary,
            output_dir=str(output_path),
            checkpoint_out=str(checkpoint_out_path) if checkpoint_out_path else None,
            metrics_path=str(metrics_path) if metrics_path else None,
            events_path=str(events_path) if events_path else None,
            error_class="system",
            error=exception_details(exc),
            nvrx={
                "enabled": bool((resiliency_config or {}).get("enabled")),
                "restarts": 0,
            },
        )
        write_json_atomic(paths.result_path, result)
        return result
    except ResiliencyUnavailableError as exc:
        result = make_task_result(
            status_code=StatusCode.NONRETRYABLE_ERROR.value,
            status_summary="Resiliency requested but unavailable",
            output_dir=str(output_path),
            metrics_path=str(metrics_path) if metrics_path else None,
            events_path=str(events_path) if events_path else None,
            error_class="user",
            error=exception_details(exc),
            nvrx={"enabled": True, "implementation": "nvrx"},
        )
        write_json_atomic(paths.result_path, result)
        return result
    except user_error_types as exc:
        result = make_task_result(
            status_code=StatusCode.NONRETRYABLE_ERROR.value,
            status_summary=user_error_summary,
            output_dir=str(output_path),
            metrics_path=str(metrics_path) if metrics_path else None,
            events_path=str(events_path) if events_path else None,
            error_class="user",
            error=exception_details(exc),
        )
        write_json_atomic(paths.result_path, result)
        return result
    except Exception as exc:
        result = make_task_result(
            status_code=StatusCode.RETRYABLE_ERROR.value,
            status_summary=retryable_error_summary,
            output_dir=str(output_path),
            metrics_path=str(metrics_path) if metrics_path else None,
            events_path=str(events_path) if events_path else None,
            error_class="transient",
            error=exception_details(exc),
        )
        write_json_atomic(paths.result_path, result)
        return result
    finally:
        _restore_signal_handlers(signal_state)


__all__ = [
    "StopRequestedError",
    "run_task_attempt",
]
