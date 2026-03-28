"""Background job status polling and callback dispatch.

This is an internal module extracted from ``cluster.py`` to reduce its size.
The public API surface remains on the :class:`~slurm.cluster.Cluster` class;
functions here are implementation details.
"""

import logging
import time
import threading
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from .callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
)

if TYPE_CHECKING:
    from .cluster import Cluster
    from .job import Job

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Callback dispatch
# ---------------------------------------------------------------------------


def dispatch_callbacks(
    callbacks: List[BaseCallback], method_name: str, context: Any
) -> None:
    """Dispatch a lifecycle event to all registered callbacks.

    Calls *method_name* on each callback that opts in via
    ``should_run_on_client``, catching and logging any exceptions.
    """
    for callback in callbacks:
        if not callback.should_run_on_client(method_name):
            continue
        try:
            getattr(callback, method_name)(context)
        except Exception as exc:
            logger.warning(
                "Callback %s failed in %s: %s",
                type(callback).__name__,
                method_name,
                exc,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# Job status poller thread
# ---------------------------------------------------------------------------


class _JobStatusPoller(threading.Thread):
    """Background thread that emits JobStatusUpdated callbacks."""

    def __init__(
        self,
        cluster: "Cluster",
        job: "Job",
        subscriptions: List[Tuple[BaseCallback, float]],
    ) -> None:
        super().__init__(
            daemon=True,
            name=f"slurm-job-poller-{job.id}",
        )
        self.cluster = cluster
        self.job = job
        self.subscriptions = subscriptions
        self._stop = threading.Event()
        self._last_emit: Dict[BaseCallback, float] = {}
        self._previous_state: Optional[str] = None
        self._interval = min((interval for _, interval in subscriptions), default=5.0)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:  # pragma: no cover - background thread
        try:
            while not self._stop.is_set():
                timestamp = time.time()
                try:
                    status = self.cluster.backend.get_job_status(self.job.id)
                except Exception as exc:  # pragma: no cover - backend errors
                    status = {"JobState": "UNKNOWN", "Error": str(exc)}

                self.job._update_status_cache(status, timestamp)

                current_state = status.get("JobState") or "UNKNOWN"
                is_terminal = current_state in self.job.TERMINAL_STATES

                ctx = JobStatusUpdatedContext(
                    job=self.job,
                    job_id=self.job.id,
                    status=status,
                    timestamp=timestamp,
                    previous_state=self._previous_state,
                    is_terminal=is_terminal,
                )

                for callback, interval in self.subscriptions:
                    if not callback.should_run_on_client("on_job_status_update_ctx"):
                        continue

                    last_emit = self._last_emit.get(callback, 0.0)
                    should_emit = (
                        (timestamp - last_emit) >= interval
                        or self._previous_state != current_state
                        or is_terminal
                    )

                    if not should_emit:
                        continue

                    try:
                        callback.on_job_status_update_ctx(ctx)
                    except Exception as exc:  # pragma: no cover - callback errors
                        logger.warning(
                            "Callback %s failed during polling: %s",
                            type(callback).__name__,
                            exc,
                            exc_info=True,
                        )
                    self._last_emit[callback] = timestamp

                self._previous_state = current_state

                if is_terminal:
                    self.cluster._emit_completed_context(
                        self.job,
                        status,
                        timestamp,
                    )
                    break

                self._stop.wait(self._interval)
        finally:
            self.cluster._on_poller_finished(self.job.id)


# ---------------------------------------------------------------------------
# Poller lifecycle helpers
# ---------------------------------------------------------------------------


def maybe_start_job_poller(cluster: "Cluster", job: "Job") -> None:
    """Start a background poller for *job* if any callback requests polling."""
    subscriptions: List[Tuple[BaseCallback, float]] = []
    for callback in cluster.callbacks:
        interval = callback.get_poll_interval()
        if interval is None:
            continue
        if not callback.should_run_on_client("on_job_status_update_ctx"):
            continue
        subscriptions.append((callback, interval))

    if not subscriptions:
        return

    if not hasattr(cluster, "_job_pollers"):
        cluster._job_pollers: Dict[str, _JobStatusPoller] = {}
        cluster._job_pollers_lock = threading.Lock()

    poller = _JobStatusPoller(cluster, job, subscriptions)
    with cluster._job_pollers_lock:
        cluster._job_pollers[job.id] = poller
    poller.start()


def emit_completed_context(
    callbacks: List[BaseCallback],
    job: "Job",
    status: Dict[str, Any],
    timestamp: Optional[float],
    *,
    error_payload: Optional[Dict[str, Optional[str]]] = None,
    emitted_by: ExecutionLocus = ExecutionLocus.CLIENT,
) -> None:
    """Emit the :class:`CompletedContext` callback for a finished job."""
    if not hasattr(job, "_completed_context_lock"):
        return

    with job._completed_context_lock:
        if getattr(job, "_completed_context_emitted", False):
            return
        job._completed_context_emitted = True

    finished_at = timestamp or time.time()
    job.finished_at = job.finished_at or finished_at
    start_time = job.started_at or job.created_at
    duration: Optional[float] = None
    if job.finished_at is not None and start_time is not None:
        duration = job.finished_at - start_time

    context = CompletedContext(
        job=job if emitted_by is ExecutionLocus.CLIENT else None,
        job_id=job.id,
        job_dir=job.target_job_dir,
        job_state=status.get("JobState"),
        exit_code=status.get("ExitCode"),
        reason=status.get("Reason") or status.get("Error") or status.get("StateDesc"),
        stdout_path=job.stdout_path,
        stderr_path=job.stderr_path,
        start_time=start_time,
        end_time=job.finished_at,
        duration=duration,
        status=status,
        error_type=error_payload.get("error_type") if error_payload else None,
        error_message=error_payload.get("error_message") if error_payload else None,
        traceback=error_payload.get("traceback") if error_payload else None,
        result_path=job.result_path,
        emitted_by=emitted_by,
    )

    dispatch_callbacks(callbacks, "on_completed_ctx", context)


def on_poller_finished(
    job_pollers: Optional[Dict[str, _JobStatusPoller]],
    job_pollers_lock: Optional[threading.Lock],
    job_id: str,
) -> None:
    """Remove a finished poller from the tracking dict."""
    if job_pollers is not None:
        if job_pollers_lock is not None:
            with job_pollers_lock:
                job_pollers.pop(job_id, None)
        else:
            job_pollers.pop(job_id, None)
