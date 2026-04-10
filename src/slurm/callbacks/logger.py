"""LoggerCallback — standard-library logging for job lifecycle events."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional, TYPE_CHECKING

from ..logging import configure_logging as configure_sdk_logging
from .base import BaseCallback, WORKFLOW_METRICS_FILENAME
from .contexts import (
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
    PackagingBeginContext,
    PackagingEndContext,
    RunBeginContext,
    RunEndContext,
    SubmitBeginContext,
    SubmitEndContext,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
)
from ._metrics import log_workflow_metrics, persist_metrics_to_disk

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from ..cluster import Cluster


class LoggerCallback(BaseCallback):
    """Log lifecycle transitions using standard logging, configuring runner logging by default."""

    poll_interval_secs: Optional[float] = 2.0

    def __init__(
        self,
        *,
        logger: Optional[logging.Logger] = None,
        poll_interval: Optional[float] = None,
        log_level: int = logging.INFO,
        configure_logging: bool = True,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        if poll_interval is not None:
            self.poll_interval_secs = poll_interval

        self.log_level = log_level
        self.configure_logging = configure_logging
        self._logging_configured = False
        self._last_state: Optional[str] = None
        # Track workflow nesting depth for indented logging
        self._workflow_depth: Dict[str, int] = {}
        self._persisted_metrics: Dict[str, Dict[str, Any]] = {}

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        logger = state.pop("logger", None)
        if logger is not None:
            state["_logger_name"] = getattr(logger, "name", __name__)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        logger_name = state.pop("_logger_name", __name__)
        self.__dict__.update(state)
        self.logger = logging.getLogger(logger_name)

    def _log_workflow_metrics(
        self, workflow_name: str, metrics: Dict[str, Any]
    ) -> None:
        log_workflow_metrics(self.logger, workflow_name, metrics)

    def _persist_metrics_to_disk(
        self, workflow_dir: "os.PathLike[str]", metrics: Dict[str, Any]
    ) -> None:
        from pathlib import Path

        persist_metrics_to_disk(self.logger, Path(workflow_dir), metrics)

    def _load_metrics_from_disk(
        self,
        workflow_id: str,
        job_dir: Optional[str],
        cluster: Optional["Cluster"],
    ) -> None:
        if not job_dir or cluster is None:
            return

        metrics_path = os.path.join(job_dir, WORKFLOW_METRICS_FILENAME)
        try:
            content = cluster.backend.read_file(metrics_path)
        except FileNotFoundError:
            self.logger.debug(
                "Workflow metrics file not found at %s for %s",
                metrics_path,
                workflow_id,
            )
            return
        except Exception as exc:
            self.logger.warning(
                "Failed to read workflow metrics file %s: %s",
                metrics_path,
                exc,
            )
            return

        try:
            metrics = json.loads(content)
        except json.JSONDecodeError as exc:
            self.logger.warning(
                "Invalid workflow metrics JSON at %s: %s",
                metrics_path,
                exc,
            )
            return

        if not isinstance(metrics, dict):
            self.logger.debug(
                "Ignoring workflow metrics at %s: expected dict, got %s",
                metrics_path,
                type(metrics).__name__,
            )
            return

        self._persisted_metrics[workflow_id] = metrics

    def on_begin_package_ctx(self, ctx: PackagingBeginContext) -> None:
        task_name = getattr(ctx.task, "sbatch_options", {}).get(
            "job_name", getattr(getattr(ctx.task, "func", ctx.task), "__name__", "task")
        )
        self.logger.info("Packaging task '%s'", task_name)

    def on_end_package_ctx(self, ctx: PackagingEndContext) -> None:
        duration = f" in {ctx.duration:.2f}s" if ctx.duration is not None else ""
        self.logger.info("Packaging finished%s", duration)

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext) -> None:
        self.logger.info(
            "Submitting job %s via %s backend (partition=%s)",
            ctx.pre_submission_id,
            ctx.backend_type or "unknown",
            ctx.sbatch_options.get("partition", "default"),
        )

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        self.logger.info(
            "Job %s submitted as %s via %s backend",
            ctx.pre_submission_id,
            ctx.job_id,
            ctx.backend_type or "unknown",
        )

        stdout_path = getattr(ctx.job, "stdout_path", None)
        stderr_path = getattr(ctx.job, "stderr_path", None)
        directory = ctx.target_job_dir
        self.logger.info("Job directory: %s", directory or "unknown")
        self.logger.info("Stdout: %s", stdout_path or "unknown")
        self.logger.info("Stderr: %s", stderr_path or "unknown")

    def on_job_status_update_ctx(self, ctx: JobStatusUpdatedContext) -> None:
        state = ctx.status.get("JobState") or "UNKNOWN"
        if state == self._last_state:
            return
        self._last_state = state
        self.logger.info("[%s] status=%s", ctx.job_id, state)

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        self._configure_runner_logging()
        self.logger.info(
            "Starting remote execution: %s.%s on host=%s",
            ctx.module,
            ctx.function,
            ctx.hostname or "unknown",
        )

    def on_end_run_job_ctx(self, ctx: RunEndContext) -> None:
        if ctx.status == "success":
            self.logger.info("Remote execution completed in %.2fs", ctx.duration or 0.0)
        else:
            self.logger.error(
                "Remote execution failed: %s - %s",
                ctx.error_type or "error",
                ctx.error_message or "",
            )

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        if ctx.emitted_by is ExecutionLocus.RUNNER:
            return

        state = (ctx.job_state or "UNKNOWN").upper()
        exit_code = ctx.exit_code or "?"
        self.logger.info(
            "Job %s finished with state=%s exit=%s", ctx.job_id, state, exit_code
        )

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Log workflow orchestration start."""
        # Determine nesting depth (root workflows start at 0)
        parent_depth = 0
        if ctx.workflow_context and hasattr(ctx.workflow_context, "workflow_job_id"):
            # Check if this is a nested workflow by looking for parent
            parent_id = getattr(ctx.workflow_context, "parent_workflow_id", None)
            if parent_id and parent_id in self._workflow_depth:
                parent_depth = self._workflow_depth[parent_id] + 1

        self._workflow_depth[ctx.workflow_job_id] = parent_depth
        indent = "  " * parent_depth

        self.logger.info(
            "%s[Workflow] '%s' started (job_id=%s)",
            indent,
            ctx.workflow_name,
            ctx.workflow_job_id,
        )

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext) -> None:
        """Log child task submission."""
        parent_depth = self._workflow_depth.get(ctx.parent_workflow_id, 0)
        indent = "  " * (parent_depth + 1)
        task_type = "workflow" if ctx.child_is_workflow else "task"

        self.logger.info(
            "%s[%s] -> %s '%s' (job_id=%s)",
            indent,
            ctx.parent_workflow_name,
            task_type,
            ctx.child_task_name,
            ctx.child_job_id,
        )

        # Track child workflow depth
        if ctx.child_is_workflow:
            self._workflow_depth[ctx.child_job_id] = parent_depth + 1

    def on_workflow_end_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Log workflow orchestration completion."""
        parent_depth = self._workflow_depth.get(ctx.workflow_job_id, 0)
        indent = "  " * parent_depth

        if ctx.exception:
            self.logger.error(
                "%s[Workflow] '%s' failed: %s",
                indent,
                ctx.workflow_name,
                ctx.exception,
            )
        else:
            self.logger.info("%s[Workflow] '%s' completed", indent, ctx.workflow_name)

    def _configure_runner_logging(self) -> None:
        if not self.configure_logging or self._logging_configured:
            return
        try:
            configure_sdk_logging(level=self.log_level, use_rich=False)
            self._logging_configured = True
        except Exception as exc:  # pragma: no cover - best effort
            self.logger.warning("Failed to configure logging on runner: %s", exc)
