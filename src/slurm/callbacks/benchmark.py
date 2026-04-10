"""BenchmarkCallback — performance metrics for tasks and workflows."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

from ..task import WorkflowTask
from .base import BaseCallback, WORKFLOW_METRICS_FILENAME
from .contexts import (
    CompletedContext,
    ExecutionLocus,
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


class BenchmarkCallback(BaseCallback):
    """A callback that measures performance metrics and timing for tasks and workflows.

    This callback tracks:
    - Task-level timing (packaging, submission, execution)
    - Workflow orchestration overhead
    - Child task submission rate
    - Parallel vs sequential execution patterns
    - End-to-end workflow duration

    Example:
        >>> from slurm import Cluster
        >>> from slurm.callbacks import BenchmarkCallback
        >>>
        >>> benchmark = BenchmarkCallback()
        >>> cluster = Cluster.from_env(callbacks=[benchmark])
        >>>
        >>> # After workflow completes
        >>> metrics = benchmark.get_workflow_metrics("workflow_job_id")
        >>> print(f"Orchestration overhead: {metrics['orchestration_overhead_ms']:.2f}ms")
        >>> print(f"Child tasks submitted: {metrics['child_count']}")
        >>> print(f"Average submission interval: {metrics['avg_submission_interval_ms']:.2f}ms")
    """

    def __init__(self) -> None:
        self._timestamps: Dict[str, float] = {}
        self.logger = logging.getLogger(__name__)

        # Workflow-specific tracking
        self._workflows: Dict[str, Dict[str, Any]] = {}
        self._child_to_parent: Dict[str, str] = {}
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

    def on_begin_package_ctx(self, ctx: PackagingBeginContext) -> None:
        self._timestamps["package"] = ctx.timestamp

    def on_end_package_ctx(self, ctx: PackagingEndContext) -> None:
        start = self._timestamps.get("package")
        if start is not None:
            self.logger.info("Packaging duration: %.2fs", (ctx.timestamp - start))

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext) -> None:
        self._timestamps["submit"] = ctx.timestamp

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        start = self._timestamps.get("submit")
        if start is not None:
            self.logger.info("Submission duration: %.2fs", ctx.timestamp - start)

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        self._timestamps["run"] = ctx.start_time

    def on_end_run_job_ctx(self, ctx: RunEndContext) -> None:
        start = self._timestamps.get("run")
        if start is not None and ctx.end_time is not None:
            self.logger.info("Execution duration: %.2fs", ctx.end_time - start)

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        if ctx.start_time is not None and ctx.end_time is not None:
            self.logger.info("Total elapsed time: %.2fs", ctx.end_time - ctx.start_time)

        # Track child task completion for workflow metrics
        if ctx.job_id and ctx.job_id in self._child_to_parent:
            parent_id = self._child_to_parent[ctx.job_id]
            if parent_id in self._workflows:
                wf_data = self._workflows[parent_id]
                wf_data["completed_count"] += 1
                if ctx.duration:
                    wf_data["child_durations"].append(ctx.duration)

        if (
            ctx.emitted_by is ExecutionLocus.CLIENT
            and ctx.job is not None
            and isinstance(getattr(ctx.job, "task_func", None), WorkflowTask)
            and ctx.job_id
        ):
            job_dir = ctx.job_dir or getattr(ctx.job, "target_job_dir", None)
            cluster = getattr(ctx.job, "cluster", None)
            self._load_metrics_from_disk(ctx.job_id, job_dir, cluster)

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Track workflow orchestration start."""
        self._workflows[ctx.workflow_job_id] = {
            "name": ctx.workflow_name,
            "start_time": ctx.timestamp,
            "end_time": None,
            "child_count": 0,
            "completed_count": 0,
            "submission_times": [],
            "child_durations": [],
        }

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext) -> None:
        """Track child task submissions for throughput analysis."""
        if ctx.parent_workflow_id in self._workflows:
            wf_data = self._workflows[ctx.parent_workflow_id]
            wf_data["child_count"] += 1
            wf_data["submission_times"].append(ctx.timestamp)
            self._child_to_parent[ctx.child_job_id] = ctx.parent_workflow_id

    def on_workflow_end_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Calculate and persist workflow performance metrics."""
        wf_data = self._workflows.get(ctx.workflow_job_id)
        if not wf_data:
            return

        self._populate_child_stats_from_dir(wf_data, ctx.workflow_job_dir)
        wf_data["end_time"] = ctx.timestamp
        metrics = self._build_metrics_from_data(wf_data)
        self._persisted_metrics[ctx.workflow_job_id] = metrics

        workflow_dir = ctx.workflow_job_dir
        if workflow_dir:
            try:
                workflow_path = Path(workflow_dir)
            except TypeError:
                workflow_path = Path(str(workflow_dir))
            persist_metrics_to_disk(self.logger, workflow_path, metrics)

        log_workflow_metrics(
            self.logger, wf_data.get("name", ctx.workflow_job_id), metrics
        )

    def get_workflow_metrics(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed performance metrics for a workflow.

        Args:
            workflow_id: The workflow job ID

        Returns:
            Dictionary with workflow performance metrics, or None if not found.
            Includes:
            - name: Workflow name
            - duration_seconds: Total workflow duration
            - child_count: Number of child tasks submitted
            - completed_count: Number of child tasks completed
            - orchestration_overhead_ms: Average time between submissions
            - submission_throughput: Tasks submitted per second
            - child_avg_duration: Average child task execution time
        """
        persisted = self._persisted_metrics.get(workflow_id)
        if persisted is not None:
            return dict(persisted)

        wf_data = self._workflows.get(workflow_id)
        if not wf_data:
            return None

        return self._build_metrics_from_data(wf_data)

    def get_all_workflow_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get performance metrics for all tracked workflows.

        Returns:
            Dictionary mapping workflow_id to metrics dict
        """
        results: Dict[str, Dict[str, Any]] = {
            wf_id: metrics.copy() for wf_id, metrics in self._persisted_metrics.items()
        }

        for wf_id in self._workflows:
            if wf_id in results:
                continue
            metrics = self.get_workflow_metrics(wf_id)
            if metrics is not None:
                results[wf_id] = metrics

        return results

    def _build_metrics_from_data(self, wf_data: Dict[str, Any]) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "name": wf_data.get("name"),
            "child_count": wf_data.get("child_count", 0),
            "completed_count": wf_data.get("completed_count", 0),
        }

        start_time = wf_data.get("start_time")
        end_time = wf_data.get("end_time")
        if isinstance(start_time, (int, float)) and isinstance(end_time, (int, float)):
            duration = end_time - start_time
            if duration >= 0:
                metrics["duration_seconds"] = duration
                child_count = metrics["child_count"]
                if child_count and duration > 0:
                    metrics["submission_throughput"] = child_count / duration

        submission_times = wf_data.get("submission_times") or []
        if isinstance(submission_times, list) and len(submission_times) >= 2:
            intervals = [
                (submission_times[i] - submission_times[i - 1]) * 1000
                for i in range(1, len(submission_times))
            ]
            if intervals:
                metrics["orchestration_overhead_ms"] = sum(intervals) / len(intervals)
                metrics["min_submission_interval_ms"] = min(intervals)
                metrics["max_submission_interval_ms"] = max(intervals)

        child_durations = wf_data.get("child_durations") or []
        if isinstance(child_durations, list) and child_durations:
            metrics["child_avg_duration"] = sum(child_durations) / len(child_durations)
            metrics["child_min_duration"] = min(child_durations)
            metrics["child_max_duration"] = max(child_durations)

        return metrics

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

    def _populate_child_stats_from_dir(
        self, wf_data: Dict[str, Any], workflow_dir: Optional[Path]
    ) -> None:
        if not workflow_dir:
            return

        try:
            tasks_dir = Path(workflow_dir) / "tasks"
        except TypeError:
            tasks_dir = Path(str(workflow_dir)) / "tasks"

        if not tasks_dir.exists():
            return

        count = sum(1 for _ in tasks_dir.rglob("metadata.json"))
        if count:
            wf_data["child_count"] = count
            wf_data["completed_count"] = count
