"""Core callback types and lifecycle contexts for the Slurm SDK."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from ..runtime import JobContext

try:  # pragma: no cover - rich is a hard dependency of the SDK
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn
except Exception:  # pragma: no cover - fallback for minimal environments
    Console = None  # type: ignore
    Progress = None  # type: ignore
    SpinnerColumn = None  # type: ignore
    TextColumn = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from ..cluster import Cluster
    from ..job import Job
    from ..packaging import PackagingStrategy
    from ..workflow import WorkflowContext


class ExecutionLocus(str, Enum):
    """Indicates where a callback hook executes."""

    CLIENT = "client"
    RUNNER = "runner"
    BOTH = "both"


@dataclass
class PackagingBeginContext:
    """Context emitted when packaging begins."""

    task: Any
    packaging_config: Optional[Dict[str, Any]] = None
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class PackagingEndContext:
    """Context emitted when packaging completes."""

    task: Any
    packaging_result: Any
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)
    duration: Optional[float] = None


@dataclass
class SubmitBeginContext:
    """Context emitted immediately before job submission."""

    task: Any
    sbatch_options: Dict[str, Any]
    pre_submission_id: str
    target_job_dir: str
    cluster: Optional["Cluster"] = None
    packaging_strategy: Optional["PackagingStrategy"] = None
    backend_type: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class SubmitEndContext:
    """Context emitted right after job submission."""

    job: "Job"
    job_id: str
    pre_submission_id: str
    target_job_dir: str
    sbatch_options: Dict[str, Any]
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)
    backend_type: Optional[str] = None


@dataclass
class RunBeginContext:
    """Context emitted on the runner before executing the user function."""

    module: str
    function: str
    args_file: str
    kwargs_file: str
    output_file: str
    job_id: Optional[str] = None
    job_dir: Optional[str] = None
    hostname: Optional[str] = None
    python_executable: Optional[str] = None
    python_version: Optional[str] = None
    working_directory: Optional[str] = None
    environment_snapshot: Optional[Dict[str, str]] = None
    start_time: float = field(default_factory=time.time)
    job_context: Optional[JobContext] = None


@dataclass
class RunEndContext:
    """Context emitted on the runner after executing the user function."""

    status: str
    output_file: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    job_id: Optional[str] = None
    job_dir: Optional[str] = None
    hostname: Optional[str] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: Optional[float] = None
    job_context: Optional[JobContext] = None


@dataclass
class JobStatusUpdatedContext:
    """Context emitted by the SDK-managed polling service."""

    job: "Job"
    job_id: str
    status: Dict[str, Any]
    timestamp: float
    previous_state: Optional[str] = None
    is_terminal: bool = False


@dataclass
class CompletedContext:
    """Context emitted when a job reaches a terminal state."""

    job: Optional["Job"]
    job_id: Optional[str]
    job_dir: Optional[str]
    job_state: Optional[str]
    exit_code: Optional[str]
    reason: Optional[str]
    stdout_path: Optional[str]
    stderr_path: Optional[str]
    start_time: Optional[float]
    end_time: Optional[float]
    duration: Optional[float]
    status: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    result_path: Optional[str] = None
    emitted_by: ExecutionLocus = ExecutionLocus.CLIENT
    job_context: Optional[JobContext] = None


@dataclass
class WorkflowCallbackContext:
    """Context for workflow lifecycle events (begin/end).

    Emitted when a workflow orchestrator starts or completes execution.
    This is distinct from the regular job lifecycle events that also fire
    for the workflow job itself.
    """

    # Workflow identification
    workflow_job_id: str
    workflow_job_dir: "Path"
    workflow_name: str  # Task function name

    # Workflow context (if available)
    workflow_context: Optional["WorkflowContext"]

    # Timing
    timestamp: float

    # Result (on_workflow_end only)
    result: Optional[Any] = None
    exception: Optional[Exception] = None

    # Cluster reference (may be None in runner context)
    cluster: Optional["Cluster"] = None


@dataclass
class WorkflowTaskSubmitContext:
    """Context for child task submission events.

    Emitted when a workflow submits a child task (which may itself be
    a workflow). Enables tracking of parent-child relationships and
    orchestration progress.
    """

    # Parent workflow info
    parent_workflow_id: str
    parent_workflow_dir: "Path"
    parent_workflow_name: str

    # Child task info
    child_job_id: str
    child_job_dir: "Path"
    child_task_name: str
    child_is_workflow: bool  # True if child is also a workflow

    # Timing and cluster
    timestamp: float
    cluster: "Cluster"


WORKFLOW_METRICS_FILENAME = ".slurm_benchmark_metrics.json"


_DEFAULT_HOOK_LOCI: Dict[str, ExecutionLocus] = {
    "on_begin_package_ctx": ExecutionLocus.CLIENT,
    "on_end_package_ctx": ExecutionLocus.CLIENT,
    "on_begin_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_end_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_job_status_update_ctx": ExecutionLocus.CLIENT,
    "on_begin_run_job_ctx": ExecutionLocus.RUNNER,
    "on_end_run_job_ctx": ExecutionLocus.RUNNER,
    "on_completed_ctx": ExecutionLocus.BOTH,
    "on_workflow_begin_ctx": ExecutionLocus.RUNNER,
    "on_workflow_end_ctx": ExecutionLocus.RUNNER,
    "on_workflow_task_submitted_ctx": ExecutionLocus.CLIENT,
}

_RUNNER_HOOKS: Tuple[str, ...] = (
    "on_begin_run_job_ctx",
    "on_end_run_job_ctx",
    "on_completed_ctx",
    "on_workflow_begin_ctx",
    "on_workflow_end_ctx",
)


class BaseCallback:
    """Base class for Slurm job lifecycle callbacks."""

    execution_loci: Dict[str, ExecutionLocus] = {}
    poll_interval_secs: Optional[float] = None
    requires_pickling: bool = True

    def on_begin_package_ctx(
        self, ctx: PackagingBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_package_ctx(
        self, ctx: PackagingEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_begin_submit_job_ctx(
        self, ctx: SubmitBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_submit_job_ctx(
        self, ctx: SubmitEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_begin_run_job_ctx(
        self, ctx: RunBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_run_job_ctx(
        self, ctx: RunEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_job_status_update_ctx(
        self, ctx: JobStatusUpdatedContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_completed_ctx(
        self, ctx: CompletedContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_workflow_begin_ctx(
        self, ctx: WorkflowCallbackContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow orchestrator starts execution.

        This fires AFTER on_begin_run_job_ctx for the workflow job itself.
        Marks the point where WorkflowContext is created and orchestration begins.

        Args:
            ctx: Workflow-specific callback context
        """
        pass

    def on_workflow_end_ctx(
        self, ctx: WorkflowCallbackContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow orchestrator completes execution.

        This fires BEFORE on_end_run_job_ctx for the workflow job itself.
        Marks the point where orchestration is complete and result is ready.

        Args:
            ctx: Workflow-specific callback context with result info
        """
        pass

    def on_workflow_task_submitted_ctx(
        self, ctx: WorkflowTaskSubmitContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow submits a child task.

        Fires immediately after a task/workflow is submitted from within
        a workflow context. Enables tracking of parent-child relationships.

        Args:
            ctx: Context with parent workflow and child task info
        """
        pass

    def get_execution_locus(self, hook_name: str) -> ExecutionLocus:
        if hook_name in self.execution_loci:
            return ExecutionLocus(self.execution_loci[hook_name])
        return _DEFAULT_HOOK_LOCI.get(hook_name, ExecutionLocus.CLIENT)

    def should_run_on_client(self, hook_name: str) -> bool:
        locus = self.get_execution_locus(hook_name)
        return locus in (ExecutionLocus.CLIENT, ExecutionLocus.BOTH)

    def should_run_on_runner(self, hook_name: str) -> bool:
        locus = self.get_execution_locus(hook_name)
        return locus in (ExecutionLocus.RUNNER, ExecutionLocus.BOTH)

    def get_poll_interval(self) -> Optional[float]:
        if self.poll_interval_secs is None:
            return None
        try:
            interval = float(self.poll_interval_secs)
        except (TypeError, ValueError):
            return None
        if interval <= 0:
            return None
        return interval

    def requires_runner_transport(self) -> bool:
        if not self.requires_pickling:
            return False
        return any(self.should_run_on_runner(hook) for hook in _RUNNER_HOOKS)


class LoggerCallback(BaseCallback):
    """Log lifecycle transitions using standard Python logging (no rich dependencies)."""

    poll_interval_secs: Optional[float] = 2.0

    def __init__(
        self,
        *,
        logger: Optional[logging.Logger] = None,
        poll_interval: Optional[float] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        if poll_interval is not None:
            self.poll_interval_secs = poll_interval

        self._last_state: Optional[str] = None
        # Track workflow nesting depth for indented logging
        self._workflow_depth: Dict[str, int] = {}

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
        child_count = metrics.get("child_count", 0)
        duration = metrics.get("duration_seconds")

        if duration is not None:
            self.logger.info(
                "[Workflow] '%s' completed in %.2fs (%d child tasks)",
                workflow_name,
                duration,
                child_count,
            )
        else:
            self.logger.info(
                "[Workflow] '%s' completed (%d child tasks)",
                workflow_name,
                child_count,
            )

        overhead = metrics.get("orchestration_overhead_ms")
        if overhead is not None:
            self.logger.info("  Avg submission interval: %.2fms", overhead)
            min_interval = metrics.get("min_submission_interval_ms")
            max_interval = metrics.get("max_submission_interval_ms")
            if min_interval is not None and max_interval is not None:
                self.logger.info(
                    "  Min/Max interval: %.2fms / %.2fms",
                    min_interval,
                    max_interval,
                )

            throughput = metrics.get("submission_throughput")
            if throughput is not None:
                self.logger.info(
                    "  Submission throughput: %.2f tasks/sec",
                    throughput,
                )

        child_avg = metrics.get("child_avg_duration")
        if child_avg is not None:
            self.logger.info(
                "  Child task durations: avg=%.2fs min=%.2fs max=%.2fs",
                child_avg,
                metrics.get("child_min_duration"),
                metrics.get("child_max_duration"),
            )

    def _persist_metrics_to_disk(
        self, workflow_dir: Path, metrics: Dict[str, Any]
    ) -> None:
        try:
            workflow_dir.mkdir(parents=True, exist_ok=True)
            metrics_path = workflow_dir / WORKFLOW_METRICS_FILENAME
            with metrics_path.open("w", encoding="utf-8") as fh:
                json.dump(metrics, fh, indent=2)
        except Exception as exc:  # pragma: no cover - best effort logging only
            self.logger.debug(
                "Failed to write workflow metrics to %s: %s",
                workflow_dir,
                exc,
            )

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

    def _persist_metrics_to_disk(
        self, workflow_dir: Path, metrics: Dict[str, Any]
    ) -> None:
        try:
            workflow_dir.mkdir(parents=True, exist_ok=True)
            metrics_path = workflow_dir / WORKFLOW_METRICS_FILENAME
            with metrics_path.open("w", encoding="utf-8") as fh:
                json.dump(metrics, fh, indent=2)
        except Exception as exc:  # pragma: no cover - best effort logging only
            self.logger.debug(
                "Failed to write workflow metrics to %s: %s",
                workflow_dir,
                exc,
            )

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


class RichLoggerCallback(BaseCallback):
    """Log lifecycle transitions with rich progress bars and formatted output.

    This callback provides an enhanced terminal experience with:
    - Animated progress spinners
    - Live job status updates
    - Formatted exception tracebacks
    - Rich text formatting

    Args:
        console: Optional rich Console instance. If not provided, creates a new one.
        poll_interval: Polling interval in seconds for job status updates (default: 2.0)
    """

    poll_interval_secs: Optional[float] = 2.0

    def __init__(
        self,
        *,
        console: Optional["Console"] = None,
        poll_interval: Optional[float] = None,
    ) -> None:
        if Console is None:
            raise ImportError(
                "RichLoggerCallback requires the 'rich' package. "
                "Install it with: pip install rich"
            )

        self.logger = logging.getLogger(__name__)
        self.console = console or Console()
        if poll_interval is not None:
            self.poll_interval_secs = poll_interval

        self._progress: Optional[Progress] = None
        self._progress_task_id: Optional[int] = None
        self._job_label: Optional[str] = None
        self._last_state: Optional[str] = None
        self._phase: Optional[str] = None
        # Track workflow nesting depth for indented logging
        self._workflow_depth: Dict[str, int] = {}

    def _ensure_progress(self) -> None:
        """Create and start the progress display if not already running."""
        if self._progress is None:
            columns = [
                SpinnerColumn(),
                TextColumn("{task.description}"),
                TextColumn("[bold]{task.fields[state]}[/bold]"),
            ]
            self._progress = Progress(*columns, console=self.console, transient=True)
            self._progress.start()
            self._progress_task_id = self._progress.add_task(
                self._job_label or "Job",
                state=(self._phase or "PENDING"),
                total=None,
            )
        elif self._progress_task_id is not None and self._job_label:
            self._progress.update(self._progress_task_id, description=self._job_label)

    def _update_progress(self, state: str, description: Optional[str] = None) -> None:
        """Update the progress bar with new state and description."""
        if self._progress is None or self._progress_task_id is None:
            return
        if description is None:
            description = self._job_label or "Job"
        self._progress.update(
            self._progress_task_id, description=description, state=state
        )

    def _set_phase(self, state: str, description: Optional[str] = None) -> None:
        """Set the current phase and update progress display."""
        self._phase = state.upper()
        self._ensure_progress()
        self._update_progress(self._phase, description=description)

    def _set_job_label(self, label: str) -> None:
        """Update the job label in the progress display."""
        self._job_label = label
        if self._progress and self._progress_task_id is not None:
            self._progress.update(self._progress_task_id, description=label)

    def _stop_progress(self) -> None:
        """Stop and clean up the progress display."""
        progress = self._progress
        if progress is not None:
            try:
                if self._progress_task_id is not None:
                    progress.update(self._progress_task_id, completed=1)
            except Exception:  # pragma: no cover - defensive
                pass
            finally:
                try:
                    progress.stop()
                except Exception:  # pragma: no cover
                    pass
        self._progress = None
        self._progress_task_id = None
        self._phase = None
        self._job_label = None
        self._last_state = None

    def _log(self, message: str) -> None:
        """Log a message to the console."""
        self.console.print(message)

    def on_begin_package_ctx(self, ctx: PackagingBeginContext) -> None:
        task_name = getattr(ctx.task, "sbatch_options", {}).get(
            "job_name", getattr(getattr(ctx.task, "func", ctx.task), "__name__", "task")
        )
        label = f"{task_name}"
        self._set_job_label(label)
        self._set_phase("PACKAGING", description=label)
        self._log(f"[cyan]Packaging task[/cyan] '{task_name}'")

    def on_end_package_ctx(self, ctx: PackagingEndContext) -> None:
        duration = f" in {ctx.duration:.2f}s" if ctx.duration is not None else ""
        self._set_phase("PACKAGED")
        self._log(f"[green]✓[/green] Packaging finished{duration}")

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext) -> None:
        self._set_job_label(f"Job {ctx.pre_submission_id}")
        self._set_phase("SUBMITTING")
        self._log(
            f"[cyan]Submitting job[/cyan] {ctx.pre_submission_id} via {ctx.backend_type or 'unknown'} "
            f"(partition={ctx.sbatch_options.get('partition', 'default')})"
        )

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        self._set_job_label(f"Job {ctx.job_id}")
        self._set_phase("SUBMITTED")
        self._log(
            f"[green]✓[/green] Job {ctx.pre_submission_id} submitted as [bold]{ctx.job_id}[/bold] "
            f"via {ctx.backend_type or 'unknown'}"
        )

        stdout_path = getattr(ctx.job, "stdout_path", None)
        stderr_path = getattr(ctx.job, "stderr_path", None)
        directory = ctx.target_job_dir

        self.console.print("\n[bold]Job launch summary:[/bold]")
        self.console.print(f"  Directory: [dim]{directory or 'unknown'}[/dim]")
        self.console.print(f"  Stdout:    [dim]{stdout_path or 'unknown'}[/dim]")
        self.console.print(f"  Stderr:    [dim]{stderr_path or 'unknown'}[/dim]\n")

    def on_job_status_update_ctx(self, ctx: JobStatusUpdatedContext) -> None:
        state = ctx.status.get("JobState") or "UNKNOWN"
        if state == self._last_state:
            return
        self._last_state = state
        self._set_job_label(f"Job {ctx.job_id}")
        self._set_phase(state)

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        self._log(
            f"[cyan]Starting remote execution:[/cyan] {ctx.module}.{ctx.function} "
            f"on [bold]{ctx.hostname or 'unknown'}[/bold]"
        )

    def on_end_run_job_ctx(self, ctx: RunEndContext) -> None:
        if ctx.status == "success":
            self._log(
                f"[green]✓[/green] Remote execution completed in {ctx.duration or 0.0:.2f}s"
            )
        else:
            self.console.print(
                f"[red]✗ Remote execution failed:[/red] {ctx.error_type or 'error'}",
                style="bold red",
            )
            if ctx.error_message:
                self.console.print(f"  {ctx.error_message}")
            if ctx.traceback:
                from rich.panel import Panel
                from rich.syntax import Syntax

                tb_syntax = Syntax(
                    ctx.traceback, "pytb", theme="monokai", line_numbers=True
                )
                self.console.print(
                    Panel(tb_syntax, title="Traceback", border_style="red")
                )

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        if ctx.emitted_by is ExecutionLocus.RUNNER:
            return

        state = (ctx.job_state or "UNKNOWN").upper()
        exit_code = ctx.exit_code or "?"

        self._set_phase(state)
        self._stop_progress()

        if state == "COMPLETED" and exit_code in ("0:0", "0"):
            self._log(
                f"[green bold]✓ Job {ctx.job_id} completed successfully[/green bold] (exit={exit_code})"
            )
        elif state == "FAILED":
            self._log(
                f"[red bold]✗ Job {ctx.job_id} failed[/red bold] (state={state}, exit={exit_code})"
            )
        else:
            self._log(
                f"[yellow]Job {ctx.job_id}[/yellow] finished with state={state} exit={exit_code}"
            )

    def on_workflow_begin_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Log workflow orchestration start with rich formatting."""
        # Determine nesting depth (root workflows start at 0)
        parent_depth = 0
        if ctx.workflow_context and hasattr(ctx.workflow_context, "workflow_job_id"):
            # Check if this is a nested workflow by looking for parent
            parent_id = getattr(ctx.workflow_context, "parent_workflow_id", None)
            if parent_id and parent_id in self._workflow_depth:
                parent_depth = self._workflow_depth[parent_id] + 1

        self._workflow_depth[ctx.workflow_job_id] = parent_depth
        indent = "  " * parent_depth

        self._log(
            f"{indent}🔵 [bold blue]Workflow[/bold blue] '{ctx.workflow_name}' started "
            f"(job_id={ctx.workflow_job_id})"
        )

    def on_workflow_task_submitted_ctx(self, ctx: WorkflowTaskSubmitContext) -> None:
        """Log child task submission with rich formatting."""
        parent_depth = self._workflow_depth.get(ctx.parent_workflow_id, 0)
        indent = "  " * (parent_depth + 1)

        if ctx.child_is_workflow:
            icon = "🔵"
            task_type = "[bold blue]workflow[/bold blue]"
        else:
            icon = "⚪"
            task_type = "[dim]task[/dim]"

        self._log(
            f"{indent}{icon} [{ctx.parent_workflow_name}] → {task_type} "
            f"'{ctx.child_task_name}' (job_id={ctx.child_job_id})"
        )

        # Track child workflow depth
        if ctx.child_is_workflow:
            self._workflow_depth[ctx.child_job_id] = parent_depth + 1

    def on_workflow_end_ctx(self, ctx: WorkflowCallbackContext) -> None:
        """Log workflow orchestration completion with rich formatting."""
        parent_depth = self._workflow_depth.get(ctx.workflow_job_id, 0)
        indent = "  " * parent_depth

        if ctx.exception:
            self._log(
                f"{indent}❌ [bold red]Workflow[/bold red] '{ctx.workflow_name}' failed: "
                f"{ctx.exception}"
            )
        else:
            self._log(
                f"{indent}✅ [bold green]Workflow[/bold green] '{ctx.workflow_name}' completed"
            )


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
            and getattr(getattr(ctx.job, "task_func", None), "_is_workflow", False)
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
            self._persist_metrics_to_disk(workflow_path, metrics)

        self._log_workflow_metrics(wf_data.get("name", ctx.workflow_job_id), metrics)

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


__all__ = [
    "BaseCallback",
    "BenchmarkCallback",
    "CompletedContext",
    "ExecutionLocus",
    "JobStatusUpdatedContext",
    "LoggerCallback",
    "RichLoggerCallback",
    "PackagingBeginContext",
    "PackagingEndContext",
    "RunBeginContext",
    "RunEndContext",
    "SubmitBeginContext",
    "SubmitEndContext",
    "WorkflowCallbackContext",
    "WorkflowTaskSubmitContext",
]


def _benchmark_build_metrics_from_data(
    self: "BenchmarkCallback", wf_data: Dict[str, Any]
) -> Dict[str, Any]:
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


def _benchmark_log_workflow_metrics(
    self: "BenchmarkCallback", workflow_name: str, metrics: Dict[str, Any]
) -> None:
    child_count = metrics.get("child_count", 0)
    duration = metrics.get("duration_seconds")

    if duration is not None:
        self.logger.info(
            "[Workflow] '%s' completed in %.2fs (%d child tasks)",
            workflow_name,
            duration,
            child_count,
        )
    else:
        self.logger.info(
            "[Workflow] '%s' completed (%d child tasks)",
            workflow_name,
            child_count,
        )

    overhead = metrics.get("orchestration_overhead_ms")
    if overhead is not None:
        self.logger.info("  Avg submission interval: %.2fms", overhead)
        min_interval = metrics.get("min_submission_interval_ms")
        max_interval = metrics.get("max_submission_interval_ms")
        if min_interval is not None and max_interval is not None:
            self.logger.info(
                "  Min/Max interval: %.2fms / %.2fms",
                min_interval,
                max_interval,
            )

        throughput = metrics.get("submission_throughput")
        if throughput is not None:
            self.logger.info(
                "  Submission throughput: %.2f tasks/sec",
                throughput,
            )

    child_avg = metrics.get("child_avg_duration")
    if child_avg is not None:
        self.logger.info(
            "  Child task durations: avg=%.2fs min=%.2fs max=%.2fs",
            child_avg,
            metrics.get("child_min_duration"),
            metrics.get("child_max_duration"),
        )


def _benchmark_persist_metrics_to_disk(
    self: "BenchmarkCallback", workflow_dir: Path, metrics: Dict[str, Any]
) -> None:
    try:
        workflow_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = workflow_dir / WORKFLOW_METRICS_FILENAME
        with metrics_path.open("w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)
    except Exception as exc:  # pragma: no cover - best effort logging only
        self.logger.debug(
            "Failed to write workflow metrics to %s: %s",
            workflow_dir,
            exc,
        )


def _benchmark_load_metrics_from_disk(
    self: "BenchmarkCallback",
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


def _benchmark_populate_child_stats_from_dir(
    self: "BenchmarkCallback", wf_data: Dict[str, Any], workflow_dir: Optional[Path]
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


BenchmarkCallback._populate_child_stats_from_dir = (
    _benchmark_populate_child_stats_from_dir  # type: ignore[attr-defined]
)
# Attach helper implementations to BenchmarkCallback to support pickled instances
BenchmarkCallback._build_metrics_from_data = _benchmark_build_metrics_from_data  # type: ignore[attr-defined]
BenchmarkCallback._log_workflow_metrics = _benchmark_log_workflow_metrics  # type: ignore[attr-defined]
BenchmarkCallback._persist_metrics_to_disk = _benchmark_persist_metrics_to_disk  # type: ignore[attr-defined]
BenchmarkCallback._load_metrics_from_disk = _benchmark_load_metrics_from_disk  # type: ignore[attr-defined]
