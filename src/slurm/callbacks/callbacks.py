"""Core callback types and lifecycle contexts for the Slurm SDK."""

from __future__ import annotations

import logging
import time
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


_DEFAULT_HOOK_LOCI: Dict[str, ExecutionLocus] = {
    "on_begin_package_ctx": ExecutionLocus.CLIENT,
    "on_end_package_ctx": ExecutionLocus.CLIENT,
    "on_begin_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_end_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_job_status_update_ctx": ExecutionLocus.CLIENT,
    "on_begin_run_job_ctx": ExecutionLocus.RUNNER,
    "on_end_run_job_ctx": ExecutionLocus.RUNNER,
    "on_completed_ctx": ExecutionLocus.BOTH,
}

_RUNNER_HOOKS: Tuple[str, ...] = (
    "on_begin_run_job_ctx",
    "on_end_run_job_ctx",
    "on_completed_ctx",
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


class BenchmarkCallback(BaseCallback):
    """A callback that measures high-level durations."""

    def __init__(self) -> None:
        self._timestamps: Dict[str, float] = {}
        self.logger = logging.getLogger(__name__)

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
]
