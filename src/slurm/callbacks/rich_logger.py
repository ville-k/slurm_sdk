"""RichLoggerCallback — rich terminal UI for job lifecycle events."""

from __future__ import annotations

import logging
import signal
import sys
from typing import Any, Dict, Optional, TYPE_CHECKING, cast

from ..logging import configure_logging as configure_sdk_logging
from .base import BaseCallback
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

try:  # pragma: no cover - rich is a hard dependency of the SDK
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
except Exception:  # pragma: no cover - fallback for minimal environments
    Console = None  # type: ignore
    Progress = None  # type: ignore
    BarColumn = None  # type: ignore
    TextColumn = None  # type: ignore
    TimeElapsedColumn = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from rich.progress import TaskID

    from ..job import Job


class RichLoggerCallback(BaseCallback):
    """Log lifecycle transitions with rich progress bars and formatted output.

    This callback provides an enhanced terminal experience with:
    - Animated progress bars showing lifecycle phase completion
    - Live job status updates
    - Formatted exception tracebacks
    - Rich text formatting

    Args:
        console: Optional rich Console instance. If not provided, creates a new one.
        poll_interval: Polling interval in seconds for job status updates (default: 2.0)
        log_level: Logging level applied on the runner when configure_logging is True.
        configure_logging: If True (default), configure runner logging before execution.
        verbose: If True, show detailed line-by-line logging output during packaging.
                 If False (default), show only the most recent packaging output line
                 in the progress bar for a cleaner display.
    """

    poll_interval_secs: Optional[float] = 2.0

    # Phase progression mapping: phase -> completion percentage
    _PHASE_PROGRESS = {
        "PENDING": 0,
        "PACKAGING": 10,
        "PACKAGED": 20,
        "SUBMITTING": 30,
        "SUBMITTED": 40,
        "PENDING_SLURM": 45,  # Job pending in SLURM queue
        "CONFIGURING": 50,
        "RUNNING": 60,
        "COMPLETING": 80,
        "COMPLETED": 100,
        "FAILED": 100,
        "CANCELLED": 100,
        "TIMEOUT": 100,
    }

    def __init__(
        self,
        *,
        console: Optional["Console"] = None,
        poll_interval: Optional[float] = None,
        verbose: bool = False,
        log_level: int = logging.INFO,
        configure_logging: bool = True,
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
        self._verbose = verbose
        self.log_level = log_level
        self.configure_logging = configure_logging
        self._logging_configured = False

        # Suppress verbose paramiko logging
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        self._progress: Optional[Progress] = None
        self._progress_task_id: Optional[TaskID] = None
        self._job_label: Optional[str] = None
        self._task_name: Optional[str] = None  # Store original task name
        self._job_id: Optional[str] = None  # Store actual job ID
        self._current_job: Optional["Job"] = None  # Store job for cancellation
        self._last_state: Optional[str] = None
        self._phase: Optional[str] = None
        # Track workflow nesting depth for indented logging
        self._workflow_depth: Dict[str, int] = {}
        # Track latest packaging output for non-verbose mode
        self._latest_packaging_output: Optional[str] = None
        # Signal handling for Ctrl-C detach and Ctrl-\ cancel
        self._original_sigint_handler = None
        self._original_sigquit_handler = None
        self._waiting_for_job = False

    def _sigquit_handler(self, signum: int, frame: Any) -> None:
        r"""Cancel remote job on first Ctrl-\, allow default behavior on second."""
        if self._waiting_for_job and self._current_job and self._job_id:
            try:
                self.console.print(
                    f"\n[yellow]Cancelling job {self._job_id}...[/yellow]"
                )
                self._current_job.cancel()
                self.console.print(
                    "[dim]Press Ctrl-\\ again to kill the process.[/dim]\n"
                )

                # Restore SIGQUIT handler so next Ctrl-\ uses default behavior
                if self._original_sigquit_handler is not None:
                    signal.signal(signal.SIGQUIT, self._original_sigquit_handler)
                    self._original_sigquit_handler = None
            except Exception as e:
                self.console.print(f"[red]Failed to cancel job: {e}[/red]\n")

    def _sigint_handler(self, signum: int, frame: Any) -> None:
        """Detach from running job, leaving it running on the cluster."""
        if self._waiting_for_job:
            self.console.print("\n[yellow]Detaching from job...[/yellow]")

            if self._progress:
                self._progress.stop()
                self._progress = None

            self._restore_signal_handlers()

            if self._job_id:
                self.console.print(
                    f"[dim]Job {self._job_id} continues running on the cluster.[/dim]"
                )
            self.console.print(
                "[dim]Use 'scancel <job_id>' to cancel the job if needed.[/dim]\n"
            )

            sys.exit(0)
        else:
            if self._original_sigint_handler:
                if callable(self._original_sigint_handler):
                    cast(Any, self._original_sigint_handler)(signum, frame)
                else:
                    sys.exit(1)

    def _install_signal_handlers(self) -> None:
        r"""Install Ctrl-C (detach) and Ctrl-\ (cancel) handlers."""
        if self._original_sigint_handler is None:
            self._original_sigint_handler = signal.signal(
                signal.SIGINT, self._sigint_handler
            )
            self._original_sigquit_handler = signal.signal(
                signal.SIGQUIT, self._sigquit_handler
            )
            self._waiting_for_job = True

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        if self._original_sigint_handler is not None:
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            self._original_sigint_handler = None
        if self._original_sigquit_handler is not None:
            signal.signal(signal.SIGQUIT, self._original_sigquit_handler)
            self._original_sigquit_handler = None
        self._waiting_for_job = False

    def _format_description(self, phase: str) -> str:
        """Always show task name for consistent display throughout job lifecycle."""
        return self._task_name or "Job"

    def _ensure_progress(self) -> None:
        """Create and start the progress display if not already running."""
        if self._progress is None:
            from rich.table import Column

            # Bar width is 25% of terminal for consistent layout
            terminal_width = self.console.width or 120
            bar_width = max(20, int(terminal_width * 0.25))

            columns = [
                TimeElapsedColumn(),
                TextColumn("[bold]{task.description}[/bold]", style="cyan"),
                BarColumn(bar_width=bar_width, table_column=Column(no_wrap=True)),
                TextColumn(
                    "[cyan]{task.fields[phase_name]}[/cyan]",
                    table_column=Column(width=12),
                ),
                TextColumn(
                    "{task.fields[phase_info]}",
                    style="dim",
                    table_column=Column(no_wrap=False),
                ),
            ]
            self._progress = Progress(*columns, console=self.console, transient=False)
            self._progress.start()

            phase = self._phase or "PENDING"
            progress_pct = self._PHASE_PROGRESS.get(phase, 0)
            description = self._format_description(phase)

            self._progress_task_id = self._progress.add_task(
                description,
                phase_name=phase,
                phase_info="",
                total=100,
                completed=progress_pct,
            )
        elif self._progress_task_id is not None:
            phase = self._phase or "PENDING"
            description = self._format_description(phase)
            self._progress.update(
                self._progress_task_id, description=description, phase_name=phase
            )

    def _update_progress(self, state: str, phase_info: Optional[str] = None) -> None:
        """Update the progress bar with new state and phase info."""
        if self._progress is None or self._progress_task_id is None:
            return

        progress_pct = self._PHASE_PROGRESS.get(state, 0)
        formatted_description = self._format_description(state)
        display_info = phase_info if phase_info is not None else ""

        self._progress.update(
            self._progress_task_id,
            description=formatted_description,
            phase_name=state,
            completed=progress_pct,
            phase_info=display_info,
        )

    def _set_phase(self, state: str, phase_info: Optional[str] = None) -> None:
        """Set the current phase and update progress display."""
        self._phase = state.upper()
        self._ensure_progress()
        self._update_progress(self._phase, phase_info=phase_info)

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
                    progress.update(self._progress_task_id, completed=100)
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

    def update_packaging_output(self, line: str) -> None:
        """Update packaging output (e.g., Docker build logs).

        In non-verbose mode, only the latest line is shown in the progress bar.
        In verbose mode, each line is logged immediately.

        Args:
            line: A single line of packaging command output
        """
        if self._verbose:
            # In verbose mode, log each line immediately
            self.console.print(f"  [dim]{line}[/dim]")
        else:
            # In non-verbose mode, just track the latest line for the progress bar
            self._latest_packaging_output = line
            # Update the progress display with the latest output
            if self._progress and self._progress_task_id is not None:
                # Show the latest output line in the phase_info field (truncated if needed)
                display_line = line[:80] + "..." if len(line) > 80 else line

                # Keep the same progress percentage, just update the phase_info
                progress_pct = self._PHASE_PROGRESS.get(self._phase or "PACKAGING", 10)
                self._progress.update(
                    self._progress_task_id,
                    completed=progress_pct,
                    phase_info=display_line,
                )

    def on_begin_package_ctx(self, ctx: PackagingBeginContext) -> None:
        task_name = getattr(ctx.task, "sbatch_options", {}).get(
            "job_name", getattr(getattr(ctx.task, "func", ctx.task), "__name__", "task")
        )
        # Store task name for consistent display
        self._task_name = task_name
        self._set_job_label(task_name)
        self._set_phase("PACKAGING")
        self._latest_packaging_output = None
        if self._verbose:
            self._log(f"[cyan]Packaging task[/cyan] '{task_name}'")

    def on_end_package_ctx(self, ctx: PackagingEndContext) -> None:
        duration = f" in {ctx.duration:.2f}s" if ctx.duration is not None else ""
        self._set_phase("PACKAGED")
        self._latest_packaging_output = None
        if self._verbose:
            self._log(f"[green]✓[/green] Packaging finished{duration}")
        else:
            # In non-verbose mode, show a minimal completion message
            self._update_progress("PACKAGED")

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext) -> None:
        self._set_phase("SUBMITTING")
        self._log(
            f"[cyan]Submitting job[/cyan] {ctx.pre_submission_id} via {ctx.backend_type or 'unknown'} "
            f"(partition={ctx.sbatch_options.get('partition', 'default')})"
        )

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        self._job_id = ctx.job_id
        self._current_job = ctx.job
        self._set_phase("SUBMITTED")

        self.logger.debug(
            "Job %s submitted as %s via %s",
            ctx.pre_submission_id,
            ctx.job_id,
            ctx.backend_type or "unknown",
        )

        stdout_path = getattr(ctx.job, "stdout_path", None)
        stderr_path = getattr(ctx.job, "stderr_path", None)
        directory = ctx.target_job_dir

        self.console.print("\n[bold]Job launch summary:[/bold]")
        self.console.print(f"  Job ID:    [bold cyan]{ctx.job_id}[/bold cyan]")
        self.console.print(f"  Directory: [dim]{directory or 'unknown'}[/dim]")
        self.console.print(f"  Stdout:    [dim]{stdout_path or 'unknown'}[/dim]")
        self.console.print(f"  Stderr:    [dim]{stderr_path or 'unknown'}[/dim]")
        self.console.print(
            "\n[dim]Press Ctrl-C to detach from running job. Press Ctrl-\\ to cancel the job.[/dim]\n"
        )

        self._install_signal_handlers()

    def on_job_status_update_ctx(self, ctx: JobStatusUpdatedContext) -> None:
        state = ctx.status.get("JobState") or "UNKNOWN"
        if state == self._last_state:
            return
        self._last_state = state
        self._set_phase(state, phase_info="")

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        self._configure_runner_logging(use_rich=True)
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

        self._restore_signal_handlers()

        state = (ctx.job_state or "UNKNOWN").upper()
        exit_code = ctx.exit_code or "?"

        if state == "COMPLETED" and exit_code in ("0:0", "0"):
            completion_msg = (
                f"✓ Job {ctx.job_id} completed successfully (exit={exit_code})"
            )
            self._set_phase(state, phase_info=completion_msg)
        elif state == "FAILED":
            completion_msg = (
                f"✗ Job {ctx.job_id} failed (state={state}, exit={exit_code})"
            )
            self._set_phase(state, phase_info=completion_msg)
        else:
            completion_msg = (
                f"Job {ctx.job_id} finished with state={state} exit={exit_code}"
            )
            self._set_phase(state, phase_info=completion_msg)

        self._stop_progress()

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

    def _configure_runner_logging(self, *, use_rich: bool) -> None:
        if not self.configure_logging or self._logging_configured:
            return
        try:
            configure_sdk_logging(level=self.log_level, use_rich=use_rich)
            self._logging_configured = True
        except Exception:  # pragma: no cover - best effort
            # console may not be initialized yet; use logger for fallback
            self.logger.warning("Failed to configure logging on runner", exc_info=True)
