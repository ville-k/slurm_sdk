import argparse
import logging
import os
import pathlib
import time
import tempfile
from typing import List, Tuple, Optional

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich import box

from slurm.callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
    PackagingBeginContext,
    PackagingEndContext,
    RunBeginContext,
    RunEndContext,
    SubmitBeginContext,
    SubmitEndContext,
)
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging

DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.container_example.toml")

log = logging.getLogger(__name__)


@task(name="callback_example", time="00:02:00", mem="1G", ntasks=1, nodes=1)
def callback_example(x: int) -> int:
    """A tiny task used for the demo."""
    time.sleep(1)
    return x + 1


class PreflightCallback(BaseCallback):
    """Rich UI callback for a pleasant launch experience.

    Why this design:
    - A single, hierarchical table reflects how humans think about launches (phases → items),
      and keeps the output anchored while the UI updates.
    - We render dimmed placeholders for future items to signal intent and avoid layout jumps.
    - The callback owns the Live screen and relies on SDK-managed polling so applications
      don't manage threads or lifecycle details.
    - When the job finishes, we render a final (non-live) snapshot so results persist in logs.
    """

    poll_interval_secs = 2.0

    def __init__(self, console: Optional[Console] = None) -> None:
        self.console = console or Console()
        # (name, status) where status in {pending, running, ok, fail}
        self._phases: List[Tuple[str, str]] = [
            ("Packaging", "pending"),
            ("Submitting", "pending"),
            ("Running", "pending"),
            ("Completed", "pending"),
        ]
        self._job_id = "-"
        self._job_dir = "-"
        self._job_state = "pending"
        self._exit_code = ""
        self._error_message = ""
        self._packaging_strategy = "-"
        self._package_duration: Optional[float] = None
        self._backend_name = "-"
        self._submit_partition = "-"
        self._submit_account = "-"
        # validations (name, status)
        self._validations: List[Tuple[str, str]] = [
            ("W&B connectivity", "pending"),
            ("Configuration file valid", "pending"),
            ("Data file path accessible", "pending"),
        ]
        self._live: Optional[Live] = None
        self._last_status: dict = {}
        self._completed = False
        self._cluster: Optional[Cluster] = None
        self._job = None

    def start(self) -> None:
        """Draw initial skeleton so the layout is stable before logs arrive."""
        self._ensure_live()

    # --- helpers ---
    def _render_table(self) -> Table:
        table = Table(title="Launch & Pre-flight", box=box.SIMPLE_HEAVY)
        table.add_column("Phase", style="bold")
        table.add_column("Item")
        table.add_column("Details")

        def icon_ok() -> str:
            return "[green]✓[/green]"

        def icon_fail() -> str:
            return "[red]×[/red]"

        def icon_spin() -> str:
            return "[yellow]⟳[/yellow]"

        def fmt_job_state(state: str) -> str:
            s = (state or "").upper()
            if s in ("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
                return f"[red]{s}[/red]"
            if s in ("COMPLETED",):
                return f"[green]{s}[/green]"
            if s in ("RUNNING", "PENDING", "CONFIGURING", "COMPLETING"):
                return f"[yellow]{s}[/yellow]"
            return s

        # Packaging phase group
        table.add_row("Packaging", "", "")
        # Future (not-running) items grayed out
        # Validation rows grouped under Packaging
        for name, status in self._validations:
            if status == "pending":
                details = f"[dim]{icon_spin()} Checking[/dim]"
            elif status == "running":
                details = f"[yellow]{icon_spin()} Checking[/yellow]"
            elif status == "ok":
                details = f"{icon_ok()} Passed"
            else:
                # Treat any other status as failure and show message if recorded
                fail_msg = self._error_message or "Failed"
                details = f"{icon_fail()} {fail_msg}"
            item = (
                f"Validate {name}"
                if status != "pending"
                else f"[dim]Validate {name}[/dim]"
            )
            table.add_row("", item, details)

        if self._packaging_strategy and self._packaging_strategy != "-":
            table.add_row(
                "", "Strategy", f"[italic]{self._packaging_strategy}[/italic]"
            )
        if self._package_duration is not None:
            table.add_row(
                "",
                "Duration",
                f"{self._package_duration:.2f}s",
            )

        # Submitting phase group
        sub_status = next(s for n, s in self._phases if n == "Submitting")
        table.add_row("Submitting", "", "")
        sub_details = (
            icon_ok()
            if sub_status == "ok"
            else (
                f"[dim]{icon_spin()} Pending[/dim]"
                if sub_status == "pending"
                else f"{icon_spin()} In-progress"
            )
        )
        table.add_row("", "Submit job request", sub_details)
        if self._backend_name != "-":
            table.add_row("", "Backend", self._backend_name)
        if self._submit_partition != "-":
            table.add_row("", "Partition", self._submit_partition)
        if self._submit_account != "-":
            table.add_row("", "Account", self._submit_account)

        # Running phase group
        run_status = next(s for n, s in self._phases if n == "Running")
        table.add_row("Running", "", "")
        job_id_item = "Job ID" if run_status != "pending" else "[dim]Job ID[/dim]"
        job_id_val = (
            f"[bold]{self._job_id}[/bold]"
            if self._job_id != "-"
            else ("[dim]pending[/dim]" if run_status == "pending" else "")
        )
        state_item = "Job State" if run_status != "pending" else "[dim]Job State[/dim]"
        state_val = (
            fmt_job_state(self._job_state)
            if run_status != "pending"
            else "[dim]pending[/dim]"
        )
        dir_item = "Target Dir" if run_status != "pending" else "[dim]Target Dir[/dim]"
        dir_val = (
            f"[bold]{self._job_dir}[/bold]"
            if self._job_dir != "-"
            else ("[dim]pending[/dim]" if run_status == "pending" else "")
        )
        # Order: Job ID, Job State, Target Dir
        table.add_row("", job_id_item, job_id_val)
        table.add_row("", state_item, state_val)
        table.add_row("", dir_item, dir_val)

        # Completed phase group
        comp_status = next(s for n, s in self._phases if n == "Completed")
        table.add_row("Completed", "", "")
        # Only render non-dim details after job has completed
        if comp_status == "pending":
            table.add_row("", "[dim]Final Status[/dim]", "[dim]pending[/dim]")
        else:
            # Final status under Completed with extra info from last status
            if comp_status == "ok":
                final_details = f"{icon_ok()} Success"
            else:
                msg = self._error_message or (
                    f"ExitCode: {self._exit_code}" if self._exit_code else "Failed"
                )
                final_details = f"{icon_fail()} {msg}"
            table.add_row("", "Final Status", final_details)
            # Additional info from scheduler response if available
            if self._last_status:
                exitc = self._last_status.get("ExitCode", "")
                reason = self._last_status.get("Reason", "") or self._last_status.get(
                    "Error", ""
                )
                if exitc:
                    table.add_row("", "Exit Code", exitc)
                if reason:
                    table.add_row("", "Reason", reason)

        return table

    def _ensure_live(self) -> None:
        if self._live is None:
            self._live = Live(
                self._render_table(),
                refresh_per_second=8,
                console=self.console,
                screen=True,
                redirect_stdout=True,
                redirect_stderr=True,
                transient=False,
            )
            self._live.start()
        else:
            self._live.update(self._render_table())

    def _set_phase(self, name: str, status: str) -> None:
        self._phases = [(n, (status if n == name else s)) for (n, s) in self._phases]
        self._ensure_live()
        time.sleep(0.1)

    def set_job_state(self, state: str, exit_code: Optional[str] = None) -> None:
        self._job_state = state or ""
        if exit_code is not None:
            self._exit_code = exit_code
        # If running detected, ensure phase marked running
        if state in ("RUNNING", "PENDING", "CONFIGURING"):
            self._set_phase("Running", "running")
        self._ensure_live()

    # --- typed hooks ---
    def on_begin_package_ctx(self, ctx: PackagingBeginContext) -> None:
        if ctx.cluster is not None:
            self._cluster = ctx.cluster
        if isinstance(ctx.packaging_config, dict):
            pkg_type = ctx.packaging_config.get("type") or "auto"
            self._packaging_strategy = pkg_type
        self._ensure_live()
        self._set_phase("Packaging", "running")

    # Override to capture richer details once packaging completes
    def on_end_package_ctx(self, ctx: PackagingEndContext) -> None:
        strategy = ctx.packaging_result
        if strategy is not None:
            self._packaging_strategy = type(strategy).__name__
        if ctx.duration is not None:
            self._package_duration = ctx.duration
        # Run fake validations with visible progression
        # Mark validation rows as running, then ok
        self._validations[0] = (self._validations[0][0], "running")
        self._ensure_live()
        time.sleep(0.5)
        self._validations[0] = (self._validations[0][0], "ok")
        self._ensure_live()
        time.sleep(0.2)
        self._validations[1] = (self._validations[1][0], "running")
        self._ensure_live()
        time.sleep(1.5)
        self._validations[1] = (self._validations[1][0], "ok")
        self._ensure_live()
        time.sleep(0.2)
        self._validations[2] = (self._validations[2][0], "running")
        self._ensure_live()
        time.sleep(1.5)
        self._validations[2] = (self._validations[2][0], "ok")
        self._set_phase("Packaging", "ok")
        self._set_phase("Submitting", "running")

    def on_begin_submit_job_ctx(self, ctx: SubmitBeginContext) -> None:
        self._backend_name = ctx.backend_type or "unknown"
        self._submit_partition = ctx.sbatch_options.get("partition", "-")
        self._submit_account = ctx.sbatch_options.get("account", "-")
        self._job_dir = ctx.target_job_dir
        self._ensure_live()

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        self._job_id = ctx.job_id
        self._job = ctx.job
        self._set_phase("Submitting", "ok")
        self._set_phase("Running", "running")

    def on_begin_run_job_ctx(self, ctx: RunBeginContext) -> None:
        # If sent to runner, we would update; typically this UI callback is not pickled.
        self._ensure_live()

    def on_end_run_job_ctx(self, ctx: RunEndContext) -> None:
        if ctx.error_message:
            self._error_message = ctx.error_message

    def on_job_status_update_ctx(self, ctx: JobStatusUpdatedContext) -> None:
        self._last_status = ctx.status or {}
        state = ctx.status.get("JobState", "")
        exit_code = ctx.status.get("ExitCode")
        if state:
            self.set_job_state(state, exit_code)

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        if ctx.emitted_by is ExecutionLocus.RUNNER or self._completed:
            return
        self._completed = True
        if ctx.status:
            self._last_status = ctx.status
        if ctx.error_message and not self._error_message:
            self._error_message = ctx.error_message
        if ctx.job_dir:
            self._job_dir = ctx.job_dir
        self.set_job_state(ctx.job_state or "", ctx.exit_code)
        phase_status = "ok" if (ctx.job_state or "").upper() == "COMPLETED" else "fail"
        self._set_phase("Running", phase_status)
        self._set_phase("Completed", phase_status)
        self._finalize_display()
        if phase_status == "fail":
            self._show_error_log(ctx)

    def _finalize_display(self) -> None:
        if self._live is None:
            return
        final_table = self._render_table()
        self._live.update(final_table)
        self._live.stop()
        self._live = None
        try:
            self.console.print(final_table)
        except Exception:
            pass

    def _show_error_log(self, ctx: CompletedContext) -> None:
        path = ctx.stderr_path
        if not path:
            self.console.print("[red]No stderr log available for this job.[/red]")
            return

        job = ctx.job or self._job
        cluster = self._cluster or (job.cluster if job else None)
        backend = getattr(cluster, "backend", None) if cluster else None

        content: Optional[str] = None

        if backend and hasattr(backend, "download_file"):
            try:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    temp_path = tmp.name
                try:
                    backend.download_file(path, temp_path)
                    with open(temp_path, "r", encoding="utf-8", errors="replace") as fh:
                        content = fh.read()
                finally:
                    os.unlink(temp_path)
            except Exception as exc:
                log.debug("Failed to download stderr log '%s': %s", path, exc)

        if content is None:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    content = fh.read()
            except Exception as exc:
                log.debug("Failed to read stderr log locally '%s': %s", path, exc)

        if not content:
            self.console.print(f"[red]stderr log unavailable or empty at {path}[/red]")
            return

        tail = "\n".join(content.strip().splitlines()[-20:])
        self.console.print("[red]Job stderr tail:[/red]")
        self.console.print(tail or "(empty)")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the example script."""

    parser = argparse.ArgumentParser(
        description=(
            "Submit the hello_container task using a Slurmfile that configures "
            "the container packaging strategy."
        )
    )
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load (defaults to the bundled example).",
    )
    parser.add_argument(
        "--env",
        default="local",
        help=("Environment key within the Slurmfile to load (defaults to 'local')."),
    )
    return parser


def main():
    console = Console()
    parser = build_parser()
    args = parser.parse_args()
    configure_logging()

    callback = PreflightCallback(console=console)

    cluster = Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[callback],
    )

    env_submit = cluster.environment_config["submit"]

    job = callback_example.submit(
        cluster=cluster,
        packaging=cluster.packaging_defaults,
        account=env_submit.get("account"),
        partition=env_submit.get("partition"),
    )(41)

    job.wait()
    if job.is_successful():
        result = job.get_result()
        console.print("[bold green]Result:[/bold green] %s" % result)
    else:
        exit_code = job.get_status().get("ExitCode")
        console.print(
            "[bold red]Job failed:[/bold red] exit_code=%s" % (exit_code or "unknown")
        )


if __name__ == "__main__":
    main()
