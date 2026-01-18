"""Live jobs dashboard for monitoring SLURM jobs."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from ...cluster import Cluster

STATE_COLORS: Dict[str, str] = {
    "RUNNING": "green",
    "PENDING": "yellow",
    "COMPLETED": "blue",
    "FAILED": "red",
    "CANCELLED": "magenta",
    "TIMEOUT": "red",
    "NODE_FAIL": "red",
    "COMPLETING": "cyan",
    "CONFIGURING": "cyan",
    "SUSPENDED": "yellow",
}


def _get_state_color(state: str) -> str:
    """Get color for job state."""
    base_state = state.split()[0] if state else ""
    return STATE_COLORS.get(base_state, "white")


class JobsDashboard:
    """Live dashboard for monitoring SLURM jobs."""

    def __init__(
        self,
        cluster: "Cluster",
        job_filter: Optional[str] = None,
        account: Optional[str] = None,
        poll_interval: float = 5.0,
    ) -> None:
        """Initialize the jobs dashboard.

        Args:
            cluster: Cluster instance for querying job status.
            job_filter: Optional job ID or job name to filter on.
            account: Optional account/user to filter jobs.
            poll_interval: Seconds between queue polls.
        """
        self.cluster = cluster
        self.job_filter = job_filter
        self.account = account
        self.poll_interval = poll_interval
        self.console = Console()
        self._last_update: Optional[float] = None

    def _filter_jobs(self, queue: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter jobs based on job_filter and account."""
        filtered = queue

        if self.account:
            filtered = [
                j
                for j in filtered
                if str(j.get("USER", "") or j.get("UserId", "")) == self.account
            ]

        if self.job_filter:
            result = []
            for job in filtered:
                job_id = str(
                    job.get("JOBID", "")
                    or job.get("JobID", "")
                    or job.get("job_id", "")
                )
                job_name = str(job.get("NAME", "") or job.get("JobName", "") or "")
                if self.job_filter in job_id or self.job_filter in job_name:
                    result.append(job)
            filtered = result

        return filtered

    def _build_jobs_table(self, jobs: List[Dict[str, Any]]) -> Table:
        """Build a Rich table for the jobs list."""
        table = Table(title="Jobs", expand=True)
        table.add_column("Job ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="white", max_width=30)
        table.add_column("State", style="white")
        table.add_column("User", style="dim")
        table.add_column("Time", style="dim")
        table.add_column("Partition", style="dim")
        table.add_column("Nodes", justify="right", style="dim")
        table.add_column("Reason", style="dim", max_width=20)

        for job in jobs:
            job_id = str(
                job.get("JOBID", "")
                or job.get("JobID", "")
                or job.get("job_id", "")
                or ""
            )
            name = str(job.get("NAME", "") or job.get("JobName", "") or "")
            state = str(job.get("STATE", "") or job.get("JobState", "") or "")
            user = str(job.get("USER", "") or job.get("UserId", "") or "")
            time_str = str(job.get("TIME", "") or job.get("RunTime", "") or "")
            partition = str(job.get("PARTITION", "") or job.get("Partition", "") or "")
            nodes = str(job.get("NODES", "") or job.get("NumNodes", "") or "")
            reason = str(job.get("REASON", "") or job.get("Reason", "") or "")

            color = _get_state_color(state)
            styled_state = f"[{color}]{state}[/{color}]"

            table.add_row(
                job_id, name, styled_state, user, time_str, partition, nodes, reason
            )

        return table

    def _build_single_job_panel(self, job_id: str) -> Panel:
        """Build a detailed panel for a single job."""
        try:
            job = self.cluster.get_job(job_id)
            status = job.get_status()
        except Exception as e:
            return Panel(
                f"[red]Error fetching job {job_id}: {e}[/red]",
                title=f"Job {job_id}",
                border_style="red",
            )

        state = status.get("JobState", "UNKNOWN")
        color = _get_state_color(state)

        details: List[str] = []
        details.append(f"[bold]State:[/bold] [{color}]{state}[/{color}]")

        field_mappings = [
            ("ExitCode", "Exit Code"),
            ("WorkDir", "Work Dir"),
            ("Partition", "Partition"),
            ("Account", "Account"),
            ("UserId", "User"),
            ("SubmitTime", "Submitted"),
            ("StartTime", "Started"),
            ("EndTime", "Ended"),
            ("RunTime", "Run Time"),
            ("TimeLimit", "Time Limit"),
            ("Reason", "Reason"),
            ("NumNodes", "Nodes"),
            ("NumCPUs", "CPUs"),
            ("NodeList", "Node List"),
        ]

        for key, label in field_mappings:
            if value := status.get(key):
                details.append(f"[bold]{label}:[/bold] {value}")

        panel_content = "\n".join(details)
        return Panel(panel_content, title=f"Job {job_id}", border_style=color)

    def _build_dashboard(self) -> Group:
        """Build the complete dashboard layout."""
        elements = []

        try:
            queue = self.cluster.get_queue()
            jobs = self._filter_jobs(queue)
        except Exception as e:
            elements.append(
                Panel(f"[red]Error fetching queue: {e}[/red]", title="Error")
            )
            return Group(*elements)

        if self.job_filter and len(jobs) == 1:
            job_id = str(
                jobs[0].get("JOBID", "")
                or jobs[0].get("JobID", "")
                or jobs[0].get("job_id", "")
            )
            elements.append(self._build_single_job_panel(job_id))
        elif jobs:
            elements.append(self._build_jobs_table(jobs))
        else:
            filter_desc = ""
            if self.job_filter:
                filter_desc += f" matching '{self.job_filter}'"
            if self.account:
                filter_desc += f" for account '{self.account}'"
            elements.append(
                Panel(
                    f"[dim]No jobs found{filter_desc}.[/dim]",
                    title="Jobs",
                )
            )

        self._last_update = time.time()
        status_text = Text()
        status_text.append("Last updated: ", style="dim")
        status_text.append(
            time.strftime("%H:%M:%S", time.localtime(self._last_update)), style="cyan"
        )
        status_text.append(" | ", style="dim")
        status_text.append("Press Ctrl+C to exit", style="dim")
        elements.append(status_text)

        return Group(*elements)

    def run(self) -> None:
        """Run the live dashboard until interrupted."""
        with Live(
            self._build_dashboard(),
            console=self.console,
            refresh_per_second=1,
            screen=False,
        ) as live:
            try:
                while True:
                    time.sleep(self.poll_interval)
                    live.update(self._build_dashboard())
            except KeyboardInterrupt:
                pass
