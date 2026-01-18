"""Detail panel widget for showing selected item information."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from ..data import JobInfo, PartitionInfo
from ...common.styles import get_state_color
from ...common.widgets import EmptyState, KeyValue, SectionHeader


class DetailPanel(VerticalScroll):
    """Panel showing details for the selected tree item."""

    DEFAULT_CSS = """
    DetailPanel {
        width: 100%;
        height: 100%;
        padding: 1;
        background: $surface;
    }

    DetailPanel .detail-title {
        text-style: bold;
        color: $text;
        padding: 0 0 1 0;
    }

    DetailPanel .action-hint {
        color: $text-muted;
        text-style: italic;
        padding: 1 0;
    }
    """

    def __init__(self, **kwargs) -> None:
        """Initialize the detail panel."""
        super().__init__(**kwargs)
        self._current_job: JobInfo | None = None

    def compose(self) -> ComposeResult:
        """Compose initial empty state."""
        yield EmptyState("Select an item to view details")

    def show_job(self, job: JobInfo) -> None:
        """Display job details.

        Args:
            job: Job information to display.
        """
        self._current_job = job
        self.remove_children()

        color = get_state_color(job.state)

        self.mount(Static(f"[bold]Job: {job.job_id}[/bold]", classes="detail-title"))
        self.mount(KeyValue("State", f"[{color}]{job.state}[/{color}]"))
        self.mount(KeyValue("Name", job.name or "-"))
        self.mount(KeyValue("Partition", job.partition or "-"))
        self.mount(KeyValue("Account", job.account or "-"))
        self.mount(KeyValue("User", job.user or "-"))

        self.mount(SectionHeader("Time"))
        self.mount(KeyValue("Submitted", job.submit_time or "-"))
        self.mount(KeyValue("Started", job.start_time or "-"))
        if job.end_time:
            self.mount(KeyValue("Ended", job.end_time))
        self.mount(KeyValue("Runtime", job.time or "-"))
        self.mount(KeyValue("Time Limit", job.time_limit or "-"))

        self.mount(SectionHeader("Resources"))
        self.mount(KeyValue("Nodes", job.nodes or "-"))
        self.mount(KeyValue("CPUs", job.num_cpus or "-"))

        if job.work_dir:
            self.mount(SectionHeader("Paths"))
            self.mount(KeyValue("Work Dir", job.work_dir))

        if job.reason:
            self.mount(SectionHeader("Status"))
            self.mount(KeyValue("Reason", job.reason))

        if job.exit_code and job.exit_code != "0:0":
            self.mount(KeyValue("Exit Code", job.exit_code))

        # Action hints
        base_state = job.state.split()[0].upper() if job.state else ""
        if base_state in ("RUNNING", "PENDING"):
            self.mount(Static("\n[C]ancel job  [R]efresh", classes="action-hint"))
        else:
            self.mount(Static("\n[R]efresh", classes="action-hint"))

    def show_partition(self, partition: PartitionInfo) -> None:
        """Display partition details.

        Args:
            partition: Partition information to display.
        """
        self._current_job = None
        self.remove_children()

        if partition.is_up:
            status = "[green]UP[/green]"
        else:
            status = "[red]DOWN[/red]"

        self.mount(
            Static(f"[bold]Partition: {partition.name}[/bold]", classes="detail-title")
        )
        self.mount(KeyValue("Status", status))
        self.mount(KeyValue("Total Nodes", str(partition.total_nodes)))
        self.mount(KeyValue("Time Limit", partition.time_limit or "-"))

        if partition.node_states:
            self.mount(SectionHeader("Node States"))
            for state, count in sorted(partition.node_states.items()):
                self.mount(KeyValue(state, str(count)))

        if partition.cpus:
            self.mount(SectionHeader("Resources"))
            self.mount(KeyValue("CPUs/Node", partition.cpus))
            if partition.memory:
                self.mount(KeyValue("Memory/Node", partition.memory))

    def show_user_summary(self, user: str, job_count: int) -> None:
        """Display user summary.

        Args:
            user: Username.
            job_count: Number of jobs for this user.
        """
        self._current_job = None
        self.remove_children()

        self.mount(Static(f"[bold]User: {user}[/bold]", classes="detail-title"))
        self.mount(KeyValue("Jobs", str(job_count)))
        self.mount(
            Static(
                "\nExpand this node to see individual jobs.",
                classes="action-hint",
            )
        )

    def show_section_summary(self, title: str, count: int, description: str) -> None:
        """Display section summary.

        Args:
            title: Section title.
            count: Item count.
            description: Description text.
        """
        self._current_job = None
        self.remove_children()

        self.mount(Static(f"[bold]{title}[/bold]", classes="detail-title"))
        self.mount(KeyValue("Total", str(count)))
        self.mount(Static(f"\n{description}", classes="action-hint"))

    def show_empty(self) -> None:
        """Show empty state."""
        self._current_job = None
        self.remove_children()
        self.mount(EmptyState("Select an item to view details"))

    def get_current_job(self) -> JobInfo | None:
        """Get the currently displayed job.

        Returns:
            Current job info, or None if not showing a job.
        """
        return self._current_job
