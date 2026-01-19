"""Dashboard TUI application for monitoring SLURM jobs and clusters."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.widgets import Footer, Header, Static

from .data import DashboardDataProvider
from .widgets.cluster_tree import ClusterTree, NodeType
from .widgets.detail_panel import DetailPanel
from .widgets.status_bar import StatusBar
from ..common.styles import DASHBOARD_CSS

if TYPE_CHECKING:
    from slurm.cluster import Cluster


class DashboardApp(App[None]):
    """Interactive TUI dashboard for monitoring SLURM jobs and cluster status."""

    TITLE = "SLURM Dashboard"
    CSS = DASHBOARD_CSS

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("r", "refresh", "Refresh", priority=True),
        Binding("a", "toggle_auto_refresh", "Auto-refresh", priority=True),
        Binding("c", "cancel_job", "Cancel Job", priority=True),
        Binding("escape", "unfocus", "Unfocus", show=False),
    ]

    def __init__(
        self,
        cluster: "Cluster",
        refresh_interval: int = 30,
        **kwargs,
    ) -> None:
        """Initialize the dashboard application.

        Args:
            cluster: Configured Cluster instance for data access.
            refresh_interval: Auto-refresh interval in seconds.
            **kwargs: Additional App arguments.
        """
        super().__init__(**kwargs)
        self._cluster = cluster
        self._refresh_interval = refresh_interval
        self._data_provider = DashboardDataProvider(cluster)
        self._auto_refresh = True
        self._refresh_timer = None

    def compose(self) -> ComposeResult:
        """Compose the dashboard layout."""
        yield Header()

        with Horizontal(id="main-container"):
            with Container(id="nav-panel", classes="nav-panel"):
                yield Static("Navigation", classes="panel-title")
                yield ClusterTree(id="cluster-tree")

            with Container(id="content-panel", classes="content-panel"):
                yield Static("Details", classes="panel-title")
                yield DetailPanel(id="detail-panel")

        yield StatusBar(id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        """Handle app mount - start initial data load and auto-refresh."""
        self._do_refresh()
        self._start_auto_refresh()
        self.query_one(ClusterTree).focus()

    def _start_auto_refresh(self) -> None:
        """Start auto-refresh timer if enabled."""
        if self._auto_refresh and self._refresh_timer is None:
            self._refresh_timer = self.set_interval(
                self._refresh_interval,
                self._auto_refresh_callback,
            )

    def _stop_auto_refresh(self) -> None:
        """Stop auto-refresh timer."""
        if self._refresh_timer is not None:
            self._refresh_timer.stop()
            self._refresh_timer = None

    def _auto_refresh_callback(self) -> None:
        """Callback for auto-refresh timer."""
        if self._auto_refresh:
            self._do_refresh()

    def _do_refresh(self) -> None:
        """Refresh data from cluster."""
        status_bar = self.query_one(StatusBar)

        try:
            data = self._data_provider.refresh()

            # Update tree
            tree = self.query_one(ClusterTree)
            tree.update_data(data)

            # Update status bar
            status_bar.last_refresh = data.last_refresh
            status_bar.error_message = data.error

        except Exception as e:
            status_bar.error_message = str(e)
            status_bar.last_refresh = datetime.now()

    def on_tree_node_selected(self, event: ClusterTree.NodeSelected) -> None:
        """Handle tree node selection to update detail panel."""
        detail_panel = self.query_one(DetailPanel)
        node_data = event.node.data

        if node_data is None:
            detail_panel.show_empty()
            return

        if node_data.node_type == NodeType.JOB and node_data.job:
            detail_panel.show_job(node_data.job)

        elif node_data.node_type == NodeType.PARTITION and node_data.partition:
            detail_panel.show_partition(node_data.partition)

        elif node_data.node_type == NodeType.USER and node_data.user:
            # Count jobs for this user
            data = self._data_provider.data
            job_count = len(data.account_jobs.get(node_data.user, []))
            detail_panel.show_user_summary(node_data.user, job_count)

        elif node_data.node_type == NodeType.MY_JOBS:
            data = self._data_provider.data
            detail_panel.show_section_summary(
                "My Jobs",
                len(data.my_jobs),
                "Your currently queued and running jobs.",
            )

        elif node_data.node_type == NodeType.ACCOUNT_JOBS:
            data = self._data_provider.data
            total = sum(len(jobs) for jobs in data.account_jobs.values())
            detail_panel.show_section_summary(
                "Account Jobs",
                total,
                "Jobs from other users in your account.",
            )

        elif node_data.node_type == NodeType.CLUSTER_STATUS:
            data = self._data_provider.data
            detail_panel.show_section_summary(
                "Cluster Status",
                len(data.partitions),
                "Partition availability and node states.",
            )

        else:
            detail_panel.show_empty()

    def action_refresh(self) -> None:
        """Manual refresh action."""
        self._do_refresh()
        self.notify("Data refreshed", severity="information", timeout=2)

    def action_toggle_auto_refresh(self) -> None:
        """Toggle auto-refresh on/off."""
        self._auto_refresh = not self._auto_refresh
        status_bar = self.query_one(StatusBar)
        status_bar.auto_refresh = self._auto_refresh

        if self._auto_refresh:
            self._start_auto_refresh()
            self.notify("Auto-refresh enabled", severity="information", timeout=2)
        else:
            self._stop_auto_refresh()
            self.notify("Auto-refresh disabled", severity="information", timeout=2)

    def action_cancel_job(self) -> None:
        """Cancel the currently selected job."""
        detail_panel = self.query_one(DetailPanel)
        job = detail_panel.get_current_job()

        if job is None:
            self.notify("No job selected", severity="warning", timeout=2)
            return

        base_state = job.state.split()[0].upper() if job.state else ""
        if base_state not in ("RUNNING", "PENDING"):
            self.notify(
                f"Cannot cancel job in state: {job.state}",
                severity="warning",
                timeout=2,
            )
            return

        # Show confirmation
        self._confirm_cancel(job.job_id)

    def _confirm_cancel(self, job_id: str) -> None:
        """Show cancel confirmation and execute if confirmed.

        Args:
            job_id: Job ID to cancel.
        """
        # For now, directly cancel (could add a modal confirmation in future)
        success = self._data_provider.cancel_job(job_id)

        if success:
            self.notify(f"Job {job_id} cancelled", severity="information", timeout=2)
            self._do_refresh()
        else:
            self.notify(
                f"Failed to cancel job {job_id}",
                severity="error",
                timeout=3,
            )

    def action_unfocus(self) -> None:
        """Remove focus from current widget."""
        self.query_one(ClusterTree).focus()
