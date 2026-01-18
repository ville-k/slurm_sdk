"""Live cluster dashboard for monitoring SLURM partitions."""

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


class ClusterDashboard:
    """Live dashboard for monitoring SLURM cluster partitions."""

    def __init__(
        self,
        cluster: "Cluster",
        account: Optional[str] = None,
        poll_interval: float = 5.0,
    ) -> None:
        """Initialize the cluster dashboard.

        Args:
            cluster: Cluster instance for querying cluster info.
            account: Optional account to highlight resource usage for.
            poll_interval: Seconds between info polls.
        """
        self.cluster = cluster
        self.account = account
        self.poll_interval = poll_interval
        self.console = Console()
        self._last_update: Optional[float] = None

    def _aggregate_partitions(
        self, partitions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Aggregate partition rows by partition name with utilization stats."""
        aggregated: Dict[str, Dict[str, Any]] = {}

        for partition in partitions:
            name = str(
                partition.get("PARTITION", "") or partition.get("partition", "") or ""
            )
            if not name:
                continue

            nodes_str = str(
                partition.get("NODES", "") or partition.get("nodes", "") or "0"
            )
            try:
                nodes = int(nodes_str)
            except ValueError:
                nodes = 0

            state = str(
                partition.get("STATE", "") or partition.get("state", "") or ""
            ).lower()

            if name not in aggregated:
                aggregated[name] = {
                    "name": name,
                    "avail": str(
                        partition.get("AVAIL", "") or partition.get("avail", "") or ""
                    ),
                    "total_nodes": 0,
                    "allocated": 0,
                    "idle": 0,
                    "down": 0,
                    "other": 0,
                    "timelimit": str(
                        partition.get("TIMELIMIT", "")
                        or partition.get("timelimit", "")
                        or ""
                    ),
                }

            aggregated[name]["total_nodes"] += nodes

            if "alloc" in state or "mix" in state:
                aggregated[name]["allocated"] += nodes
            elif "idle" in state:
                aggregated[name]["idle"] += nodes
            elif "down" in state or "drain" in state:
                aggregated[name]["down"] += nodes
            else:
                aggregated[name]["other"] += nodes

        return list(aggregated.values())

    def _get_utilization_color(self, utilization: float) -> str:
        """Get color based on utilization percentage."""
        if utilization < 50:
            return "green"
        elif utilization < 80:
            return "yellow"
        else:
            return "red"

    def _build_partitions_table(self, partitions: List[Dict[str, Any]]) -> Table:
        """Build a Rich table for partition information."""
        table = Table(title="Cluster Partitions", expand=True)
        table.add_column("Partition", style="cyan", no_wrap=True)
        table.add_column("Avail", style="white", justify="center")
        table.add_column("Total", justify="right")
        table.add_column("Allocated", justify="right")
        table.add_column("Idle", justify="right")
        table.add_column("Down", justify="right")
        table.add_column("Utilization", min_width=20)
        table.add_column("Time Limit", style="dim")

        for partition in sorted(partitions, key=lambda x: x["name"]):
            name = partition["name"]
            avail = partition["avail"]
            total = partition["total_nodes"]
            allocated = partition["allocated"]
            idle = partition["idle"]
            down = partition["down"]
            timelimit = partition["timelimit"]

            if avail.lower() == "up":
                avail_styled = "[green]up[/green]"
            elif avail.lower() == "down":
                avail_styled = "[red]down[/red]"
            else:
                avail_styled = avail

            available_nodes = total - down
            if available_nodes > 0:
                utilization = (allocated / available_nodes) * 100
            else:
                utilization = 0.0

            util_color = self._get_utilization_color(utilization)
            bar_width = 15
            filled = int((utilization / 100) * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
            util_str = f"[{util_color}]{bar}[/{util_color}] {utilization:5.1f}%"

            table.add_row(
                name,
                avail_styled,
                str(total),
                f"[yellow]{allocated}[/yellow]" if allocated > 0 else str(allocated),
                f"[green]{idle}[/green]" if idle > 0 else str(idle),
                f"[red]{down}[/red]" if down > 0 else str(down),
                util_str,
                timelimit,
            )

        return table

    def _build_account_summary(self, queue: List[Dict[str, Any]]) -> Optional[Panel]:
        """Build a summary panel for the specified account's jobs."""
        if not self.account:
            return None

        user_jobs = [
            j
            for j in queue
            if str(j.get("USER", "") or j.get("UserId", "")) == self.account
        ]

        if not user_jobs:
            return Panel(
                f"[dim]No jobs found for account '{self.account}'[/dim]",
                title=f"Account: {self.account}",
            )

        running = sum(
            1
            for j in user_jobs
            if str(j.get("STATE", "") or j.get("JobState", "")).startswith("RUNNING")
        )
        pending = sum(
            1
            for j in user_jobs
            if str(j.get("STATE", "") or j.get("JobState", "")).startswith("PENDING")
        )
        other = len(user_jobs) - running - pending

        summary_lines = [
            f"[bold]Total Jobs:[/bold] {len(user_jobs)}",
            f"[bold]Running:[/bold] [green]{running}[/green]",
            f"[bold]Pending:[/bold] [yellow]{pending}[/yellow]",
        ]
        if other > 0:
            summary_lines.append(f"[bold]Other:[/bold] {other}")

        return Panel(
            "\n".join(summary_lines),
            title=f"Account: {self.account}",
            border_style="cyan",
        )

    def _build_dashboard(self) -> Group:
        """Build the complete dashboard layout."""
        elements = []

        try:
            info = self.cluster.get_cluster_info()
            partitions = info.get("partitions", [])
            aggregated = self._aggregate_partitions(partitions)
        except Exception as e:
            elements.append(
                Panel(f"[red]Error fetching cluster info: {e}[/red]", title="Error")
            )
            return Group(*elements)

        if aggregated:
            elements.append(self._build_partitions_table(aggregated))
        else:
            elements.append(
                Panel(
                    "[dim]No partition information available.[/dim]", title="Partitions"
                )
            )

        if self.account:
            try:
                queue = self.cluster.get_queue()
                if account_panel := self._build_account_summary(queue):
                    elements.append(account_panel)
            except Exception:
                pass

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
