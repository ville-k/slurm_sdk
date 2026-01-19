"""Dashboard command for interactive TUI job monitoring."""

from __future__ import annotations

from typing import Annotated, Optional

import cyclopts
from rich.console import Console

console = Console(stderr=True)

dash_app = cyclopts.App(
    name="dash",
    help="Interactive TUI dashboard for monitoring jobs and cluster status.",
)


@dash_app.default
def dash(
    env: Annotated[
        Optional[str],
        cyclopts.Parameter(
            help="Environment name from Slurmfile.",
        ),
    ] = None,
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            help="Path to Slurmfile.",
        ),
    ] = None,
    refresh_interval: Annotated[
        int,
        cyclopts.Parameter(
            help="Auto-refresh interval in seconds.",
        ),
    ] = 30,
) -> None:
    """Launch the interactive dashboard TUI.

    The dashboard provides a navigable view of:
    - Your jobs (running, pending, completed)
    - Jobs from your account
    - Cluster partition status

    Navigate with arrow keys, press 'r' to refresh manually,
    'a' to toggle auto-refresh, and 'q' to quit.
    """
    try:
        from ..tui import check_textual_available

        check_textual_available()
    except ImportError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print(
            "\n[dim]Install TUI support with: pip install slurm-sdk[tui][/dim]"
        )
        raise SystemExit(1)

    from ..tui.dashboard.app import DashboardApp
    from .utils import get_cluster

    cluster = get_cluster(env=env, slurmfile=slurmfile)
    app = DashboardApp(cluster=cluster, refresh_interval=refresh_interval)
    app.run()
