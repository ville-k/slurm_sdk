"""Cluster subcommand for the slurm CLI."""

from __future__ import annotations

import os
import sys
from typing import Annotated, Optional

import cyclopts
from rich.console import Console

from .formatters import print_cluster_info, print_environments_table
from .utils import get_cluster, list_slurmfile_environments

cluster_app = cyclopts.App(
    name="cluster",
    help="Manage cluster configurations.",
)

console = Console(stderr=True)


@cluster_app.command(name="list")
def list_clusters(
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--slurmfile", "-f"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """List configured environments from Slurmfile (offline, no connection)."""
    environments = list_slurmfile_environments(slurmfile=slurmfile)
    print_environments_table(environments)


@cluster_app.command(name="show")
def show_cluster(
    env: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--env", "-e"],
            help="Environment name from Slurmfile.",
        ),
    ] = None,
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--slurmfile", "-f"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """Show cluster partition information (requires connection)."""
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    info = cluster.get_cluster_info()
    env_name = env or "default"
    print_cluster_info(env_name, info)


@cluster_app.command(name="watch")
def watch_cluster(
    account: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--account", "-a"],
            help="Highlight resource usage for this account/user.",
        ),
    ] = None,
    poll_interval: Annotated[
        float,
        cyclopts.Parameter(
            name=["--poll-interval", "-i"],
            help="Seconds between info polls.",
        ),
    ] = 5.0,
    env: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--env", "-e"],
            help="Environment name from Slurmfile.",
        ),
    ] = None,
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--slurmfile", "-f"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """Watch cluster partitions in real-time with a live dashboard.

    Shows a continuously updating view of partition utilization including
    allocated, idle, and down nodes with utilization percentages.
    """
    from .live import ClusterDashboard

    cluster = get_cluster(env=env, slurmfile=slurmfile)
    dashboard = ClusterDashboard(
        cluster=cluster,
        account=account,
        poll_interval=poll_interval,
    )
    dashboard.run()


@cluster_app.command(name="connect")
def connect_cluster(
    env_name: Annotated[
        Optional[str],
        cyclopts.Parameter(
            help="Environment name to connect to (from Slurmfile).",
        ),
    ] = None,
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--slurmfile", "-f"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """Connect to a cluster's login node via SSH.

    Opens an interactive SSH session to the cluster's login node as configured
    in the Slurmfile. If no environment is specified, uses the default.
    """
    environments = list_slurmfile_environments(slurmfile=slurmfile)

    if not environments:
        console.print("[red]Error:[/red] No environments configured in Slurmfile.")
        sys.exit(1)

    target_env = None
    if env_name:
        for e in environments:
            if e["name"] == env_name:
                target_env = e
                break
        if not target_env:
            console.print(f"[red]Error:[/red] Environment '{env_name}' not found.")
            console.print("[dim]Available environments:[/dim]")
            for e in environments:
                console.print(f"  - {e['name']}")
            sys.exit(1)
    else:
        target_env = environments[0]

    hostname = target_env.get("hostname")
    if not hostname:
        console.print(
            f"[red]Error:[/red] No hostname configured for environment "
            f"'{target_env['name']}'."
        )
        console.print(
            "[dim]The 'connect' command requires an SSH-configured environment.[/dim]"
        )

        ssh_envs = [e for e in environments if e.get("hostname")]
        if ssh_envs:
            console.print("\n[dim]Available SSH environments:[/dim]")
            for e in ssh_envs:
                console.print(f"  - {e['name']} ({e['hostname']})")
            console.print(
                f"\n[dim]Try: slurm cluster connect {ssh_envs[0]['name']}[/dim]"
            )
        else:
            console.print(
                "\n[dim]Hint: Add a hostname to your Slurmfile to enable SSH connections.[/dim]"
            )
        sys.exit(1)

    cluster = get_cluster(env=target_env["name"], slurmfile=slurmfile)
    backend = cluster.backend
    backend_type = type(backend).__name__

    if backend_type != "SSHCommandBackend":
        console.print(
            "[red]Error:[/red] Cannot connect to local backend. "
            "SSH connection requires a remote cluster."
        )
        sys.exit(1)

    console.print(f"[cyan]Connecting to {hostname}...[/cyan]")

    _open_ssh_session(backend)


def _open_ssh_session(backend: "SSHCommandBackend") -> None:  # noqa: F821
    """Open an interactive SSH session."""
    import select
    import termios
    import tty

    try:
        client = backend.client
        channel = client.get_transport().open_session()
        channel.get_pty(
            term=os.environ.get("TERM", "xterm-256color"),
            width=80,
            height=24,
        )
        channel.invoke_shell()

        oldtty = termios.tcgetattr(sys.stdin)
        try:
            tty.setraw(sys.stdin.fileno())
            tty.setcbreak(sys.stdin.fileno())
            channel.setblocking(0)

            while True:
                r, w, e = select.select([channel, sys.stdin], [], [])

                if channel in r:
                    try:
                        data = channel.recv(1024)
                        if len(data) == 0:
                            break
                        sys.stdout.write(data.decode("utf-8", errors="replace"))
                        sys.stdout.flush()
                    except Exception:
                        break

                if sys.stdin in r:
                    data = os.read(sys.stdin.fileno(), 1024)
                    if len(data) == 0:
                        break
                    channel.send(data)

        finally:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)

        console.print("\n[dim]Connection closed.[/dim]")

    except Exception as e:
        console.print(f"\n[red]Connection error: {e}[/red]")
        sys.exit(1)
