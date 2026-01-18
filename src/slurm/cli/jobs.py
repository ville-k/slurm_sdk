"""Jobs subcommand for the slurm CLI."""

from __future__ import annotations

import os
import sys
from typing import Annotated, Optional

import cyclopts
from rich.console import Console

from .formatters import print_job_details, print_jobs_table
from .utils import get_cluster

jobs_app = cyclopts.App(
    name="jobs",
    help="Manage SLURM jobs.",
)

console = Console(stderr=True)


@jobs_app.command(name="list")
def list_jobs(
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
    """List jobs in the SLURM queue."""
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    queue = cluster.get_queue()
    print_jobs_table(queue)


@jobs_app.command(name="show")
def show_job(
    job_id: Annotated[
        str,
        cyclopts.Parameter(
            help="SLURM job ID to show details for.",
        ),
    ],
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
    """Show details for a specific job."""
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    job = cluster.get_job(job_id)
    status = job.get_status()
    print_job_details(job_id, status)


@jobs_app.command(name="watch")
def watch_jobs(
    job_filter: Annotated[
        Optional[str],
        cyclopts.Parameter(
            help="Job ID or job name to filter on.",
        ),
    ] = None,
    account: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--account", "-a"],
            help="Filter jobs by account/user. Defaults to $USER.",
        ),
    ] = None,
    poll_interval: Annotated[
        float,
        cyclopts.Parameter(
            name=["--poll-interval", "-i"],
            help="Seconds between queue polls.",
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
    """Watch jobs in real-time with a live dashboard.

    Shows a continuously updating view of jobs in the SLURM queue.
    If a job ID or name is provided, shows detailed info for matching jobs.
    """
    from .live import JobsDashboard

    if account is None:
        account = os.environ.get("USER")

    cluster = get_cluster(env=env, slurmfile=slurmfile)
    dashboard = JobsDashboard(
        cluster=cluster,
        job_filter=job_filter,
        account=account,
        poll_interval=poll_interval,
    )
    dashboard.run()


@jobs_app.command(name="connect")
def connect_job(
    job_id: Annotated[
        str,
        cyclopts.Parameter(
            help="SLURM job ID to connect to.",
        ),
    ],
    node: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--node", "-n"],
            help="Target specific node in multi-node jobs.",
        ),
    ] = None,
    no_container: Annotated[
        bool,
        cyclopts.Parameter(
            name=["--no-container"],
            help="Connect to bare node instead of container (for container jobs).",
        ),
    ] = False,
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
    """Connect to a running job with an interactive shell.

    For container jobs, automatically attaches to the container. Use --no-container
    to connect to the bare compute node instead.

    For multi-node jobs, prompts to select which node to connect to unless
    --node is specified.
    """
    import logging

    # Suppress the metadata warning for connect - we don't need full job metadata
    logging.getLogger("slurm.job").setLevel(logging.ERROR)

    cluster = get_cluster(env=env, slurmfile=slurmfile)

    console.print(f"[dim]Checking job {job_id} status...[/dim]")
    job = cluster.get_job(job_id)
    status = job.get_status()

    job_state = str(status.get("JobState", ""))
    if not job_state.startswith("RUNNING"):
        console.print(
            f"[red]Error:[/red] Job {job_id} is not running (state: {job_state})"
        )
        sys.exit(1)

    # Check if this is a container job
    is_container_job = _is_container_job(cluster)
    pre_submission_id = job.pre_submission_id

    # Handle multi-node jobs
    target_node = _resolve_target_node(status, node)

    # Build the srun command
    srun_cmd = ["srun", f"--jobid={job_id}", "--overlap", "--pty"]

    if target_node:
        srun_cmd.extend(["-w", target_node])

    # Add container flags if applicable
    if is_container_job and not no_container:
        if pre_submission_id:
            container_name = f"slurm-sdk-{pre_submission_id}"
            srun_cmd.append(f"--container-name={container_name}")
            console.print(f"[cyan]Connecting to container in job {job_id}...[/cyan]")
            console.print(f"[dim]Container: {container_name}[/dim]")
        else:
            console.print(
                "[yellow]Warning:[/yellow] Container job detected but pre_submission_id "
                "not available. Connecting to node instead."
            )
            console.print(
                "[dim]Hint: Jobs must be submitted with SDK v0.5+ for container attach.[/dim]"
            )
            is_container_job = False
    else:
        console.print(f"[cyan]Connecting to job {job_id}...[/cyan]")

    srun_cmd.append("bash")
    srun_command_str = " ".join(srun_cmd)

    console.print(f"[dim]Command: {srun_command_str}[/dim]")

    backend = cluster.backend
    backend_type = type(backend).__name__

    if backend_type == "SSHCommandBackend":
        _connect_via_ssh(backend, srun_command_str)
    else:
        _connect_local(srun_command_str)


def _is_container_job(cluster) -> bool:
    """Check if the cluster environment uses container packaging."""
    packaging_defaults = getattr(cluster, "packaging_defaults", None)
    if packaging_defaults and isinstance(packaging_defaults, dict):
        return packaging_defaults.get("type") == "container"
    return False


def _resolve_target_node(status: dict, specified_node: Optional[str]) -> Optional[str]:
    """Resolve which node to connect to, prompting for multi-node jobs."""
    if specified_node:
        return specified_node

    node_list = status.get("NodeList", "")
    if not node_list:
        return None

    # Parse node list - could be "node1" or "node[1-3]" or "node1,node2,node3"
    nodes = _parse_node_list(node_list)

    if len(nodes) <= 1:
        return nodes[0] if nodes else None

    # Multi-node job - prompt user to select
    console.print(f"\n[yellow]Multi-node job detected ({len(nodes)} nodes)[/yellow]")
    console.print("Select a node to connect to:\n")

    for i, node_name in enumerate(nodes, 1):
        console.print(f"  [{i}] {node_name}")

    console.print()

    while True:
        try:
            choice = input(f"Enter choice [1-{len(nodes)}]: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(nodes):
                selected = nodes[idx]
                console.print(f"[dim]Selected: {selected}[/dim]")
                return selected
            console.print(
                f"[red]Please enter a number between 1 and {len(nodes)}[/red]"
            )
        except ValueError:
            console.print("[red]Please enter a valid number[/red]")
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Cancelled[/yellow]")
            sys.exit(0)


def _parse_node_list(node_list: str) -> list:
    """Parse SLURM node list notation into individual node names.

    Handles formats like:
    - "node1"
    - "node1,node2,node3"
    - "node[1-3]"
    - "node[1,3,5]"
    - "gpu-[001-004]"
    """
    import re

    nodes = []

    # Split by comma first (for lists like "node1,node2")
    parts = node_list.split(",")

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Check for range notation like "node[1-3]" or "node[001-004]"
        range_match = re.match(r"^(.+)\[(\d+)-(\d+)\]$", part)
        if range_match:
            prefix = range_match.group(1)
            start = int(range_match.group(2))
            end = int(range_match.group(3))
            # Preserve leading zeros
            width = len(range_match.group(2))
            for i in range(start, end + 1):
                nodes.append(f"{prefix}{str(i).zfill(width)}")
            continue

        # Check for list notation like "node[1,3,5]"
        list_match = re.match(r"^(.+)\[([^\]]+)\]$", part)
        if list_match:
            prefix = list_match.group(1)
            indices = list_match.group(2).split(",")
            for idx in indices:
                idx = idx.strip()
                if "-" in idx:
                    # Handle range within list like "node[1-3,5,7-9]"
                    range_parts = idx.split("-")
                    start = int(range_parts[0])
                    end = int(range_parts[1])
                    width = len(range_parts[0])
                    for i in range(start, end + 1):
                        nodes.append(f"{prefix}{str(i).zfill(width)}")
                else:
                    nodes.append(f"{prefix}{idx}")
            continue

        # Plain node name
        nodes.append(part)

    return nodes


def _get_terminal_size() -> tuple:
    """Get terminal size, with fallback defaults."""
    try:
        import shutil

        size = shutil.get_terminal_size()
        return (size.columns, size.lines)
    except Exception:
        return (80, 24)


def _connect_via_ssh(backend: "SSHCommandBackend", command: str) -> None:  # noqa: F821
    """Connect to job via SSH backend with PTY."""
    import select
    import termios
    import tty

    console.print("[dim]Opening SSH channel to login node...[/dim]")

    try:
        client = backend.client
        transport = client.get_transport()

        if not transport or not transport.is_active():
            console.print("[red]Error:[/red] SSH connection is not active.")
            sys.exit(1)

        channel = transport.open_session()

        # Get actual terminal size
        width, height = _get_terminal_size()
        console.print(f"[dim]Requesting PTY ({width}x{height})...[/dim]")

        channel.get_pty(
            term=os.environ.get("TERM", "xterm-256color"),
            width=width,
            height=height,
        )

        console.print(f"[dim]Executing: {command}[/dim]")
        console.print(
            "[yellow]Waiting for srun to connect to job allocation...[/yellow]"
        )
        console.print("[dim](This may take a moment if the job is busy)[/dim]")

        channel.exec_command(command)

        # Brief wait to check if command fails immediately
        import time

        time.sleep(0.2)

        if channel.exit_status_ready():
            exit_status = channel.recv_exit_status()
            stderr_data = ""
            if channel.recv_stderr_ready():
                stderr_data = channel.recv_stderr(4096).decode(
                    "utf-8", errors="replace"
                )
            console.print(f"[red]Error:[/red] Command failed (exit {exit_status})")
            if stderr_data:
                console.print(f"[red]{stderr_data.strip()}[/red]")
            sys.exit(1)

        # Clear the status messages and enter raw mode
        console.print(
            "[green]Connected! Press Ctrl+D or type 'exit' to disconnect.[/green]"
        )

        oldtty = termios.tcgetattr(sys.stdin)
        try:
            tty.setraw(sys.stdin.fileno())
            tty.setcbreak(sys.stdin.fileno())
            channel.setblocking(0)

            while True:
                r, w, e = select.select([channel, sys.stdin], [], [], 0.5)

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

                # Check if channel closed
                if channel.closed:
                    break

        finally:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)

        exit_status = channel.recv_exit_status()
        if exit_status != 0:
            console.print(
                f"\n[yellow]Session ended with exit code {exit_status}[/yellow]"
            )

    except Exception as e:
        console.print(f"\n[red]Connection error: {e}[/red]")
        sys.exit(1)


def _connect_local(command: str) -> None:
    """Connect to job locally using subprocess."""
    import subprocess

    try:
        # Command is constructed internally from validated job_id and flags, not user input
        subprocess.run(command, shell=True, check=False)  # nosec B602
    except KeyboardInterrupt:
        pass


@jobs_app.command(name="cancel")
def cancel_job(
    job_id: Annotated[
        str,
        cyclopts.Parameter(
            help="SLURM job ID to cancel.",
        ),
    ],
    force: Annotated[
        bool,
        cyclopts.Parameter(
            name=["--force", "-f"],
            help="Skip confirmation prompt.",
        ),
    ] = False,
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
            name=["--slurmfile", "-s"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """Cancel a running or pending job.

    Sends a cancellation signal to the job via `scancel`. The job will be
    terminated if running, or removed from the queue if pending.
    """
    import logging

    # Suppress the metadata warning - we don't need full job metadata
    logging.getLogger("slurm.job").setLevel(logging.ERROR)

    cluster = get_cluster(env=env, slurmfile=slurmfile)

    # Get job status first to show what we're cancelling
    console.print(f"[dim]Checking job {job_id} status...[/dim]")
    job = cluster.get_job(job_id)
    status = job.get_status()

    job_state = str(status.get("JobState", "UNKNOWN"))
    job_name = status.get("JobName", "unknown")

    # Check if job is already completed/cancelled
    terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"}
    if job_state in terminal_states:
        console.print(
            f"[yellow]Job {job_id} is already in terminal state: {job_state}[/yellow]"
        )
        return

    console.print(f"[cyan]Job {job_id}:[/cyan] {job_name} ({job_state})")

    # Confirm unless --force is used
    if not force:
        try:
            confirm = input(f"Cancel job {job_id}? [y/N]: ").strip().lower()
            if confirm not in ("y", "yes"):
                console.print("[yellow]Cancelled.[/yellow]")
                return
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Cancelled.[/yellow]")
            return

    # Cancel the job
    console.print(f"[dim]Cancelling job {job_id}...[/dim]")
    success = job.cancel()

    if success:
        console.print(f"[green]Job {job_id} cancelled successfully.[/green]")
    else:
        console.print(f"[red]Failed to cancel job {job_id}.[/red]")
        sys.exit(1)


@jobs_app.command(name="debug")
def debug_job(
    job_id: Annotated[
        str,
        cyclopts.Parameter(
            help="SLURM job ID to debug.",
        ),
    ],
    port: Annotated[
        int,
        cyclopts.Parameter(
            name=["--port", "-p"],
            help="Local port for debugger forwarding.",
        ),
    ] = 5678,
    wait: Annotated[
        bool,
        cyclopts.Parameter(
            name=["--wait", "-w"],
            help="Signal job to wait for debugger attachment.",
        ),
    ] = False,
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
    """Attach a debugger to a running job.

    Sets up SSH port forwarding from local port to the job's debugpy port
    on the compute node. The job must be running with debugpy enabled
    (via DebugCallback or DEBUGPY_PORT environment variable).

    After connecting, use VS Code's "Remote Attach" configuration to connect
    to localhost:<port>.
    """
    import logging

    # Suppress the metadata warning for debug - we don't need full job metadata
    logging.getLogger("slurm.job").setLevel(logging.ERROR)

    cluster = get_cluster(env=env, slurmfile=slurmfile)

    console.print(f"[dim]Checking job {job_id} status...[/dim]")
    job = cluster.get_job(job_id)
    status = job.get_status()

    job_state = str(status.get("JobState", ""))
    if not job_state.startswith("RUNNING"):
        console.print(
            f"[red]Error:[/red] Job {job_id} is not running (state: {job_state})"
        )
        sys.exit(1)

    node_list = status.get("NodeList", "")
    if not node_list:
        console.print("[red]Error:[/red] Could not determine job's compute node.")
        sys.exit(1)

    first_node = node_list.split(",")[0].split("[")[0]

    remote_port = 5678

    console.print(f"[cyan]Setting up debug session for job {job_id}[/cyan]")
    console.print(f"[dim]Compute node: {first_node}[/dim]")
    console.print(f"[dim]Remote debugpy port: {remote_port}[/dim]")
    console.print(f"[dim]Local port: {port}[/dim]")

    backend = cluster.backend
    backend_type = type(backend).__name__

    if backend_type != "SSHCommandBackend":
        console.print("\n[yellow]Local backend detected.[/yellow]")
        console.print(
            f"Connect your debugger directly to [cyan]{first_node}:{remote_port}[/cyan]"
        )
        _print_vscode_config(first_node, remote_port)
        return

    console.print("\n[bold]Starting SSH port forwarding...[/bold]")
    console.print(
        f"Forwarding [cyan]localhost:{port}[/cyan] → "
        f"[cyan]{first_node}:{remote_port}[/cyan]"
    )

    _print_vscode_config("localhost", port)

    console.print("\n[dim]Press Ctrl+C to stop port forwarding[/dim]\n")

    _start_port_forwarding(backend, first_node, remote_port, port)


def _print_vscode_config(host: str, port: int) -> None:
    """Print VS Code launch.json configuration."""
    config = f'''
[bold]VS Code launch.json configuration:[/bold]
{{
    "name": "Attach to SLURM Job",
    "type": "debugpy",
    "request": "attach",
    "connect": {{
        "host": "{host}",
        "port": {port}
    }},
    "pathMappings": [
        {{
            "localRoot": "${{workspaceFolder}}",
            "remoteRoot": "/path/in/container"
        }}
    ]
}}'''
    console.print(config)


def _start_port_forwarding(
    backend: "SSHCommandBackend",  # noqa: F821
    remote_host: str,
    remote_port: int,
    local_port: int,
) -> None:
    """Start SSH port forwarding."""
    import socket
    import threading

    client = backend.client
    transport = client.get_transport()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind(("127.0.0.1", local_port))
        server_socket.listen(5)
        console.print(
            f"[green]Port forwarding active on localhost:{local_port}[/green]"
        )

        def handle_connection(local_conn: socket.socket) -> None:
            try:
                channel = transport.open_channel(
                    "direct-tcpip",
                    (remote_host, remote_port),
                    local_conn.getpeername(),
                )
                if channel is None:
                    console.print("[red]Failed to open SSH channel[/red]")
                    local_conn.close()
                    return

                while True:
                    import select as sel

                    r, _, _ = sel.select([local_conn, channel], [], [], 1.0)

                    if local_conn in r:
                        data = local_conn.recv(4096)
                        if not data:
                            break
                        channel.send(data)

                    if channel in r:
                        data = channel.recv(4096)
                        if not data:
                            break
                        local_conn.send(data)

            except Exception as e:
                console.print(f"[dim]Connection closed: {e}[/dim]")
            finally:
                local_conn.close()

        try:
            while True:
                local_conn, addr = server_socket.accept()
                console.print(f"[dim]New connection from {addr}[/dim]")
                thread = threading.Thread(
                    target=handle_connection, args=(local_conn,), daemon=True
                )
                thread.start()

        except KeyboardInterrupt:
            console.print("\n[yellow]Port forwarding stopped.[/yellow]")

    except OSError as e:
        console.print(f"[red]Error binding to port {local_port}: {e}[/red]")
        sys.exit(1)
    finally:
        server_socket.close()
