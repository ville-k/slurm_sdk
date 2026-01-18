"""Rich output formatters for the slurm CLI."""

from __future__ import annotations

from typing import Any, Dict, List

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# Color mapping for job states
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


def print_jobs_table(queue: List[Dict[str, Any]]) -> None:
    """Display job queue as a Rich table with color-coded states.

    Args:
        queue: List of job dictionaries from cluster.get_queue().
    """
    if not queue:
        console.print("[dim]No jobs in queue.[/dim]")
        return

    table = Table(title="Jobs")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("State", style="white")
    table.add_column("User", style="dim")
    table.add_column("Time", style="dim")
    table.add_column("Partition", style="dim")
    table.add_column("Nodes", justify="right", style="dim")

    for job in queue:
        job_id = str(
            job.get("JOBID", "") or job.get("JobID", "") or job.get("job_id", "") or ""
        )
        name = str(job.get("NAME", "") or job.get("JobName", "") or "")
        state = str(job.get("STATE", "") or job.get("JobState", "") or "")
        user = str(job.get("USER", "") or job.get("UserId", "") or "")
        time_str = str(job.get("TIME", "") or job.get("RunTime", "") or "")
        partition = str(job.get("PARTITION", "") or job.get("Partition", "") or "")
        nodes = str(job.get("NODES", "") or job.get("NumNodes", "") or "")

        color = _get_state_color(state)
        styled_state = f"[{color}]{state}[/{color}]"

        table.add_row(job_id, name, styled_state, user, time_str, partition, nodes)

    console.print(table)


def print_job_details(job_id: str, status: Dict[str, Any]) -> None:
    """Display detailed job info in a panel.

    Args:
        job_id: The job ID.
        status: Job status dictionary from job.get_status().
    """
    state = status.get("JobState", "UNKNOWN")
    color = _get_state_color(state)

    details: List[str] = []
    details.append(f"[bold]State:[/bold] [{color}]{state}[/{color}]")

    if exit_code := status.get("ExitCode"):
        details.append(f"[bold]Exit Code:[/bold] {exit_code}")

    if work_dir := status.get("WorkDir") or status.get("WorkDirectory"):
        details.append(f"[bold]Work Dir:[/bold] {work_dir}")

    if partition := status.get("Partition"):
        details.append(f"[bold]Partition:[/bold] {partition}")

    if account := status.get("Account"):
        details.append(f"[bold]Account:[/bold] {account}")

    if user := status.get("UserId") or status.get("User"):
        details.append(f"[bold]User:[/bold] {user}")

    if submit_time := status.get("SubmitTime"):
        details.append(f"[bold]Submitted:[/bold] {submit_time}")

    if start_time := status.get("StartTime"):
        details.append(f"[bold]Started:[/bold] {start_time}")

    if end_time := status.get("EndTime"):
        details.append(f"[bold]Ended:[/bold] {end_time}")

    if run_time := status.get("RunTime"):
        details.append(f"[bold]Run Time:[/bold] {run_time}")

    if time_limit := status.get("TimeLimit") or status.get("Timelimit"):
        details.append(f"[bold]Time Limit:[/bold] {time_limit}")

    if reason := status.get("Reason"):
        details.append(f"[bold]Reason:[/bold] {reason}")

    if num_nodes := status.get("NumNodes"):
        details.append(f"[bold]Nodes:[/bold] {num_nodes}")

    if num_cpus := status.get("NumCPUs"):
        details.append(f"[bold]CPUs:[/bold] {num_cpus}")

    panel_content = "\n".join(details)
    panel = Panel(panel_content, title=f"Job {job_id}", border_style=color)
    console.print(panel)


def print_environments_table(environments: List[Dict[str, Any]]) -> None:
    """List configured environments from Slurmfile.

    Args:
        environments: List of environment info dicts.
    """
    if not environments:
        console.print("[dim]No environments configured in Slurmfile.[/dim]")
        return

    table = Table(title="Environments")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Hostname", style="white")
    table.add_column("Slurmfile", style="dim")

    for env in environments:
        name = env.get("name", "")
        hostname = env.get("hostname", "") or "[dim](local)[/dim]"
        slurmfile = env.get("slurmfile", "")

        table.add_row(name, hostname, slurmfile)

    console.print(table)


def _aggregate_partitions(partitions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate partition rows by partition name.

    SLURM's sinfo returns one row per partition per node state. This function
    aggregates them to show one row per partition with total nodes and a
    summary of node states.
    """
    aggregated: Dict[str, Dict[str, Any]] = {}

    for partition in partitions:
        name = str(
            partition.get("PARTITION", "") or partition.get("partition", "") or ""
        )
        if not name:
            continue

        nodes_str = str(partition.get("NODES", "") or partition.get("nodes", "") or "0")
        try:
            nodes = int(nodes_str)
        except ValueError:
            nodes = 0

        state = str(partition.get("STATE", "") or partition.get("state", "") or "")

        if name not in aggregated:
            aggregated[name] = {
                "name": name,
                "avail": str(
                    partition.get("AVAIL", "") or partition.get("avail", "") or ""
                ),
                "total_nodes": 0,
                "states": {},
                "timelimit": str(
                    partition.get("TIMELIMIT", "")
                    or partition.get("timelimit", "")
                    or ""
                ),
                "cpus": str(
                    partition.get("CPUS", "") or partition.get("cpus", "") or ""
                ),
                "memory": str(
                    partition.get("MEMORY", "") or partition.get("memory", "") or ""
                ),
            }

        aggregated[name]["total_nodes"] += nodes
        if state and nodes > 0:
            aggregated[name]["states"][state] = (
                aggregated[name]["states"].get(state, 0) + nodes
            )

    result = []
    for name in sorted(aggregated.keys()):
        data = aggregated[name]
        states = data["states"]
        if states:
            state_summary = ", ".join(
                f"{count} {state}" for state, count in sorted(states.items())
            )
        else:
            state_summary = "n/a"

        result.append(
            {
                "name": data["name"],
                "avail": data["avail"],
                "nodes": str(data["total_nodes"]),
                "state": state_summary,
                "timelimit": data["timelimit"],
                "cpus": data["cpus"],
                "memory": data["memory"],
            }
        )

    return result


def print_cluster_info(env_name: str, info: Dict[str, Any]) -> None:
    """Display partition information from cluster.

    Args:
        env_name: Name of the environment.
        info: Cluster info dictionary from cluster.get_cluster_info().
    """
    partitions = info.get("partitions", [])

    if not partitions:
        console.print(
            f"[dim]No partition information available for '{env_name}'.[/dim]"
        )
        return

    aggregated = _aggregate_partitions(partitions)

    table = Table(title=f"Cluster Partitions ({env_name})")
    table.add_column("Partition", style="cyan", no_wrap=True)
    table.add_column("Avail", style="white")
    table.add_column("Nodes", justify="right", style="white")
    table.add_column("Node States", style="dim")
    table.add_column("Timelimit", style="dim")

    for partition in aggregated:
        name = partition["name"]
        avail = partition["avail"]
        nodes = partition["nodes"]
        state = partition["state"]
        timelimit = partition["timelimit"]

        if avail.lower() == "up":
            avail_styled = "[green]up[/green]"
        elif avail.lower() == "down":
            avail_styled = "[red]down[/red]"
        else:
            avail_styled = avail

        table.add_row(name, avail_styled, nodes, state, timelimit)

    console.print(table)
