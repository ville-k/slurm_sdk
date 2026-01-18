"""MCP subcommand for the slurm CLI."""

from __future__ import annotations

from typing import Annotated, Optional

import cyclopts
from rich.console import Console
from rich.panel import Panel

mcp_app = cyclopts.App(
    name="mcp",
    help="Manage the MCP (Model Context Protocol) server.",
)

console = Console()


@mcp_app.command(name="run")
def run_mcp(
    transport: Annotated[
        str,
        cyclopts.Parameter(
            name=["--transport", "-t"],
            help="Transport type: 'stdio' (default) or 'http'.",
        ),
    ] = "stdio",
    host: Annotated[
        str,
        cyclopts.Parameter(
            name=["--host", "-H"],
            help="Host to bind to (for HTTP transport).",
        ),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        cyclopts.Parameter(
            name=["--port", "-p"],
            help="Port to bind to (for HTTP transport).",
        ),
    ] = 8000,
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
    """Run the MCP server exposing SLURM SDK APIs.

    The MCP server provides tools and resources for AI assistants to interact
    with SLURM clusters. It uses the same Slurmfile configuration as the SDK.

    Transport modes:
    - stdio (default): For Claude Desktop integration. Communicates via stdin/stdout.
    - http: For network access. Exposes an HTTP endpoint at http://host:port/mcp

    Available MCP Tools:
    - list_jobs: List jobs in the SLURM queue
    - get_job: Get detailed status for a specific job
    - cancel_job: Cancel a running or pending job
    - list_partitions: List cluster partitions
    - list_environments: List configured Slurmfile environments

    Available MCP Resources:
    - slurm://queue: Current job queue
    - slurm://jobs/{job_id}: Job details
    - slurm://partitions: Partition info
    - slurm://config: Slurmfile configuration
    """
    from ..mcp_server import run_server

    if transport == "http":
        console.print(f"[cyan]Starting MCP server on http://{host}:{port}/mcp[/cyan]")
        console.print("[dim]Press Ctrl+C to stop[/dim]")
    else:
        pass

    run_server(
        transport=transport,
        host=host,
        port=port,
        slurmfile=slurmfile,
        env=env,
    )


@mcp_app.command(name="status")
def status_mcp(
    slurmfile: Annotated[
        Optional[str],
        cyclopts.Parameter(
            name=["--slurmfile", "-f"],
            help="Path to Slurmfile.",
        ),
    ] = None,
) -> None:
    """Show MCP server configuration and status.

    Displays information about the MCP server configuration including
    available tools, resources, and Slurmfile settings.
    """
    from ..cli.utils import list_slurmfile_environments
    from ..config import resolve_slurmfile_path

    try:
        slurmfile_path = resolve_slurmfile_path(slurmfile)
        environments = list_slurmfile_environments(slurmfile=slurmfile)
    except Exception as e:
        console.print(f"[red]Error loading configuration: {e}[/red]")
        return

    config_lines = [
        f"[bold]Slurmfile:[/bold] {slurmfile_path}",
        "",
        "[bold]Environments:[/bold]",
    ]

    for env in environments:
        hostname = env.get("hostname") or "[dim](local)[/dim]"
        config_lines.append(f"  • {env['name']}: {hostname}")

    config_lines.extend(
        [
            "",
            "[bold]Available MCP Tools:[/bold]",
            "  • list_jobs - List jobs in the SLURM queue",
            "  • get_job - Get detailed status for a specific job",
            "  • cancel_job - Cancel a running or pending job",
            "  • list_partitions - List cluster partitions",
            "  • list_environments - List Slurmfile environments",
            "",
            "[bold]Available MCP Resources:[/bold]",
            "  • slurm://queue - Current job queue",
            "  • slurm://jobs/{job_id} - Job details",
            "  • slurm://partitions - Partition info",
            "  • slurm://config - Slurmfile configuration",
            "",
            "[bold]Usage:[/bold]",
            "  slurm mcp run                    # Start with stdio transport",
            "  slurm mcp run --transport http   # Start HTTP server",
        ]
    )

    panel = Panel(
        "\n".join(config_lines),
        title="MCP Server Configuration",
        border_style="cyan",
    )
    console.print(panel)
