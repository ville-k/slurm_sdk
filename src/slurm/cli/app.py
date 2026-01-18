"""Root application for the slurm CLI."""

from __future__ import annotations

import sys

import cyclopts
from rich.console import Console

from .cluster import cluster_app
from .dash import dash_app
from .docs import docs_app
from .jobs import jobs_app

console = Console(stderr=True)


def _get_version() -> str:
    """Get SDK version for --version flag."""
    try:
        from importlib.metadata import version

        return version("slurm-sdk")
    except Exception:
        return "unknown"


app = cyclopts.App(
    name="slurm",
    help="CLI for the slurm SDK - manage SLURM jobs and clusters.",
    version=_get_version(),
)

app.command(jobs_app)
app.command(cluster_app)
app.command(dash_app)
app.command(docs_app)


def _handle_error(e: Exception) -> None:
    """Handle exceptions with user-friendly messages."""
    from ..errors import (
        BackendCommandError,
        BackendError,
        BackendTimeout,
        SlurmfileEnvironmentNotFoundError,
        SlurmfileError,
        SlurmfileInvalidError,
        SlurmfileNotFoundError,
    )

    if isinstance(e, SlurmfileNotFoundError):
        console.print(f"[red]Error:[/red] {e}")
        console.print(
            "\n[dim]Hint: Create a Slurmfile in your project directory, "
            "or use --slurmfile to specify a path.[/dim]"
        )
    elif isinstance(e, SlurmfileEnvironmentNotFoundError):
        console.print(f"[red]Error:[/red] {e}")
        console.print(
            "\n[dim]Hint: Use 'slurm cluster list' to see available environments.[/dim]"
        )
    elif isinstance(e, SlurmfileInvalidError):
        console.print(f"[red]Error:[/red] {e}")
        console.print("\n[dim]Hint: Check your Slurmfile for TOML syntax errors.[/dim]")
    elif isinstance(e, SlurmfileError):
        console.print(f"[red]Slurmfile Error:[/red] {e}")
    elif isinstance(e, BackendTimeout):
        console.print(f"[red]Connection Timeout:[/red] {e}")
        console.print(
            "\n[dim]Hint: Check network connectivity and cluster availability.[/dim]"
        )
    elif isinstance(e, BackendCommandError):
        console.print(f"[red]Command Error:[/red] {e}")
        console.print(
            "\n[dim]Hint: Verify SSH credentials and cluster connectivity.[/dim]"
        )
    elif isinstance(e, BackendError):
        console.print(f"[red]Backend Error:[/red] {e}")
    else:
        console.print(f"[red]Error:[/red] {e}")

    sys.exit(1)


def main() -> None:
    """Entry point for the slurm CLI."""
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as e:
        _handle_error(e)
