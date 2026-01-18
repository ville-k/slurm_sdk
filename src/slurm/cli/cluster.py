"""Cluster subcommand for the slurm CLI."""

from __future__ import annotations

from typing import Annotated, Optional

import cyclopts

from .formatters import print_cluster_info, print_environments_table
from .utils import get_cluster, list_slurmfile_environments

cluster_app = cyclopts.App(
    name="cluster",
    help="Manage cluster configurations.",
)


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
