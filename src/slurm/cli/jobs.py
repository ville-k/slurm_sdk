"""Jobs subcommand for the slurm CLI."""

from __future__ import annotations

from typing import Annotated, Optional

import cyclopts

from .formatters import print_job_details, print_jobs_table
from .utils import get_cluster

jobs_app = cyclopts.App(
    name="jobs",
    help="Manage SLURM jobs.",
)


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
