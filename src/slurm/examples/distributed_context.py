"""Example showcasing JobContext introspection for distributed workloads.

This example demonstrates:
- Using JobContext to access SLURM environment variables
- Multi-task allocations (ntasks=4)
- Inspecting distributed job information (rank, world_size, hostnames)
- PyTorch distributed environment variables
"""

from __future__ import annotations

import argparse
import os
from typing import Optional

from rich.console import Console

from slurm.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging
from slurm.runtime import JobContext


@task(time="00:05:00", ntasks=4)
def inspect_allocation(params: Optional[list[str]] = None, *, job: JobContext) -> dict:
    """Return a summary of the distributed context visible to the job."""

    hostnames = job.hostnames or (os.uname().nodename,)
    torch_env = job.torch_distributed_env()

    return {
        "job_id": job.job_id,
        "node_rank": job.node_rank,
        "rank": job.rank,
        "world_size": job.world_size,
        "nodes": hostnames,
        "master_addr": torch_env.get("MASTER_ADDR"),
        "torch_env": torch_env,
        "raw_env_keys": sorted(list(job.environment.keys()))[:5],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit a task that inspects the SBATCH allocation context.",
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    return parser


def main() -> None:
    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    console.print("[bold cyan]Submitting SBATCH context inspection job...[/bold cyan]")

    # Create cluster from args
    cluster = Cluster.from_args(
        args,
        callbacks=[LoggerCallback(console=console)],
    )

    # Submit job (uses cluster defaults)
    job = cluster.submit(inspect_allocation)()

    job.wait()
    result = job.get_result()

    console.print("[bold green]Allocation summary:[/bold green]")
    console.print(result)


if __name__ == "__main__":
    main()
