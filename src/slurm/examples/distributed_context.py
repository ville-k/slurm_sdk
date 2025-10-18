"""Example showcasing JobContext introspection for distributed workloads."""

from __future__ import annotations

import argparse
import os
import pathlib
from typing import Optional

from rich.console import Console

from slurm.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging
from slurm.runtime import JobContext

DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.container_example.toml")


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
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load (defaults to the container example).",
    )
    parser.add_argument(
        "--env",
        default="default",
        help="Environment key within the Slurmfile to load (defaults to 'default').",
    )
    return parser


def main() -> None:
    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    console.print("[bold cyan]Submitting SBATCH context inspection job...[/bold cyan]")

    callback = LoggerCallback(console=console)

    cluster = Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[callback],
    )

    submit_config = cluster.environment_config.get("submit", {})

    job = inspect_allocation.submit(
        cluster=cluster,
        packaging=cluster.packaging_defaults,
        account=submit_config.get("account"),
        partition=submit_config.get("partition"),
    )()

    job.wait()
    result = job.get_result()

    console.print("[bold green]Allocation summary:[/bold green]")
    console.print(result)


if __name__ == "__main__":
    main()
