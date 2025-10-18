"""Hello-world style example that demonstrates container packaging."""

from __future__ import annotations

import argparse
import pathlib
import socket
import time

from rich.console import Console

from slurm.callbacks.callbacks import (
    BenchmarkCallback,
    RichLoggerCallback,
)
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.container_example.toml")


@task(
    time="00:05:00",
    mem="10G",
    cpus_per_task=1,
)
def hello_container_task() -> str:
    """Report basic runtime information from inside the container."""

    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    print(message)
    return message


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the example script."""

    parser = argparse.ArgumentParser(
        description=(
            "Submit the hello_container task using a Slurmfile that configures "
            "the container packaging strategy."
        )
    )
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load (defaults to the bundled example).",
    )
    parser.add_argument(
        "--env",
        default="default",
        help=("Environment key within the Slurmfile to load (defaults to 'default')."),
    )
    return parser


def main() -> None:
    """Entry point for the container packaging example."""

    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    cluster = Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[RichLoggerCallback(console=console), BenchmarkCallback()],
    )

    env_submit = cluster.environment_config["submit"]

    job = hello_container_task.submit(
        cluster=cluster,
        packaging=cluster.packaging_defaults,
        account=env_submit.get("account"),
        partition=env_submit.get("partition"),
    )()

    job.wait()
    result = job.get_result()
    console.print(f"Result: {result}")


if __name__ == "__main__":
    main()
