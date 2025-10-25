"""Hello-world style example that demonstrates container packaging.

This example shows:
- Using container packaging with string-based syntax
- Specifying a container image directly in the decorator
- Using argparse helpers for cluster configuration
"""

from __future__ import annotations

import argparse
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


@task(
    time="00:05:00",
    mem="10G",
    cpus_per_task=1,
    packaging="container:ubuntu:22.04",
)
def hello_container_task() -> str:
    """Report basic runtime information from inside the container."""

    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    print(message)
    return message


def main() -> None:
    """Entry point for the container packaging example."""

    console = Console()
    parser = argparse.ArgumentParser(
        description="Submit a task using container packaging"
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    args = parser.parse_args()

    configure_logging()

    # Create cluster from args
    cluster = Cluster.from_args(
        args,
        callbacks=[RichLoggerCallback(console=console), BenchmarkCallback()],
    )

    # Submit job (uses container packaging from task decorator)
    job = hello_container_task.submit(cluster)()

    job.wait()
    result = job.get_result()
    console.print(f"Result: {result}")


if __name__ == "__main__":
    main()
