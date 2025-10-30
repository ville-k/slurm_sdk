"""Hello-world style example that demonstrates container packaging.

This example shows:
- Using container packaging
- Specifying a container image directly in the decorator
- Using argparse helpers for cluster configuration
"""

from __future__ import annotations

import argparse
import getpass
import logging

from slurm.callbacks.callbacks import (
    RichLoggerCallback,
)
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.job import Job


@task(
    time="00:05:00",
    mem="10G",
    cpus_per_task=1,
)
def hello_container_task(greeted: str) -> str:
    """Report basic runtime information from inside the container."""
    import socket
    import time

    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello {greeted} from {hostname} at {current_time}!"
    return message


def main() -> None:
    """Entry point for the container packaging example."""

    parser = argparse.ArgumentParser(
        description="Submit a task using container packaging"
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    # Create cluster from args with default container packaging
    cluster = Cluster.from_args(
        args,
        callbacks=[RichLoggerCallback()],
        # Set default container packaging configuration
        default_packaging="container",
        default_packaging_dockerfile="src/slurm/examples/hello_container.Dockerfile",
    )
    with cluster:
        greeted = getpass.getuser()
        job: Job[str] = hello_container_task(greeted)

        success = job.wait()
        if success:
            result: str = job.get_result()
            print(f"Result: {result}")
        else:
            print("Job failed!")
            print("Job std out:")
            print(job.get_stdout())
            print("Job std err:")
            print(job.get_stderr())


if __name__ == "__main__":
    main()
