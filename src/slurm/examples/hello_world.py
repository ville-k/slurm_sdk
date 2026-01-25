"""Simple hello world example demonstrating basic slurm-sdk usage.

This example shows:
- Creating a cluster with argparse helpers
- Using container packaging with a Dockerfile
- Submitting a task and retrieving results
"""

import argparse
import logging

from slurm.callbacks.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.job import Job


@task(
    time="00:05:00",
    mem="1G",
    cpus_per_task=1,
)
def hello_world() -> str:
    """
    A simple hello world task.

    Returns:
        A greeting message
    """
    import socket
    import time

    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    return message


def main():
    """
    Main entry point for the example script.
    """
    parser = argparse.ArgumentParser(
        description="Submit a simple hello world job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    # Build cluster kwargs
    cluster_kwargs = {
        "callbacks": [LoggerCallback()],
    }

    # Only set default packaging/dockerfile if the user didn't specify packaging via --packaging
    # This allows tests to specify packaging="wheel" without building containers
    packaging_arg = getattr(args, "packaging", None)
    if packaging_arg is None:
        # No --packaging argument provided, use container as default
        cluster_kwargs["default_packaging"] = "container"
        cluster_kwargs["default_packaging_dockerfile"] = (
            "src/slurm/examples/hello_world.Dockerfile"
        )
    elif packaging_arg == "container":
        # Container packaging specified, but no Dockerfile set yet - set it
        if not getattr(args, "packaging_dockerfile", None):
            cluster_kwargs["default_packaging_dockerfile"] = (
                "src/slurm/examples/hello_world.Dockerfile"
            )
    # If --packaging was provided with another value (wheel, none), don't set defaults

    cluster = Cluster.from_args(args, **cluster_kwargs)

    # Submit job (uses cluster defaults for packaging, account, partition)
    job: Job[str] = cluster.submit(hello_world)()

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
