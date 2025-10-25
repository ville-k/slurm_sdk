"""Simple hello world example demonstrating basic slurm-sdk usage.

This example shows:
- Creating a cluster with explicit configuration (no config file needed)
- Using the new string-based packaging syntax with cluster defaults
- Using argparse helpers for common cluster configuration
"""

import logging
import argparse
import socket
import time

from slurm.callbacks.callbacks import BenchmarkCallback, LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


@task(
    time="00:05:00",
    mem="1G",
    cpus_per_task=1,
)
def hello_world():
    """
    A simple hello world task.

    Returns:
        A greeting message
    """
    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    print(message)

    return message


def main():
    """
    Main entry point for the example script.
    """
    parser = argparse.ArgumentParser(description="Submit a simple hello world job")

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    # Add example-specific arguments
    parser.add_argument(
        "--banner-timeout",
        type=int,
        default=30,
        help="Timeout for waiting for SSH banner (seconds)",
    )
    parser.add_argument("--loglevel", type=str, default="INFO", help="Logging level")

    args = parser.parse_args()

    configure_logging(level=getattr(logging, args.loglevel.upper(), logging.INFO))

    # Create cluster from args with additional callbacks
    cluster = Cluster.from_args(
        args,
        banner_timeout=args.banner_timeout,
        callbacks=[
            LoggerCallback(),
            BenchmarkCallback(),
        ],
    )

    # Submit job (uses cluster defaults for packaging, account, partition)
    job = hello_world.submit(cluster)()

    job.wait()
    result = job.get_result()
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
