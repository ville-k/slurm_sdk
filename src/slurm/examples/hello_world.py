import logging
import argparse
import os
import socket
import time

from slurm.callbacks.callbacks import BenchmarkCallback, LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


@task(
    name="hello_world",
    partition="cpu",
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
    parser = argparse.ArgumentParser(description="Submit jobs to a SLURM cluster")
    parser.add_argument("--hostname", required=True, help="Hostname for SSH backend")
    parser.add_argument(
        "--username",
        default=os.environ.get("USER"),
        help="Username for SSH backend. Defaults to current user's username.",
    )
    parser.add_argument("--account", help="SLURM account")
    parser.add_argument("--partition", help="SLURM partition")
    parser.add_argument(
        "--banner-timeout",
        type=int,
        default=30,
        help="Timeout for waiting for SSH banner (seconds)",
    )
    parser.add_argument("--loglevel", type=str, default="INFO", help="Logging level")
    args = parser.parse_args()

    configure_logging(level=getattr(logging, args.loglevel.upper(), logging.INFO))
    backend_kwargs = {}
    backend_kwargs["hostname"] = args.hostname
    backend_kwargs["username"] = args.username
    backend_kwargs["banner_timeout"] = args.banner_timeout

    cluster = Cluster(
        backend_type="ssh",
        callbacks=[
            LoggerCallback(),
            BenchmarkCallback(),
        ],
        **backend_kwargs,
    )

    job = hello_world.submit(
        cluster=cluster,
        account=args.account,
        partition=args.partition,
        packaging={
            "type": "wheel",
            "python_version": "3.9",
        },
    )()

    job.wait()
    result = job.get_result()
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
