import argparse
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
    """Simple hello world task that reports hostname and timestamp."""

    hostname = socket.gethostname()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello from {hostname} at {current_time}!"
    print(message)
    return message


def main():
    parser = argparse.ArgumentParser(
        description="Submit jobs to a SLURM cluster using Cluster.from_env"
    )
    parser.add_argument(
        "slurmfile",
        help="Path to the Slurmfile configuration to load.",
    )
    parser.add_argument(
        "--env",
        default="default",
        help="Environment name within the Slurmfile to use.",
    )
    args = parser.parse_args()

    configure_logging()

    cluster = Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[LoggerCallback(), BenchmarkCallback()],
    )

    job = hello_world.submit(
        cluster=cluster,
        packaging=cluster.packaging_defaults,
        account=cluster.environment_config["submit"]["account"],
        partition=cluster.environment_config["submit"]["partition"],
    )()

    job.wait()
    result = job.get_result()
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
