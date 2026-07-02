"""Two peers running concurrently in one local allocation."""

import logging
import os
import socket
import tempfile

from slurm import Cluster, parallel, task


@task(time="00:02:00", mem="256M", cpus_per_task=1)
def greet_ocean() -> str:
    return f"ocean on {socket.gethostname()} (pid {os.getpid()})"


@task(time="00:02:00", mem="256M", cpus_per_task=1)
def greet_atmosphere() -> str:
    return f"atmosphere on {socket.gethostname()} (pid {os.getpid()})"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    base = tempfile.mkdtemp(prefix="slurm_sdk_first_parallel_")
    cluster = Cluster(
        backend_type="local",
        job_base_dir=base,
        default_packaging="none",
    )
    with cluster:
        job = parallel(greet_ocean, greet_atmosphere)
        job.wait()
        results = job.get_results()

    print(results["greet_ocean"])
    print(results["greet_atmosphere"])


if __name__ == "__main__":
    main()
