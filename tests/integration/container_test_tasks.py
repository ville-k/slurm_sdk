"""Task functions for container packaging integration tests.

These tasks are defined in a separate module so they can be properly imported
when running inside containers (since test files aren't copied into containers).

The tasks use basic @task decorators - packaging configuration comes from
cluster-level defaults set in the tests.
"""

from __future__ import annotations

from slurm.decorators import task


@task(time="00:02:00", mem="1G")
def hello_container():
    """Basic container task that returns hostname."""
    import socket

    return f"Hello from {socket.gethostname()}"


@task(time="00:02:00", mem="1G")
def numpy_task():
    """Container task that uses numpy."""
    import numpy as np

    return int(np.array([1, 2, 3]).sum())


@task(time="00:01:00", mem="512M")
def process_item(item: int) -> int:
    """Process a single item in an array job."""
    return item * 2


@task(time="00:02:00", mem="1G")
def read_mounted_file(file_path: str) -> str:
    """Read a file from a mounted volume."""
    with open(file_path) as f:
        return f.read()
