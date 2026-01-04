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


# ============================================================================
# Tasks for Workflow Tests (used by wheel packaging tests)
# ============================================================================


@task(time="00:01:00", mem="512M")
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


@task(time="00:01:00", mem="512M")
def multiply(a: int, b: int) -> int:
    """Multiply two numbers."""
    return a * b


@task(time="00:01:00", mem="512M")
def failing_task() -> int:
    """Task that intentionally fails."""
    raise ValueError("This task is designed to fail")


# ============================================================================
# Plain Functions for Container Packaging Workflow Tests
# ============================================================================
# These are plain functions (not decorated) that will be wrapped with
# container packaging configuration in the tests themselves


def container_add(a: int, b: int) -> int:
    """Add two numbers (for container packaging)."""
    return a + b


def container_multiply(a: int, b: int) -> int:
    """Multiply two numbers (for container packaging)."""
    return a * b


def container_failing_task() -> int:
    """Task that intentionally fails (for container packaging)."""
    raise ValueError("This task is designed to fail")


# ============================================================================
# Tasks for Output Directory Tests
# ============================================================================


@task(time="00:01:00", mem="512M")
def write_output_file(filename: str, content: str, job) -> str:
    """Write content to a file in the output directory."""
    if job.output_dir is None:
        raise ValueError("output_dir is not set in JobContext")

    output_path = job.output_dir / filename
    output_path.write_text(content)
    return str(output_path)


@task(time="00:01:00", mem="512M")
def read_output_file(filename: str, job) -> str:
    """Read content from a file in the output directory."""
    if job.output_dir is None:
        raise ValueError("output_dir is not set in JobContext")

    output_path = job.output_dir / filename
    return output_path.read_text()


@task(time="00:01:00", mem="512M")
def get_output_dir_path(job) -> str:
    """Return the output directory path."""
    return str(job.output_dir) if job.output_dir else ""


# ============================================================================
# Workflow Definitions for Container Packaging Tests
# ============================================================================
# These workflows will be created in the tests after wrapping the plain
# functions with container packaging configuration
