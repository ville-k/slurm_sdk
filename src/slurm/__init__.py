# slurm/__init__.py

"""
This package provides a Python SDK for interacting with Slurm clusters.
"""

__version__ = "0.1.0"

from .cluster import Cluster
from .decorators import task, workflow
from .job import Job
from .task import SlurmTask
from .runtime import JobContext
from .workflow import WorkflowContext
from .array_job import ArrayJob
from .context import (
    get_active_context,
    set_active_context,
    reset_active_context,
)

__all__ = [
    "task",
    "workflow",
    "Job",
    "ArrayJob",
    "Cluster",
    "SlurmTask",
    "JobContext",
    "WorkflowContext",
    "get_active_context",
    "set_active_context",
    "reset_active_context",
]
