# slurm/__init__.py

"""
This package provides a Python SDK for interacting with Slurm clusters.
"""

try:
    from importlib.metadata import version

    __version__ = version("slurm-sdk")
except Exception:
    __version__ = "unknown"

from .cluster import Cluster
from .decorators import task, workflow
from .job import Job
from .task import SlurmTask
from .runtime import JobContext
from .workflow import WorkflowContext
from .array_job import ArrayJob
from .callbacks import (
    BaseCallback,
    LoggerCallback,
    BenchmarkCallback,
)
from .errors import (
    SubmissionError,
    DownloadError,
    BackendError,
    BackendTimeout,
    BackendCommandError,
    PackagingError,
    SlurmfileError,
)

__all__ = [
    # Decorators
    "task",
    "workflow",
    # Core classes
    "Job",
    "ArrayJob",
    "Cluster",
    "SlurmTask",
    "JobContext",
    "WorkflowContext",
    # Callbacks
    "BaseCallback",
    "LoggerCallback",
    "BenchmarkCallback",
    # Errors
    "SubmissionError",
    "DownloadError",
    "BackendError",
    "BackendTimeout",
    "BackendCommandError",
    "PackagingError",
    "SlurmfileError",
]
