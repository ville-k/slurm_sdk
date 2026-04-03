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
from .decorators import task, workflow, parse_packaging_config
from .job import Job, JobSnapshot
from .task import SlurmTask
from .runtime import JobContext
from .workflow import WorkflowContext
from .array_job import ArrayJob
from .packaging import PackagingConfig
from .callbacks import (
    BaseCallback,
    LoggerCallback,
    BenchmarkCallback,
    RichLoggerCallback,
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
    # Decorators and utilities
    "task",
    "workflow",
    "parse_packaging_config",
    # Core classes
    "Job",
    "JobSnapshot",
    "ArrayJob",
    "Cluster",
    "SlurmTask",
    "JobContext",
    "WorkflowContext",
    # Configuration
    "PackagingConfig",
    # Callbacks
    "BaseCallback",
    "LoggerCallback",
    "BenchmarkCallback",
    "RichLoggerCallback",
    # Errors
    "SubmissionError",
    "DownloadError",
    "BackendError",
    "BackendTimeout",
    "BackendCommandError",
    "PackagingError",
    "SlurmfileError",
]
