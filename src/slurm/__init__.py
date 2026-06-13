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
from .task import SlurmTask, WorkflowTask, BoundTask
from .runtime import JobContext
from .workflow import WorkflowContext
from .array_job import ArrayJob
from .parallel import parallel, Peer, Pool, Topology
from .parallel.peer_info import PeerInfo, PeerGroup
from .parallel_job import ParallelJob, PeerOutcome
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
    TopologyError,
    PeerFailureError,
    CompositeJobError,
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
    "WorkflowTask",
    "BoundTask",
    "JobContext",
    "WorkflowContext",
    # Parallel / topology
    "parallel",
    "Peer",
    "Pool",
    "Topology",
    "ParallelJob",
    "PeerOutcome",
    "PeerInfo",
    "PeerGroup",
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
    "TopologyError",
    "PeerFailureError",
    "CompositeJobError",
]
