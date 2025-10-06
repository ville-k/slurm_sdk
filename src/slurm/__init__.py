# slurm/__init__.py

"""
This package provides a Python SDK for interacting with Slurm clusters.
"""

__version__ = "0.1.0"

from .cluster import Cluster
from .decorators import task
from .job import Job
from .task import SlurmTask
from .runtime import JobContext

__all__ = [
    "task",
    "Job",
    "Cluster",
    "SlurmTask",
    "JobContext",
]
