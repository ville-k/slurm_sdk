"""Live dashboard components for the slurm CLI."""

from __future__ import annotations

from .cluster_dashboard import ClusterDashboard
from .jobs_dashboard import JobsDashboard

__all__ = ["ClusterDashboard", "JobsDashboard"]
