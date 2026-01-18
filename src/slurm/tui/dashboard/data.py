"""Data provider for the dashboard TUI.

Fetches and caches SLURM job and cluster information for display.
"""

from __future__ import annotations

import getpass
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from slurm.cluster import Cluster


@dataclass
class JobInfo:
    """Normalized job information for display."""

    job_id: str
    name: str
    state: str
    user: str
    account: str
    partition: str
    nodes: str
    time: str
    time_limit: str
    submit_time: str
    start_time: str
    end_time: str
    work_dir: str
    num_cpus: str
    reason: str
    exit_code: str

    @classmethod
    def from_queue_entry(cls, entry: Dict[str, Any]) -> "JobInfo":
        """Create JobInfo from queue entry dict.

        Args:
            entry: Raw queue entry from cluster.get_queue().

        Returns:
            Normalized JobInfo instance.
        """
        return cls(
            job_id=str(
                entry.get("JOBID", "")
                or entry.get("JobID", "")
                or entry.get("job_id", "")
                or ""
            ),
            name=str(entry.get("NAME", "") or entry.get("JobName", "") or ""),
            state=str(entry.get("STATE", "") or entry.get("JobState", "") or ""),
            user=str(entry.get("USER", "") or entry.get("UserId", "") or ""),
            account=str(entry.get("ACCOUNT", "") or entry.get("Account", "") or ""),
            partition=str(
                entry.get("PARTITION", "") or entry.get("Partition", "") or ""
            ),
            nodes=str(entry.get("NODES", "") or entry.get("NumNodes", "") or ""),
            time=str(entry.get("TIME", "") or entry.get("RunTime", "") or ""),
            time_limit=str(
                entry.get("TIME_LIMIT", "")
                or entry.get("TimeLimit", "")
                or entry.get("Timelimit", "")
                or ""
            ),
            submit_time=str(
                entry.get("SUBMIT_TIME", "") or entry.get("SubmitTime", "") or ""
            ),
            start_time=str(
                entry.get("START_TIME", "") or entry.get("StartTime", "") or ""
            ),
            end_time=str(entry.get("END_TIME", "") or entry.get("EndTime", "") or ""),
            work_dir=str(
                entry.get("WORK_DIR", "")
                or entry.get("WorkDir", "")
                or entry.get("WorkDirectory", "")
                or ""
            ),
            num_cpus=str(entry.get("CPUS", "") or entry.get("NumCPUs", "") or ""),
            reason=str(entry.get("REASON", "") or entry.get("Reason", "") or ""),
            exit_code=str(
                entry.get("EXIT_CODE", "") or entry.get("ExitCode", "") or ""
            ),
        )

    @classmethod
    def from_status(cls, job_id: str, status: Dict[str, Any]) -> "JobInfo":
        """Create JobInfo from detailed status dict.

        Args:
            job_id: The job ID.
            status: Detailed status from job.get_status() or scontrol.

        Returns:
            Normalized JobInfo instance.
        """
        return cls(
            job_id=job_id,
            name=str(status.get("JobName", "") or status.get("Name", "") or ""),
            state=str(status.get("JobState", "") or status.get("State", "") or ""),
            user=str(status.get("UserId", "") or status.get("User", "") or ""),
            account=str(status.get("Account", "") or ""),
            partition=str(status.get("Partition", "") or ""),
            nodes=str(status.get("NumNodes", "") or status.get("Nodes", "") or ""),
            time=str(status.get("RunTime", "") or ""),
            time_limit=str(
                status.get("TimeLimit", "") or status.get("Timelimit", "") or ""
            ),
            submit_time=str(status.get("SubmitTime", "") or ""),
            start_time=str(status.get("StartTime", "") or ""),
            end_time=str(status.get("EndTime", "") or ""),
            work_dir=str(
                status.get("WorkDir", "") or status.get("WorkDirectory", "") or ""
            ),
            num_cpus=str(status.get("NumCPUs", "") or ""),
            reason=str(status.get("Reason", "") or ""),
            exit_code=str(status.get("ExitCode", "") or ""),
        )


@dataclass
class PartitionInfo:
    """Normalized partition information for display."""

    name: str
    avail: str
    total_nodes: int
    node_states: Dict[str, int]
    time_limit: str
    cpus: str
    memory: str

    @property
    def state_summary(self) -> str:
        """Get human-readable summary of node states."""
        if not self.node_states:
            return "n/a"
        return ", ".join(
            f"{count} {state}" for state, count in sorted(self.node_states.items())
        )

    @property
    def is_up(self) -> bool:
        """Check if partition is available."""
        return self.avail.lower() == "up"


@dataclass
class DashboardData:
    """Container for all dashboard data."""

    my_jobs: List[JobInfo] = field(default_factory=list)
    account_jobs: Dict[str, List[JobInfo]] = field(default_factory=dict)
    partitions: List[PartitionInfo] = field(default_factory=list)
    last_refresh: Optional[datetime] = None
    current_user: str = ""
    current_account: str = ""
    environment_name: str = ""
    error: Optional[str] = None


class DashboardDataProvider:
    """Fetches and caches SLURM data for the dashboard."""

    def __init__(self, cluster: "Cluster") -> None:
        """Initialize data provider.

        Args:
            cluster: Configured Cluster instance for data access.
        """
        self.cluster = cluster
        self._data = DashboardData()
        self._current_user = os.environ.get("USER", getpass.getuser())

    @property
    def data(self) -> DashboardData:
        """Get the current dashboard data."""
        return self._data

    def refresh(self) -> DashboardData:
        """Refresh all data from SLURM.

        Returns:
            Updated DashboardData instance.
        """
        self._data.error = None

        try:
            # Get all jobs in queue
            queue = self.cluster.get_queue()
            all_jobs = [JobInfo.from_queue_entry(entry) for entry in queue]

            # Filter for current user's jobs
            self._data.my_jobs = [
                job for job in all_jobs if job.user == self._current_user
            ]

            # Find user's account from their jobs
            user_accounts = {job.account for job in self._data.my_jobs if job.account}
            self._data.current_account = (
                next(iter(user_accounts)) if user_accounts else ""
            )

            # Group account jobs by user (excluding current user)
            self._data.account_jobs = {}
            if self._data.current_account:
                account_jobs = [
                    job
                    for job in all_jobs
                    if job.account == self._data.current_account
                    and job.user != self._current_user
                ]
                for job in account_jobs:
                    if job.user not in self._data.account_jobs:
                        self._data.account_jobs[job.user] = []
                    self._data.account_jobs[job.user].append(job)

            # Get partition info
            cluster_info = self.cluster.get_cluster_info()
            self._data.partitions = self._aggregate_partitions(
                cluster_info.get("partitions", [])
            )

            self._data.current_user = self._current_user
            self._data.last_refresh = datetime.now()

        except Exception as e:
            self._data.error = str(e)

        return self._data

    def _aggregate_partitions(
        self, raw_partitions: List[Dict[str, Any]]
    ) -> List[PartitionInfo]:
        """Aggregate partition rows by partition name.

        SLURM's sinfo returns one row per partition per node state.
        This aggregates them to show one row per partition.

        Args:
            raw_partitions: Raw partition data from get_cluster_info().

        Returns:
            List of aggregated PartitionInfo objects.
        """
        aggregated: Dict[str, Dict[str, Any]] = {}

        for partition in raw_partitions:
            name = str(
                partition.get("PARTITION", "") or partition.get("partition", "") or ""
            )
            if not name:
                continue

            nodes_str = str(
                partition.get("NODES", "") or partition.get("nodes", "") or "0"
            )
            try:
                nodes = int(nodes_str)
            except ValueError:
                nodes = 0

            state = str(partition.get("STATE", "") or partition.get("state", "") or "")

            if name not in aggregated:
                aggregated[name] = {
                    "name": name,
                    "avail": str(
                        partition.get("AVAIL", "") or partition.get("avail", "") or ""
                    ),
                    "total_nodes": 0,
                    "states": {},
                    "time_limit": str(
                        partition.get("TIMELIMIT", "")
                        or partition.get("timelimit", "")
                        or ""
                    ),
                    "cpus": str(
                        partition.get("CPUS", "") or partition.get("cpus", "") or ""
                    ),
                    "memory": str(
                        partition.get("MEMORY", "") or partition.get("memory", "") or ""
                    ),
                }

            aggregated[name]["total_nodes"] += nodes
            if state and nodes > 0:
                aggregated[name]["states"][state] = (
                    aggregated[name]["states"].get(state, 0) + nodes
                )

        result = []
        for name in sorted(aggregated.keys()):
            data = aggregated[name]
            result.append(
                PartitionInfo(
                    name=data["name"],
                    avail=data["avail"],
                    total_nodes=data["total_nodes"],
                    node_states=data["states"],
                    time_limit=data["time_limit"],
                    cpus=data["cpus"],
                    memory=data["memory"],
                )
            )

        return result

    def get_job_details(self, job_id: str) -> Optional[JobInfo]:
        """Get detailed information for a specific job.

        Args:
            job_id: The job ID to get details for.

        Returns:
            JobInfo with detailed status, or None if not found.
        """
        try:
            # First check if job is in cache
            for job in self._data.my_jobs:
                if job.job_id == job_id:
                    return job

            for user_jobs in self._data.account_jobs.values():
                for job in user_jobs:
                    if job.job_id == job_id:
                        return job

            # Try to fetch from cluster
            job = self.cluster.get_job(job_id)
            status = job.get_status()
            return JobInfo.from_status(job_id, status)

        except Exception:
            return None

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job.

        Args:
            job_id: The job ID to cancel.

        Returns:
            True if cancellation was successful.
        """
        try:
            job = self.cluster.get_job(job_id)
            job.cancel()
            return True
        except Exception:
            return False
