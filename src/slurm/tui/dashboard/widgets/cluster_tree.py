"""Navigation tree widget for the dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Optional

from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from ..data import DashboardData, JobInfo, PartitionInfo
from ...common.styles import get_state_color

if TYPE_CHECKING:
    pass


class NodeType(Enum):
    """Types of nodes in the navigation tree."""

    ROOT = "root"
    MY_JOBS = "my_jobs"
    ACCOUNT_JOBS = "account_jobs"
    CLUSTER_STATUS = "cluster_status"
    JOB = "job"
    USER = "user"
    PARTITION = "partition"


@dataclass
class TreeNodeData:
    """Data attached to tree nodes."""

    node_type: NodeType
    job: Optional[JobInfo] = None
    partition: Optional[PartitionInfo] = None
    user: Optional[str] = None


class ClusterTree(Tree[TreeNodeData]):
    """Navigation tree for cluster hierarchy.

    Displays:
    - My Jobs (current user's jobs)
    - Account Jobs (grouped by user)
    - Cluster Status (partitions)
    """

    DEFAULT_CSS = """
    ClusterTree {
        width: 100%;
        height: 100%;
        scrollbar-gutter: stable;
    }

    ClusterTree > .tree--cursor {
        background: $accent;
    }

    ClusterTree .job-running {
        color: green;
    }

    ClusterTree .job-pending {
        color: yellow;
    }

    ClusterTree .job-completed {
        color: blue;
    }

    ClusterTree .job-failed {
        color: red;
    }

    ClusterTree .job-cancelled {
        color: magenta;
    }

    ClusterTree .partition-up {
        color: green;
    }

    ClusterTree .partition-down {
        color: red;
    }
    """

    def __init__(
        self,
        label: str = "Cluster",
        data: Optional[TreeNodeData] = None,
        **kwargs,
    ) -> None:
        """Initialize the cluster tree.

        Args:
            label: Root label for the tree.
            data: Optional data for root node.
            **kwargs: Additional Tree arguments.
        """
        if data is None:
            data = TreeNodeData(node_type=NodeType.ROOT)
        super().__init__(label, data=data, **kwargs)

    def update_data(self, data: DashboardData) -> None:
        """Update tree with new dashboard data.

        Args:
            data: DashboardData containing jobs and partitions.
        """
        self.clear()

        # Update root label with environment info
        if data.environment_name:
            self.root.label = f"📦 {data.environment_name}"
        else:
            self.root.label = "📦 Cluster"

        # Add My Jobs section
        my_jobs_count = len(data.my_jobs)
        my_jobs_node = self.root.add(
            f"👤 My Jobs ({my_jobs_count})",
            data=TreeNodeData(node_type=NodeType.MY_JOBS),
            expand=True,
        )
        for job in data.my_jobs:
            self._add_job_node(my_jobs_node, job)

        # Add Account Jobs section
        total_account_jobs = sum(len(jobs) for jobs in data.account_jobs.values())
        if data.current_account:
            account_label = f"👥 Account: {data.current_account} ({total_account_jobs})"
        else:
            account_label = f"👥 Account Jobs ({total_account_jobs})"

        account_node = self.root.add(
            account_label,
            data=TreeNodeData(node_type=NodeType.ACCOUNT_JOBS),
            expand=False,
        )
        for user, jobs in sorted(data.account_jobs.items()):
            user_node = account_node.add(
                f"👤 {user} ({len(jobs)})",
                data=TreeNodeData(node_type=NodeType.USER, user=user),
            )
            for job in jobs:
                self._add_job_node(user_node, job)

        # Add Cluster Status section
        cluster_node = self.root.add(
            f"🖥️ Cluster Status ({len(data.partitions)})",
            data=TreeNodeData(node_type=NodeType.CLUSTER_STATUS),
            expand=True,
        )
        for partition in data.partitions:
            self._add_partition_node(cluster_node, partition)

        self.root.expand()

    def _add_job_node(self, parent: TreeNode[TreeNodeData], job: JobInfo) -> None:
        """Add a job node to the tree.

        Args:
            parent: Parent tree node.
            job: Job information.
        """
        state_icon = self._get_state_icon(job.state)
        color = get_state_color(job.state)
        label = f"{state_icon} [{color}]{job.job_id}[/] {job.name}"

        parent.add_leaf(
            label,
            data=TreeNodeData(node_type=NodeType.JOB, job=job),
        )

    def _add_partition_node(
        self, parent: TreeNode[TreeNodeData], partition: PartitionInfo
    ) -> None:
        """Add a partition node to the tree.

        Args:
            parent: Parent tree node.
            partition: Partition information.
        """
        if partition.is_up:
            status_icon = "🟢"
            color = "green"
        else:
            status_icon = "🔴"
            color = "red"

        label = f"{status_icon} [{color}]{partition.name}[/] ({partition.total_nodes} nodes)"

        parent.add_leaf(
            label,
            data=TreeNodeData(node_type=NodeType.PARTITION, partition=partition),
        )

    def _get_state_icon(self, state: str) -> str:
        """Get icon for job state.

        Args:
            state: Job state string.

        Returns:
            Emoji icon for the state.
        """
        base_state = state.split()[0].upper() if state else ""
        icons = {
            "RUNNING": "▶️",
            "PENDING": "⏳",
            "COMPLETED": "✅",
            "FAILED": "❌",
            "CANCELLED": "🚫",
            "TIMEOUT": "⏰",
            "NODE_FAIL": "💥",
            "COMPLETING": "🔄",
            "CONFIGURING": "⚙️",
            "SUSPENDED": "⏸️",
        }
        return icons.get(base_state, "•")

    def get_selected_data(self) -> Optional[TreeNodeData]:
        """Get data for the currently selected node.

        Returns:
            TreeNodeData for selected node, or None.
        """
        if self.cursor_node and self.cursor_node.data:
            return self.cursor_node.data
        return None
