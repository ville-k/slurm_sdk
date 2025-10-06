"""
No-packaging strategy - assumes code is already available on the cluster.
"""

from typing import Any, Dict, List, Optional, Union, Callable

from .base import PackagingStrategy

# Avoid circular imports for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..task import SlurmTask


class NonePackagingStrategy(PackagingStrategy):
    """
    Strategy that performs no packaging, assuming code is already available.
    """

    def prepare(
        self, task: Union["SlurmTask", Callable], cluster: "Cluster"
    ) -> Dict[str, Any]:
        """
        No preparation needed for this strategy.

        Args:
            task: The task to package
            cluster: The cluster to submit to

        Returns:
            Empty dict as no packaging was performed
        """
        return {"status": "success", "message": "No packaging required."}

    def generate_setup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate minimal setup commands.

        Args:
            task: The task to set up
            job_id: The SLURM job ID
            job_dir: The job directory

        Returns:
            List of shell commands for environment setup
        """
        return []

    def generate_cleanup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate cleanup commands.

        Args:
            task: The task to clean up after
            job_id: The SLURM job ID
            job_dir: The job directory

        Returns:
            List of shell commands for cleanup
        """
        return []
