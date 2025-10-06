"""
Base classes and interfaces for packaging strategies.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable

# Avoid circular imports for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..task import SlurmTask


class PackagingStrategy(ABC):
    """Base class for packaging strategies."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    @abstractmethod
    def prepare(
        self, task: Union["SlurmTask", Callable], cluster: "Cluster"
    ) -> Dict[str, Any]:
        """
        Prepare the package before job submission (e.g., build wheel/container).

        Returns:
            A dictionary containing information about the prepared package
            (e.g., path to wheel, container image name).
        """
        pass

    @abstractmethod
    # Add job_id parameter with default None for compatibility
    def generate_setup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate bash commands to set up the environment within the SLURM job script.

        Args:
            task: The task being submitted.
            job_id: The SLURM job ID string (e.g., '$SLURM_JOB_ID') for use in paths/commands.
            job_dir: The job directory for use in paths/commands.

        Returns:
            A list of bash command strings.
        """
        pass

    @abstractmethod
    # Add job_id parameter with default None for compatibility
    def generate_cleanup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate bash commands to clean up the environment after the task runs.

        Args:
            task: The task being submitted.
            job_id: The SLURM job ID string (e.g., '$SLURM_JOB_ID') for use in paths/commands.
            job_dir: The job directory for use in paths/commands.

        Returns:
            A list of bash command strings.
        """
        pass

    def wrap_execution_command(
        self,
        command: str,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> str:
        """Allow strategies to wrap the python execution command.

        Container-based strategies can override this hook to prepend commands
        (for example, ``srun --container-image``)
        while the default implementation leaves the command unchanged.
        """

        return command
