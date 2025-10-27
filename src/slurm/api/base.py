"""
Base module for SLURM API backends.

This module defines the abstract base class for all SLURM API backends,
providing a common interface for interacting with SLURM clusters.
"""

import abc
from typing import Any, Dict, List, Optional


class BackendBase(abc.ABC):
    """
    Abstract base class for SLURM API backends.

    This class defines the interface that all SLURM API backends must implement.
    Concrete implementations include REST API, local command-line, and SSH-based backends.
    """

    @abc.abstractmethod
    def submit_job(
        self,
        script: str,
        target_job_dir: str,
        pre_submission_id: str,
        account: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> str:
        """
        Submit a job script to the SLURM cluster.

        Args:
            script: The job script content as a string.
            target_job_dir: Absolute path to the job-specific directory on the target.
            pre_submission_id: Unique ID used for filenames/paths for this submission.
            account: Optional SLURM account to use.
            partition: Optional SLURM partition to use.

        Returns:
            str: The job ID of the submitted job.

        Raises:
            Exception: If the job submission fails.
        """
        pass

    @abc.abstractmethod
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a job.

        Args:
            job_id: The ID of the job to query.

        Returns:
            Dict[str, Any]: A dictionary containing job status information.

        Raises:
            Exception: If the job status query fails.
        """
        pass

    @abc.abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a job.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            bool: True if the job was successfully canceled, False otherwise.

        Raises:
            Exception: If the job cancellation fails.
        """
        pass

    @abc.abstractmethod
    def get_queue(self) -> List[Dict[str, Any]]:
        """
        Get the current job queue.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing job information.

        Raises:
            Exception: If the queue query fails.
        """
        pass

    @abc.abstractmethod
    def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get information about the SLURM cluster.

        Returns:
            Dict[str, Any]: A dictionary containing cluster information.

        Raises:
            Exception: If the cluster info query fails.
        """
        pass

    @abc.abstractmethod
    def is_remote(self) -> bool:
        """
        Return True if this backend requires remote file operations.

        This is used to determine whether files need to be transferred
        between local and remote systems (SSH backend) or can be accessed
        directly (local backend).

        Returns:
            bool: True if backend is remote (requires file transfer),
                  False if backend is local (direct file access).
        """
        pass
