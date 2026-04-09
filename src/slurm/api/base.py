"""
Base module for SLURM API backends.

This module defines the abstract base class for all SLURM API backends,
providing a common interface for interacting with SLURM clusters.
"""

import abc
import threading
from typing import Any, Callable, Dict, List, Optional


class BackendBase(abc.ABC):
    """
    Abstract base class for SLURM API backends.

    This class defines the interface that all SLURM API backends must implement.
    Concrete implementations include REST API, local command-line, and SSH-based backends.
    """

    hostname: str = "localhost"
    """Hostname of the target system. SSH backends override via instance attribute."""

    @abc.abstractmethod
    def submit_job(
        self,
        script: str,
        target_job_dir: str,
        pre_submission_id: str,
        account: Optional[str] = None,
        partition: Optional[str] = None,
        array_spec: Optional[str] = None,
    ) -> str:
        """
        Submit a job script to the SLURM cluster.

        Args:
            script: The job script content as a string.
            target_job_dir: Absolute path to the job-specific directory on the target.
            pre_submission_id: Unique ID used for filenames/paths for this submission.
            account: Optional SLURM account to use.
            partition: Optional SLURM partition to use.
            array_spec: Optional array specification for native SLURM arrays.
                Format: "0-N" or "0-N%M" where M is max concurrent tasks.
                Example: "0-99" or "0-99%10" (max 10 concurrent).
                If provided, submits as native SLURM array job using --array flag.

        Returns:
            str: The job ID of the submitted job. For array jobs, returns the
                base job ID in SLURM array format (e.g., "12345_[0-99]").

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
    def get_job_accounting(self, job_id: str) -> Dict[str, Any]:
        """
        Get job information from Slurm accounting (for completed jobs).

        This uses sacct to query job history, which works for jobs that have
        completed and left the queue.

        Args:
            job_id: The ID of the job to query.

        Returns:
            Dict[str, Any]: A dictionary containing job accounting information.

        Raises:
            Exception: If the accounting query fails.
        """
        pass

    @abc.abstractmethod
    def get_account_jobs(
        self, account: str, start_time: str, end_time: str = "now"
    ) -> List[Dict[str, Any]]:
        """
        Query sacct for all jobs in an account within a time range.

        Args:
            account: The Slurm account name to query.
            start_time: Start of time range (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).
            end_time: End of time range (default: "now").

        Returns:
            List[Dict[str, Any]]: List of job dictionaries with fields:
                - JobID: Job identifier
                - JobName: Name of the job
                - User: Username who submitted the job
                - Account: Slurm account
                - State: Job state (COMPLETED, FAILED, etc.)
                - ExitCode: Job exit code
                - AllocTRES: Allocated trackable resources (e.g., "gres/gpu=4")
                - AllocGRES: Alias for AllocTRES (backwards compatibility)
                - AllocNodes: Number of allocated nodes
                - Start: Job start time
                - End: Job end time
                - Elapsed: Job elapsed time (format: DD-HH:MM:SS)
                - Partition: Slurm partition

        Raises:
            Exception: If the query fails.
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

    @abc.abstractmethod
    def read_file(self, file_path: str) -> str:
        """Read the contents of a file on the target system.

        Args:
            file_path: Absolute path to the file to read.

        Returns:
            str: The file contents as a string.

        Raises:
            FileNotFoundError: If the file does not exist.
            RuntimeError: If the read fails.
        """
        pass

    @abc.abstractmethod
    def execute_command(self, command: str) -> str:
        """Execute a shell command on the target system and return stdout.

        Args:
            command: The shell command to execute.

        Returns:
            str: The standard output from the command.

        Raises:
            RuntimeError: If the command fails.
        """
        pass

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download/copy a file from the target system to a local path.

        For remote backends (SSH), this transfers the file over the network.
        For local backends, this copies the file locally.

        The default implementation performs a local file copy, suitable for
        backends where the filesystem is directly accessible.

        Args:
            remote_path: Absolute path to the source file on the target system.
            local_path: Absolute path to the destination file on the local system.

        Raises:
            FileNotFoundError: If the source file does not exist.
            RuntimeError: If the copy/download fails.
        """
        import shutil

        shutil.copy2(remote_path, local_path)

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload/copy a local file to the target system.

        For remote backends (SSH), this transfers the file over the network.
        For local backends, this copies the file locally.

        The default implementation performs a local file copy with parent
        directory creation, suitable for backends with direct filesystem access.

        Args:
            local_path: Absolute path to the source file on the local system.
            remote_path: Absolute path to the destination on the target system.

        Raises:
            FileNotFoundError: If the source file does not exist.
            RuntimeError: If the copy/upload fails.
        """
        import os
        import shutil

        os.makedirs(os.path.dirname(remote_path), exist_ok=True)
        shutil.copy2(local_path, remote_path)

    def write_file(self, file_path: str, content: str) -> None:
        """Write string content to a file on the target system.

        Args:
            file_path: Absolute path to the destination file.
            content: The string content to write.

        Raises:
            NotImplementedError: If the backend does not support this operation.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement write_file()"
        )

    def close(self) -> None:
        """Release any resources held by this backend.

        The default implementation is a no-op. Backends that hold
        persistent connections (e.g. SSH) should override this.
        """
        pass

    def reconnect(self) -> None:
        """Close and re-establish the backend connection.

        The default implementation is a no-op. Backends that hold
        persistent connections (e.g. SSH) should override this.
        Useful for recovering from stale connections in long-lived
        sessions (e.g. Jupyter notebooks).
        """
        pass

    def tail_file(
        self,
        path: str,
        *,
        follow: bool = True,
        lines: int = 10,
        on_line: Callable[[str], None],
        stop_event: Optional[threading.Event] = None,
        poll_interval: float = 1.0,
    ) -> None:
        """Stream lines from a file on the target system.

        Args:
            path: Absolute path to the file.
            follow: If True, keep reading new content (like tail -f).
                If False, read last N lines and return.
            lines: Number of initial lines to emit.
            on_line: Callback invoked for each line of text.
            stop_event: Threading event to signal the method to stop following.
            poll_interval: Seconds between re-reads when following.
        """
        raise NotImplementedError("tail_file is not supported by this backend")
