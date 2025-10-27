"""
Local backend for Slurm API.

This module provides a backend implementation that interacts with Slurm
using direct command execution on the local cluster (without SSH).
This is intended for use when running jobs on a Slurm cluster node,
such as workflow orchestrators running within a Slurm job.
"""

import os
import re
import subprocess
import tempfile
import uuid
import shlex
import logging
from typing import Any, Dict, List, Optional

from .base import BackendBase
from ..errors import BackendTimeout, BackendCommandError, BackendError

logger = logging.getLogger(__name__)


class LocalBackend(BackendBase):
    """
    SLURM backend that uses direct command execution.

    This backend executes SLURM commands (sbatch, squeue, etc.) directly
    on the local system without SSH. It's designed for use when running
    on a Slurm cluster node, such as workflow orchestrators.
    """

    def __init__(
        self,
        job_base_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ):
        """
        Initialize the local command backend.

        Args:
            job_base_dir: The base directory for job-related files.
            env: Optional environment variables to use when executing commands.
            timeout: Command timeout in seconds.
        """
        self.env = env or {}
        self.timeout = timeout

        # Resolve job_base_dir
        self._raw_job_base_dir = job_base_dir or "~/slurm_jobs"
        self.job_base_dir = self._resolve_path(self._raw_job_base_dir)

        logger.debug("LocalBackend using job base directory: %s", self.job_base_dir)

        # Create the base job directory if it doesn't exist
        os.makedirs(self.job_base_dir, exist_ok=True)
        logger.debug("Ensured job base directory exists: %s", self.job_base_dir)

    def _resolve_path(self, path: str) -> str:
        """
        Resolve a path (potentially containing ~) on the local system.

        Args:
            path: The path to resolve

        Returns:
            The resolved absolute path
        """
        if not path:
            return ""

        # Expand ~ and environment variables
        expanded = os.path.expanduser(os.path.expandvars(path))

        # Convert to absolute path
        absolute = os.path.abspath(expanded)

        logger.debug("Resolved path '%s' to '%s'", path, absolute)
        return absolute

    def _run_command(
        self, cmd: str, timeout: Optional[int] = None, check: bool = True
    ) -> tuple[str, str, int]:
        """
        Run a command on the local system.

        Args:
            cmd: The command to run
            timeout: Timeout in seconds (defaults to self.timeout)
            check: Whether to check return code

        Returns:
            Tuple[str, str, int]: A tuple of (stdout, stderr, return_code)

        Raises:
            BackendTimeout: If the command times out
            BackendCommandError: If the command fails and check=True
        """
        if timeout is None:
            timeout = self.timeout

        try:
            logger.debug("Running command: %s", cmd)

            # Merge environment variables
            env = os.environ.copy()
            env.update(self.env)

            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )

            logger.debug("Command exit code: %d", result.returncode)
            if result.stdout:
                logger.debug("Command stdout: %s", result.stdout[:500])
            if result.stderr:
                logger.debug("Command stderr: %s", result.stderr[:500])

            if check and result.returncode != 0:
                raise BackendCommandError(
                    f"Command failed with exit code {result.returncode}: {result.stderr}"
                )

            return result.stdout, result.stderr, result.returncode

        except subprocess.TimeoutExpired as e:
            raise BackendTimeout(f"Command timed out after {timeout} seconds: {cmd}")
        except Exception as e:
            if isinstance(e, (BackendTimeout, BackendCommandError)):
                raise
            raise BackendCommandError(f"Failed to execute command: {e}")

    def submit_job(
        self,
        script: str,
        target_job_dir: str,
        pre_submission_id: str,
        account: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> str:
        """
        Submit a job to the SLURM cluster.

        Args:
            script: The job script content to submit
            target_job_dir: The absolute path to the directory for job files
            pre_submission_id: The unique ID used in target_job_dir and filenames
            account: Optional SLURM account to use
            partition: Optional SLURM partition to use

        Returns:
            str: The job ID

        Raises:
            RuntimeError: If the job submission fails
        """
        logger.debug("Submitting job to local Slurm cluster")
        logger.debug("Target job directory: %s", target_job_dir)

        # Ensure target job directory exists
        os.makedirs(target_job_dir, exist_ok=True)
        logger.debug("Ensured target job directory exists: %s", target_job_dir)

        # Write script to temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False, newline="\n"
        ) as f:
            f.write(script)
            script_path = f.name

        try:
            # Make script executable
            os.chmod(script_path, 0o755)

            # Build sbatch command
            sbatch_cmd_parts = ["sbatch", f"--chdir={shlex.quote(target_job_dir)}"]

            if account:
                sbatch_cmd_parts.append(f"--account={shlex.quote(account)}")
            if partition:
                sbatch_cmd_parts.append(f"--partition={shlex.quote(partition)}")

            sbatch_cmd_parts.append(shlex.quote(script_path))
            sbatch_cmd = " ".join(sbatch_cmd_parts)

            logger.debug("Submitting with command: %s", sbatch_cmd)
            logger.debug(
                "--- BEGIN SCRIPT CONTENT ---\n%s\n--- END SCRIPT CONTENT ---", script
            )

            # Execute sbatch
            stdout, stderr, return_code = self._run_command(sbatch_cmd, check=False)

            if return_code != 0:
                raise RuntimeError(f"Failed to submit job: {stderr}")

            # Parse job ID from output
            match = re.search(r"Submitted batch job (\d+)", stdout)
            if not match:
                raise RuntimeError(
                    f"Failed to parse job ID from sbatch output: {stdout}"
                )

            job_id = match.group(1)
            logger.info("Job submitted: %s", job_id)
            return job_id

        finally:
            # Clean up temporary script file
            try:
                os.unlink(script_path)
            except Exception as e:
                logger.warning("Failed to clean up temporary script file: %s", e)

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a job.

        Args:
            job_id: The job ID

        Returns:
            Dict[str, Any]: The job status

        Raises:
            RuntimeError: If the command fails
        """
        try:
            stdout, stderr, return_code = self._run_command(
                f"scontrol show job {job_id}", check=False
            )

            logger.debug("Job status stdout: %s", stdout)
            logger.debug("Job status stderr: %s", stderr)
            logger.debug("Job status return_code: %s", return_code)

            if return_code != 0:
                if "Invalid job id specified" in stderr:
                    raise BackendCommandError(
                        f"Job {job_id} not found in SLURM queue.\n\n"
                        f"This job may have:\n"
                        f"  1. Already completed and been purged from the queue\n"
                        f"  2. Never existed (wrong job ID)\n"
                        f"  3. Been cancelled\n\n"
                        f"To check job history:\n"
                        f"  sacct -j {job_id}  # Show completed/failed jobs\n"
                        f"  squeue -j {job_id}  # Show only running/pending jobs"
                    )
                # Non-zero exit indicates command failure
                error_msg = stderr.strip() or "Unknown error"
                raise BackendCommandError(
                    f"Failed to get status for job {job_id}.\n\n"
                    f"SLURM command failed with: {error_msg}\n\n"
                    f"Possible causes:\n"
                    f"  1. SLURM controller is down or unreachable\n"
                    f"  2. Permission issues accessing job information\n"
                    f"  3. Local SLURM installation issues\n\n"
                    f"To diagnose:\n"
                    f"  scontrol show job {job_id}  # Run this manually to see SLURM's response\n"
                    f"  systemctl status slurmd  # Check if SLURM daemon is running"
                )

            # Parse the output
            status = {}
            for line in stdout.strip().split("\n"):
                for item in line.strip().split():
                    if "=" in item:
                        key, value = item.split("=", 1)
                        status[key] = value

            logger.debug("Job status: %s", status)
            return status

        except BackendTimeout:
            # Re-raise timeout errors as-is
            raise
        except BackendCommandError:
            # Re-raise command errors as-is
            raise
        except Exception as e:
            logger.error("Failed to get job status for %s: %s", job_id, e)
            raise BackendError(
                f"Unexpected error while getting status for job {job_id}.\n\n"
                f"Error: {e}\n\n"
                f"This may indicate:\n"
                f"  1. Local SLURM commands failed\n"
                f"  2. Parsing error in SLURM output format\n"
                f"  3. Permission issues\n\n"
                f"To diagnose:\n"
                f"  1. Check SLURM is running: systemctl status slurmd\n"
                f"  2. Try manual command: scontrol show job {job_id}"
            ) from e

    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a job.

        Args:
            job_id: The ID of the job to cancel

        Returns:
            bool: True if the job was successfully canceled

        Raises:
            BackendCommandError: If the job cancellation fails
        """
        logger.debug("Cancelling job: %s", job_id)

        stdout, stderr, return_code = self._run_command(
            f"scancel {job_id}", check=False
        )

        if return_code != 0:
            raise BackendCommandError(f"Job cancellation failed: {stderr}")

        logger.info("Job cancelled: %s", job_id)
        return True

    def get_queue(self) -> List[Dict[str, Any]]:
        """
        Get the current job queue.

        Returns:
            List[Dict[str, Any]]: A list of jobs in the queue

        Raises:
            RuntimeError: If the command fails
        """
        try:
            stdout, stderr, return_code = self._run_command(
                "squeue -h -o '%A|%j|%T|%u|%M|%l|%P|%a'", check=False
            )

            if return_code != 0:
                logger.warning("Failed to get queue: %s", stderr)
                return []

            # Parse the output
            jobs = []
            for line in stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) >= 8:
                    (
                        job_id,
                        job_name,
                        state,
                        user,
                        time,
                        time_limit,
                        partition,
                        account,
                    ) = parts[:8]
                    jobs.append(
                        {
                            "JOBID": job_id,
                            "NAME": job_name,
                            "STATE": state,
                            "USER": user,
                            "TIME": time,
                            "TIME_LIMIT": time_limit,
                            "PARTITION": partition,
                            "ACCOUNT": account,
                        }
                    )

            logger.debug("Found %d jobs in queue", len(jobs))
            return jobs

        except BackendTimeout as e:
            logger.warning("Warning: %s", e)
            return []
        except Exception as e:
            logger.warning("Warning: Failed to get queue: %s", e)
            return []

    def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get information about the cluster.

        Returns:
            Dict[str, Any]: Information about the cluster

        Raises:
            RuntimeError: If the command fails
        """
        try:
            stdout, stderr, return_code = self._run_command(
                "sinfo -h -o '%R|%a|%l|%D|%T'", check=False
            )

            if return_code != 0:
                logger.error("Failed to get cluster info: %s", stderr)
                raise BackendCommandError(
                    "Failed to get cluster information from local SLURM installation.\n\n"
                    f"SLURM command (sinfo) failed with: {stderr.strip()}\n\n"
                    "Possible causes:\n"
                    "  1. SLURM is not installed on this machine\n"
                    "  2. SLURMcontroller is not running\n"
                    "  3. Permission issues running sinfo command\n\n"
                    "To diagnose:\n"
                    "  1. Check SLURM installation: which sinfo\n"
                    "  2. Verify SLURM services: systemctl status slurmctld slurmd\n"
                    "  3. Try manually: sinfo -h -o '%R|%a|%l|%D|%T'\n\n"
                    "Note: This error won't affect job submission, but may limit partition information."
                )

            # Parse the output
            partitions = []
            for line in stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) >= 5:
                    partition, avail, time_limit, nodes, state = parts[:5]
                    partitions.append(
                        {
                            "PARTITION": partition,
                            "AVAIL": avail,
                            "TIMELIMIT": time_limit,
                            "NODES": nodes,
                            "STATE": state,
                        }
                    )

            logger.debug("Found %d partitions", len(partitions))
            return {"partitions": partitions}

        except BackendTimeout:
            # Re-raise timeout errors as-is
            raise
        except BackendCommandError:
            # Re-raise command errors as-is
            raise
        except Exception as e:
            logger.error("Failed to get cluster info: %s", e)
            raise BackendError(
                "Unexpected error while getting cluster information.\n\n"
                f"Error: {e}\n\n"
                "This may indicate:\n"
                "  1. Local SLURM installation issues\n"
                "  2. Parsing error in sinfo output\n"
                "  3. Unexpected SLURM response format\n\n"
                "To diagnose:\n"
                "  1. Check SLURM status: systemctl status slurmctld\n"
                "  2. Try manual command: sinfo\n\n"
                "Note: This error won't prevent job submission."
            ) from e

    def execute_command(self, command: str) -> str:
        """
        Execute a command on the local system.

        Args:
            command: The command to execute

        Returns:
            The command output

        Raises:
            RuntimeError: If the command fails
        """
        stdout, stderr, return_code = self._run_command(command, check=False)

        if return_code != 0:
            raise RuntimeError(
                f"Command failed with exit status {return_code}: {stderr}"
            )

        return stdout

    def read_file(self, file_path: str) -> str:
        """
        Read a file from the local filesystem.

        Args:
            file_path: The path to the file to read

        Returns:
            str: The file contents as a string

        Raises:
            FileNotFoundError: If the file does not exist
            RuntimeError: If the read operation fails
        """
        try:
            logger.debug(f"Reading local file: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            logger.debug(f"Successfully read {len(content)} bytes from {file_path}")
            return content

        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {file_path}") from e
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise RuntimeError(f"Failed to read file {file_path}: {e}") from e

    def is_remote(self) -> bool:
        """Return False since local backend uses direct file access."""
        return False
