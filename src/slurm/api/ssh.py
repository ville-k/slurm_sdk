"""
SSH-based backend for Slurm API.

This module provides a backend implementation that interacts with Slurm
using SSH to execute commands on a remote Slurm cluster.
"""

import os
import socket
import tempfile
import time
import uuid
import re
from typing import Any, Dict, List, Optional

import paramiko
import logging
import shlex

from .base import BackendBase
from ..errors import BackendTimeout, BackendCommandError, BackendError

logger = logging.getLogger(__name__)


class SSHCommandBackend(BackendBase):
    """
    SLURM backend that uses SSH to execute commands on a remote cluster.

    This backend establishes an SSH connection to a remote SLURM cluster
    and executes SLURM commands (sbatch, squeue, etc.) remotely.
    """

    def __init__(
        self,
        hostname: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        port: int = 22,
        remote_workdir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: int = 10,
        connection_attempts: int = 3,
        retry_delay: int = 2,
        allow_agent: bool = True,
        look_for_keys: bool = True,
        banner_timeout: int = 15,
        auth_timeout: int = 30,
        gss_auth: bool = False,
        gss_kex: bool = False,
        gss_deleg_creds: bool = True,
        gss_host: Optional[str] = None,
        disabled_algorithms: Optional[Dict[str, List[str]]] = None,
        job_base_dir: Optional[str] = None,
    ):
        """
        Initialize the SSH command backend.

        Args:
            hostname: The hostname of the SLURM cluster.
            username: The username to use for SSH authentication.
            password: The password to use for SSH authentication.
            key_filename: The path to the private key file to use for SSH authentication.
            port: The SSH port to connect to.
            remote_workdir: The remote working directory to use.
            env: Optional environment variables to use when executing commands.
            timeout: Socket timeout in seconds.
            connection_attempts: Number of connection attempts before giving up.
            retry_delay: Delay between connection attempts in seconds.
            allow_agent: Whether to allow the use of the SSH agent.
            look_for_keys: Whether to search for discoverable private key files in ~/.ssh/.
            banner_timeout: Timeout for the SSH banner in seconds.
            auth_timeout: Timeout for SSH authentication in seconds.
            gss_auth: Whether to use GSS-API authentication.
            gss_kex: Whether to use GSS-API key exchange.
            gss_deleg_creds: Whether to delegate GSS-API credentials.
            gss_host: The target name for GSS-API authentication.
            disabled_algorithms: Dictionary of disabled algorithms by type.
            job_base_dir: The base directory for job-related files.
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.port = port
        self.remote_workdir = remote_workdir or "~"
        self.env = env or {}
        self.timeout = timeout
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay
        self.allow_agent = allow_agent
        self.look_for_keys = look_for_keys
        self.banner_timeout = banner_timeout
        self.auth_timeout = auth_timeout
        self.gss_auth = gss_auth
        self.gss_kex = gss_kex
        self.gss_deleg_creds = gss_deleg_creds
        self.gss_host = gss_host
        self.disabled_algorithms = disabled_algorithms

        # Store and resolve job_base_dir
        self._raw_job_base_dir = job_base_dir or "~/slurm_jobs"  # Store the raw path
        self.job_base_dir = None  # Will be resolved after connection

        # Connect to the remote host
        self._connect()

        # Resolve job_base_dir on the remote host AFTER connecting
        self.job_base_dir = self._resolve_remote_path(self._raw_job_base_dir)
        logger.debug(
            "SSHCommandBackend using resolved job base directory: %s", self.job_base_dir
        )

        # Create the base job directory on the remote host if it doesn't exist
        self._run_command(f"mkdir -p {shlex.quote(self.job_base_dir)}")

        # Create a temporary directory on the remote host (maybe within job_base_dir?)
        # For now, keep it separate as before
        self.remote_temp_dir = self._create_remote_temp_dir()
        logger.debug("Created remote temporary directory: %s", self.remote_temp_dir)

    def _connect(self) -> None:
        """
        Connect to the remote host.

        Raises:
            Exception: If the connection fails.
        """
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh_config = paramiko.SSHConfig()
        user_config_file = os.path.expanduser("~/.ssh/config")
        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config.parse(f)

        host_config = ssh_config.lookup(self.hostname)

        connect_kwargs = {
            "hostname": host_config.get("hostname", self.hostname),
            "port": int(host_config.get("port", self.port)),
            "username": self.username or host_config.get("user"),
            "password": self.password,
            "timeout": self.timeout,
            "allow_agent": self.allow_agent,
            "look_for_keys": self.look_for_keys,
            "banner_timeout": self.banner_timeout,
            "auth_timeout": self.auth_timeout,
            "gss_auth": self.gss_auth,
            "gss_kex": self.gss_kex,
            "gss_deleg_creds": self.gss_deleg_creds,
            "gss_host": self.gss_host,
            "disabled_algorithms": self.disabled_algorithms,
        }

        if self.key_filename:
            connect_kwargs["key_filename"] = self.key_filename
        elif "identityfile" in host_config:
            connect_kwargs["key_filename"] = host_config["identityfile"][0]

        last_error = None
        for attempt in range(self.connection_attempts):
            try:
                logger.debug(
                    "Connecting to %s (attempt %d/%d)...",
                    self.hostname,
                    attempt + 1,
                    self.connection_attempts,
                )

                self.client.connect(**connect_kwargs)
                self.sftp = self.client.open_sftp()

                logger.info("Connected to %s", self.hostname)
                return
            except Exception as e:
                last_error = e
                logger.error("Connection attempt %d failed: %s", attempt + 1, e)
                if attempt < self.connection_attempts - 1:
                    logger.debug("Retrying in %d seconds...", self.retry_delay)
                    time.sleep(self.retry_delay)

        raise RuntimeError(
            f"Failed to connect to {self.hostname} after {self.connection_attempts} attempts.\n"
            f"Last error: {last_error}\n"
            f"Common fixes:\n"
            f"  - Verify SSH access: ssh {self.username}@{self.hostname}\n"
            f"  - Check SSH config at ~/.ssh/config\n"
            f"  - Ensure SSH keys or password are configured correctly"
        )

    def _run_command(self, cmd, timeout=30, retry_count=1):
        """
        Run a command on the remote host.

        Args:
            cmd: The command to run.
            timeout: Timeout in seconds.
            retry_count: Number of times to retry on failure.

        Returns:
            Tuple[str, str, int]: A tuple of (stdout, stderr, return_code).

        Raises:
            RuntimeError: If the command fails after all retries.
        """
        for attempt in range(retry_count):
            try:
                _, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
                exit_status = stdout.channel.recv_exit_status()
                stdout_str = stdout.read().decode("utf-8")
                stderr_str = stderr.read().decode("utf-8")
                return stdout_str, stderr_str, exit_status
            except socket.timeout:
                if attempt < retry_count - 1:
                    logger.debug(
                        "Command timed out, retrying (%d/%d)...",
                        attempt + 1,
                        retry_count,
                    )
                    continue
                raise BackendTimeout(
                    f"Command timed out after {timeout} seconds: {cmd}"
                )
            except Exception as e:
                if attempt < retry_count - 1:
                    logger.debug(
                        "Command failed, retrying (%d/%d): %s",
                        attempt + 1,
                        retry_count,
                        e,
                    )
                    continue
                raise BackendCommandError(f"Failed to execute command: {e}")

        # This should never be reached
        raise BackendCommandError("Unexpected error in _run_command")

    def _mkdir_remote(self, path: str) -> None:
        """Ensure a remote directory exists."""
        stdout, stderr, ret = self._run_command(f"mkdir -p {shlex.quote(path)}")
        if ret != 0:
            raise RuntimeError(f"Failed to create directory {path}: {stderr}")

    def _make_remote_executable(self, path: str) -> None:
        """Mark a remote file as executable."""
        stdout, stderr, ret = self._run_command(f"chmod +x {shlex.quote(path)}")
        if ret != 0:
            raise RuntimeError(f"Failed to chmod +x {path}: {stderr}")

    def _build_sbatch_command(
        self,
        *,
        target_job_dir: str,
        account: Optional[str],
        partition: Optional[str],
        script_path: str,
        array_spec: Optional[str] = None,
    ) -> str:
        parts = ["sbatch", f"--chdir={shlex.quote(target_job_dir)}"]
        if account:
            parts.append(f"--account={shlex.quote(account)}")
        if partition:
            parts.append(f"--partition={shlex.quote(partition)}")
        if array_spec:
            parts.append(f"--array={array_spec}")
        parts.append(shlex.quote(script_path))
        return " ".join(parts)

    def _upload_string_to_file(self, content: str, remote_path: str) -> None:
        """
        Upload a string to a file on the remote host.

        Args:
            content: The string content to upload.
            remote_path: The path to the remote file.

        Raises:
            Exception: If the upload fails.
        """
        try:
            # Create parent directory if it doesn't exist
            parent_dir = os.path.dirname(remote_path)
            if parent_dir:
                self._run_command(f"mkdir -p {parent_dir}")

            # Write the content to a temporary file, ensuring Unix line endings
            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, newline="\n"
            ) as temp_file:
                temp_file.write(content)
                temp_file_path = temp_file.name

            # Upload the temporary file
            try:
                self.sftp.put(temp_file_path, remote_path)
            finally:
                # Clean up the temporary file
                os.unlink(temp_file_path)

        except Exception as e:
            raise RuntimeError(f"Failed to upload content to file: {e}")

    def _create_remote_temp_dir(self) -> str:
        """
        Create a temporary directory on the remote host.

        Returns:
            str: The path to the created temporary directory.

        Raises:
            Exception: If the directory creation fails.
        """
        # Create a temporary directory on the remote host
        stdout, stderr, return_code = self._run_command("mktemp -d")

        if return_code != 0:
            # Fallback to creating a directory in /tmp
            unique_id = str(uuid.uuid4())[:8]
            temp_dir = f"/tmp/slurm_ssh_{unique_id}"
            stdout, stderr, return_code = self._run_command(
                f"mkdir -p {temp_dir} && echo {temp_dir}"
            )

            if return_code != 0:
                raise RuntimeError(f"Failed to create temporary directory: {stderr}")

        return stdout.strip()

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
        Submit a job to the SLURM cluster, creating the target directory first.

        Args:
            script: The job script content to submit.
            target_job_dir: The absolute path to the directory for job files.
            pre_submission_id: The unique ID used in target_job_dir and filenames.
            account: Optional SLURM account to use.
            partition: Optional SLURM partition to use.
            array_spec: Optional array specification for native SLURM arrays.
                Format: "0-N" or "0-N%M" where M is max concurrent tasks.

        Returns:
            str: The job ID. For array jobs, returns in format "12345_[0-N]".

        Raises:
            RuntimeError: If the job submission fails.
        """
        # Persist script in job directory
        script_filename = f"slurm_job_{pre_submission_id}_script.sh"
        persistent_script_path = f"{target_job_dir}/{script_filename}"

        # Also use a temporary location for sbatch (some clusters may need this)
        remote_script_filename = f"job_{uuid.uuid4().hex[:8]}.sh"
        if not self.remote_temp_dir or not self.remote_temp_dir.startswith("/"):
            raise RuntimeError(
                f"Invalid remote_temp_dir configured: {self.remote_temp_dir}"
            )
        remote_script_path = f"{self.remote_temp_dir}/{remote_script_filename}"

        try:
            logger.debug("Ensuring target job directory exists: %s", target_job_dir)
            self._mkdir_remote(target_job_dir)

            logger.debug(
                "--- BEGIN SCRIPT REPR ---\n%s\n--- END SCRIPT REPR ---", repr(script)
            )

            # Write script to persistent location in job directory
            logger.debug("Writing script to job directory: %s", persistent_script_path)
            self._upload_string_to_file(script, persistent_script_path)
            self._make_remote_executable(persistent_script_path)

            # Also write to temp location for sbatch (if different)
            if persistent_script_path != remote_script_path:
                logger.debug(
                    "Uploading job script to temp location: %s", remote_script_path
                )
                self._upload_string_to_file(script, remote_script_path)
                self._make_remote_executable(remote_script_path)
            else:
                remote_script_path = persistent_script_path

            sbatch_cmd_to_run = self._build_sbatch_command(
                target_job_dir=target_job_dir,
                account=account,
                partition=partition,
                script_path=remote_script_path,
                array_spec=array_spec,
            )

            if array_spec:
                logger.debug("Submitting array job via sbatch (array=%s)", array_spec)
            else:
                logger.debug("Submitting job via sbatch")
            logger.debug("Running command: %s", sbatch_cmd_to_run)
            stdout, stderr, return_code = self._run_command(sbatch_cmd_to_run)

            if return_code != 0:
                raise RuntimeError(
                    f"Failed to submit job ({remote_script_path}): {stderr}"
                )

            match = re.search(r"Submitted batch job (\d+)", stdout)
            if not match:
                raise RuntimeError(
                    f"Failed to parse job ID from sbatch output: {stdout}"
                )

            job_id = match.group(1)

            # For array jobs, SLURM returns the base job ID
            # We'll format it as "JOB_ID_[array_spec]" for consistency
            if array_spec:
                job_id = f"{job_id}_[{array_spec}]"
                logger.info("Array job submitted: %s", job_id)
            else:
                logger.info("Job submitted: %s", job_id)
            logger.debug("Submitted from script: %s", remote_script_path)
            logger.debug("Script persisted at: %s", persistent_script_path)
            return job_id

        except Exception as e:
            logger.error("Error during job submission process: %s", e)
            raise

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a job.

        Args:
            job_id: The job ID.

        Returns:
            Dict[str, Any]: The job status.

        Raises:
            RuntimeError: If the command fails.
        """
        try:
            # Use a simpler format that's faster to execute
            stdout, stderr, return_code = self._run_command(
                f"scontrol show job {job_id}", timeout=20, retry_count=2
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
                    f"  3. Network connectivity problems\n\n"
                    f"To diagnose:\n"
                    f"  scontrol show job {job_id}  # Run this manually to see SLURM's response"
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
                f"  1. SSH connection was lost\n"
                f"  2. Parsing error in SLURM output format\n"
                f"  3. Unexpected SLURM response\n\n"
                f"To diagnose:\n"
                f"  1. Check SSH connection: ssh {getattr(self, 'hostname', 'cluster')} echo 'connected'\n"
                f"  2. Try manual SLURM command: ssh {getattr(self, 'hostname', 'cluster')} scontrol show job {job_id}"
            ) from e

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
        _, stderr, return_code = self._run_command(f"scancel {job_id}")

        if return_code != 0:
            raise BackendCommandError(f"Job cancellation failed: {stderr}")

        return True

    def get_queue(self) -> List[Dict[str, Any]]:
        """
        Get the current job queue.

        Returns:
            List[Dict[str, Any]]: A list of jobs in the queue.

        Raises:
            RuntimeError: If the command fails.
        """
        try:
            # Use a simpler format that's faster to execute
            stdout, stderr, return_code = self._run_command(
                "squeue -h -o '%A|%j|%T|%u|%M|%l|%P|%a'", timeout=30, retry_count=2
            )

            if return_code != 0:
                raise RuntimeError(f"Failed to get queue: {stderr}")

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

            return jobs
        except TimeoutError as e:
            logger.warning("Warning: %s", e)
            return []  # Return empty list instead of failing
        except Exception as e:
            logger.warning("Warning: Failed to get queue: %s", e)
            return []  # Return empty list instead of failing

    def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get information about the cluster.

        Returns:
            Dict[str, Any]: Information about the cluster.

        Raises:
            RuntimeError: If the command fails.
        """
        try:
            # Get partition information with a simpler format
            stdout, stderr, return_code = self._run_command(
                "sinfo -h -o '%R|%a|%l|%D|%T'", timeout=20, retry_count=2
            )

            if return_code != 0:
                raise RuntimeError(f"Failed to get cluster info: {stderr}")

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

            return {"partitions": partitions}
        except BackendTimeout:
            # Re-raise timeout errors as-is
            raise
        except Exception as e:
            logger.error("Failed to get cluster info: %s", e)
            raise BackendError(
                "Failed to get cluster information.\n\n"
                f"Error: {e}\n\n"
                "This usually happens when:\n"
                "  1. SLURM controller is not running\n"
                "  2. Network issues prevent SSH connection\n"
                "  3. 'sinfo' command is not available on the cluster\n\n"
                "To diagnose:\n"
                "  1. Check SLURM controller status: ssh {hostname} systemctl status slurmctld\n"
                "  2. Verify sinfo works: ssh {hostname} sinfo\n"
                "  3. Check SSH connection: ssh {hostname} echo 'connected'\n\n"
                "Note: This error won't affect job submission, but may limit partition information.".format(
                    hostname=getattr(self, "hostname", "cluster")
                )
            ) from e

    def __del__(self):
        """
        Clean up resources when the object is destroyed.
        """
        if hasattr(self, "client") and self.client:
            try:
                # Clean up the remote temporary directory
                if hasattr(self, "remote_temp_dir"):
                    try:
                        self._run_command(f"rm -rf {self.remote_temp_dir}")
                    except Exception as e:
                        # Catch all exceptions in __del__ - we can't propagate them anyway
                        logger.debug(f"Error cleaning up remote temp dir: {e}")

                # Close the SFTP connection
                if hasattr(self, "sftp") and self.sftp:
                    try:
                        self.sftp.close()
                    except Exception as e:
                        # Catch all exceptions in __del__ - we can't propagate them anyway
                        logger.debug(f"Error closing SFTP connection: {e}")

                # Close the SSH connection
                self.client.close()
            except Exception as e:
                # Catch all exceptions in __del__ - we can't propagate them anyway
                logger.debug(f"Error during cleanup: {e}")

    def _upload_file(self, local_path: str, remote_path: str) -> None:
        """
        Upload a file to the remote host using the paramiko SFTP client.

        Args:
            local_path: Path to the local file
            remote_path: Path to the remote file

        Raises:
            RuntimeError: If the upload fails
        """
        try:
            # Expand ~ in remote path if present
            if remote_path.startswith("~"):
                # Get the home directory
                stdout, stderr, return_code = self._run_command("echo $HOME")
                if return_code != 0:
                    raise RuntimeError(f"Failed to get home directory: {stderr}")

                home_dir = stdout.strip()
                remote_path = remote_path.replace("~", home_dir, 1)

            # Create the directory if it doesn't exist
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                self._run_command(f"mkdir -p {remote_dir}")

            # Ensure we have an SFTP connection
            if not hasattr(self, "sftp") or self.sftp is None:
                self.sftp = self.client.open_sftp()

            # Upload the file
            logger.info("Uploading %s to %s", local_path, remote_path)
            self.sftp.put(local_path, remote_path)
            logger.info("Upload successful")

        except Exception as e:
            logger.error("Error uploading file: %s", e)
            raise RuntimeError(f"Failed to upload file: {e}")

    def execute_command(self, command: str) -> str:
        """
        Execute a command on the remote host.

        Args:
            command: The command to execute

        Returns:
            The command output
        """
        if not self.client:
            self._connect()

        try:
            stdin, stdout, stderr = self.client.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                error = stderr.read().decode("utf-8")
                logger.error("Error executing command: %s", command)
                logger.error("Exit status: %s", exit_status)
                logger.error("Error output: %s", error)
                raise RuntimeError(
                    f"Command failed with exit status {exit_status}: {error}"
                )

            return stdout.read().decode("utf-8")
        except Exception as e:
            logger.error("Error executing command: %s", e)
            raise

    def download_file(self, remote_path, local_path):
        """
        Download a file from the remote host.

        Args:
            remote_path: The path to the remote file.
            local_path: The path to the local file.
        """
        try:
            # Ensure local directory exists
            local_dir = os.path.dirname(local_path)
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)

            logger.debug("Attempting to download %s to %s", remote_path, local_path)
            sftp = self._get_sftp_client()
            sftp.get(remote_path, local_path)
            logger.debug("Successfully downloaded %s to %s", remote_path, local_path)
        except Exception as e:
            # More specific error handling (e.g., file not found)
            logger.error(
                "Error downloading file %s to %s: %s", remote_path, local_path, e
            )
            # Re-raise the specific exception type if possible, or a generic runtime error
            if isinstance(e, FileNotFoundError) or (
                hasattr(e, "errno") and e.errno == 2
            ):  # paramiko might raise IOError with errno 2
                raise FileNotFoundError(f"Remote file not found: {remote_path}") from e
            raise RuntimeError(f"Failed to download file: {e}") from e

    def _get_sftp_client(self):
        """Helper to get an SFTP client."""
        if not self.client:
            self._connect()  # Ensure connection is active
        if not self.client:
            raise RuntimeError("SSH client not connected.")

        try:
            return self.client.open_sftp()
        except Exception as e:
            logger.error(f"Failed to open SFTP session: {e}")
            raise RuntimeError("Failed to open SFTP session") from e

    def upload_file(self, local_path: str, remote_path: str):
        """
        Uploads a local file to the remote host.

        Args:
            local_path: Path to the local file.
            remote_path: Path to the destination on the remote host.
        """
        logger.debug(f"Uploading {local_path} to {self.hostname}:{remote_path}")
        sftp = None
        try:
            sftp = self._get_sftp_client()
            # Ensure remote directory exists (optional, but helpful)
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                try:
                    # Use POSIX path separators for remote commands
                    remote_dir_posix = remote_dir.replace("\\", "/")
                    self.execute_command(f"mkdir -p {shlex.quote(remote_dir_posix)}")
                except Exception as mkdir_e:
                    logger.warning(
                        f"Could not create remote directory {remote_dir}: {mkdir_e}. Upload might fail."
                    )

            sftp.put(local_path, remote_path)
            logger.debug(f"Successfully uploaded {local_path} to {remote_path}")
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to upload file {local_path} to {remote_path}: {e}")
            raise RuntimeError(f"SFTP upload failed: {e}") from e
        finally:
            if sftp:
                sftp.close()

    def get_remote_upload_base_path(self) -> str:
        """Provides the base directory intended for uploads (e.g., the job base dir)."""
        if not self.job_base_dir:
            raise RuntimeError("Job base directory not resolved yet.")
        return self.job_base_dir

    def read_file(self, remote_path: str) -> str:
        """
        Read a file from the remote host.

        Args:
            remote_path: The path to the remote file.

        Returns:
            str: The file contents as a string.

        Raises:
            FileNotFoundError: If the file does not exist.
            RuntimeError: If the read operation fails.
        """
        try:
            logger.debug(f"Reading remote file: {remote_path}")
            sftp = self._get_sftp_client()

            with sftp.open(remote_path, "r") as remote_file:
                content = remote_file.read().decode("utf-8")

            logger.debug(f"Successfully read {len(content)} bytes from {remote_path}")
            return content

        except IOError as e:
            # Paramiko raises IOError for file not found
            if e.errno == 2:  # ENOENT - No such file or directory
                raise FileNotFoundError(f"Remote file not found: {remote_path}") from e
            raise RuntimeError(f"Failed to read file {remote_path}: {e}") from e
        except Exception as e:
            logger.error(f"Error reading file {remote_path}: {e}")
            raise RuntimeError(f"Failed to read file {remote_path}: {e}") from e

    def _resolve_remote_path(self, path: str) -> str:
        """Resolves a path (potentially containing ~) on the remote host."""
        if not path:
            return ""

        # Check if path starts with ~/
        if path.startswith("~/") or path == "~":
            try:
                # Get home directory
                home_cmd = "echo $HOME"
                stdout, stderr, retcode = self._run_command(home_cmd)
                if retcode != 0:
                    raise RuntimeError(
                        f"Failed to get remote home directory. Stderr: {stderr}"
                    )
                home_dir = stdout.strip()
                if not home_dir:
                    raise ValueError("Remote home directory path is empty.")

                # Replace ~ with the absolute home directory path
                if path == "~":
                    resolved = home_dir
                else:  # Starts with ~/
                    resolved = os.path.join(
                        home_dir, path[2:]
                    )  # Use os.path.join for robustness

                # Ensure forward slashes for consistency on remote system
                resolved = resolved.replace("\\", "/")
                logger.debug("Resolved path '%s' to '%s'", path, resolved)
                return resolved

            except Exception as e:
                logger.error(
                    "Error resolving remote path '%s' using home dir: %s", path, e
                )
                raise RuntimeError(f"Could not resolve remote path: {path}") from e
        else:
            # Assume it's already absolute or relative path not needing expansion
            # Still ensure forward slashes
            logger.debug(
                "Path '%s' does not start with ~, returning as is (with forward slashes).",
                path,
            )
            return path.replace("\\", "/")

    def is_remote(self) -> bool:
        """Return True since SSH backend requires remote file operations."""
        return True
