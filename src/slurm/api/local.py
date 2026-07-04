"""
Local backend for Slurm API.

This module provides a backend implementation that interacts with Slurm
using direct command execution on the local cluster (without SSH).
This is intended for use when running jobs on a Slurm cluster node,
such as workflow orchestrators running within a Slurm job.
"""

import os
import re
import shutil
import subprocess  # nosec B404 - subprocess is required for SLURM command execution
import sys
import logging
import threading
import time as time_mod
from typing import Any, Callable, Dict, List, Optional, Union

from .base import BackendBase
from ._parsing import (
    parse_scontrol_status,
    parse_sacct_accounting,
    parse_sacct_account_jobs,
    parse_squeue_output,
    parse_sinfo_output,
)
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
        script_permissions: int = 0o750,
    ):
        """
        Initialize the local command backend.

        Args:
            job_base_dir: The base directory for job-related files.
            env: Optional environment variables to use when executing commands.
            timeout: Command timeout in seconds.
            script_permissions: Unix file permissions for job scripts (default: 0o750).
                Use 0o755 if SLURM requires world-readable scripts on your cluster.
        """
        self.env = env or {}
        self.timeout = timeout
        self.script_permissions = script_permissions

        # Resolve job_base_dir
        self._raw_job_base_dir = job_base_dir or "~/slurm_jobs"
        self.job_base_dir = self._resolve_path(self._raw_job_base_dir)

        logger.debug("LocalBackend using job base directory: %s", self.job_base_dir)

        # Create the base job directory if it doesn't exist
        os.makedirs(self.job_base_dir, exist_ok=True)
        logger.debug("Ensured job base directory exists: %s", self.job_base_dir)

        # Parallel-job bookkeeping for the Slurm-less bypass path. When
        # ``submit_job`` sees a rendered parallel script and ``sbatch`` is not
        # available (or ``SLURM_SDK_FORCE_LOCAL_PARALLEL=1`` is set) it runs
        # the Python supervisor directly and tracks the resulting Popen here
        # so ``get_job_status`` / ``cancel_job`` / ``get_job_accounting`` can
        # report on it without hitting a Slurm controller that isn't there.
        self._local_parallel_jobs: Dict[str, Dict[str, Any]] = {}
        self._local_parallel_lock = threading.Lock()

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
        self,
        cmd: Union[str, List[str]],
        timeout: Optional[int] = None,
        check: bool = True,
    ) -> tuple[str, str, int]:
        """
        Run a command on the local system.

        Args:
            cmd: The command to run. If a list, shell=False is used (safer).
                 If a string, shell=True is used (for backward compatibility).
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

        # Determine shell mode based on command type
        # List commands use shell=False (safer, no injection risk)
        # String commands use shell=True (for backward compat with execute_command)
        use_shell = isinstance(cmd, str)

        try:
            logger.debug("Running command: %s (shell=%s)", cmd, use_shell)

            # Merge environment variables
            env = os.environ.copy()
            env.update(self.env)

            result = subprocess.run(  # nosec B603 B602 - shell mode determined by input type
                cmd,
                shell=use_shell,
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

        except subprocess.TimeoutExpired:
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
        array_spec: Optional[str] = None,
    ) -> str:
        """
        Submit a job to the SLURM cluster.

        Args:
            script: The job script content to submit
            target_job_dir: The absolute path to the directory for job files
            pre_submission_id: The unique ID used in target_job_dir and filenames
            account: Optional SLURM account to use
            partition: Optional SLURM partition to use
            array_spec: Optional array specification for native SLURM arrays.
                Format: "0-N" or "0-N%M" where M is max concurrent tasks.

        Returns:
            str: The job ID. For array jobs, returns in format "12345_[0-N]".

        Raises:
            RuntimeError: If the job submission fails
        """
        if array_spec:
            logger.debug(
                "Submitting array job to local Slurm cluster (array=%s)", array_spec
            )
        else:
            logger.debug("Submitting job to local Slurm cluster")
        logger.debug("Target job directory: %s", target_job_dir)

        # Ensure target job directory exists
        os.makedirs(target_job_dir, exist_ok=True)
        logger.debug("Ensured target job directory exists: %s", target_job_dir)

        persistent_script_path = self._persist_script(
            script,
            target_job_dir=target_job_dir,
            filename=f"slurm_job_{pre_submission_id}_script.sh",
        )

        if self._should_bypass_sbatch(script):
            raise RuntimeError(
                "Parallel local-mode bypass now requires the prepared "
                "submission path. submit_parallel_spec(...) should call "
                "LocalBackend.submit_prepared_parallel_job(...) instead of "
                "routing through submit_job(...)."
            )

        try:
            # Build sbatch command as a list (uses shell=False for safety)
            sbatch_cmd: List[str] = ["sbatch", f"--chdir={target_job_dir}"]

            if account:
                sbatch_cmd.append(f"--account={account}")
            if partition:
                sbatch_cmd.append(f"--partition={partition}")
            if array_spec:
                sbatch_cmd.append(f"--array={array_spec}")

            sbatch_cmd.append(persistent_script_path)

            logger.debug("Submitting with command: %s", sbatch_cmd)
            logger.debug(
                "--- BEGIN SCRIPT CONTENT ---\n%s\n--- END SCRIPT CONTENT ---", script
            )

            # Execute sbatch (list format uses shell=False)
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

            # For array jobs, SLURM returns the base job ID
            # We'll format it as "JOB_ID_[array_spec]" for consistency
            if array_spec:
                job_id = f"{job_id}_[{array_spec}]"
                logger.info("Array job submitted: %s", job_id)
            else:
                logger.info("Job submitted: %s", job_id)
            logger.debug("Script persisted at: %s", persistent_script_path)
            return job_id

        except Exception:
            # On error, try to clean up the script file
            try:
                if os.path.exists(persistent_script_path):
                    os.unlink(persistent_script_path)
            except Exception as cleanup_error:
                logger.warning("Failed to clean up script file: %s", cleanup_error)
            raise

    def _persist_script(
        self,
        script: str,
        *,
        target_job_dir: str,
        filename: str,
    ) -> str:
        """Write a script to ``target_job_dir`` and return its full path."""
        persistent_script_path = os.path.join(target_job_dir, filename)
        logger.debug("Writing script to job directory: %s", persistent_script_path)
        with open(persistent_script_path, "w", newline="\n") as f:
            f.write(script)
        # nosec B103 - permissions are configurable, default 0o750 is more restrictive
        os.chmod(persistent_script_path, self.script_permissions)
        return persistent_script_path

    # -----------------------------------------------------------------
    # Local parallel-mode helpers (bypass sbatch when Slurm is absent)
    # -----------------------------------------------------------------

    _PARALLEL_SCRIPT_SENTINEL = "slurm.parallel.topology_supervisor"

    def _should_bypass_sbatch(self, script: str) -> bool:
        """Return True when we should run the parallel supervisor directly.

        The bypass fires when the script is a parallel(...) rendered script
        AND ``sbatch`` is not on ``PATH`` (developer workstation without
        Slurm). An explicit ``SLURM_SDK_FORCE_LOCAL_PARALLEL=1`` env var
        forces the bypass even when sbatch is present — useful for tests
        and for users who want to iterate on topology locally before
        submitting to a real cluster.
        """
        if self._PARALLEL_SCRIPT_SENTINEL not in script:
            return False
        if os.environ.get("SLURM_SDK_FORCE_LOCAL_PARALLEL", "").lower() in (
            "1",
            "true",
            "yes",
        ):
            return True
        return shutil.which("sbatch") is None

    def submit_prepared_parallel_job(
        self,
        *,
        prepared_submission: Any,
        script: str,
        target_job_dir: str,
        pre_submission_id: str,
    ) -> str:
        """Run a prepared parallel submission locally without ``sbatch``.

        The caller provides the prepared artifact bundle so local mode does
        not need to infer side effects by mutating the rendered batch script.
        The backend writes a dedicated prep script, runs it to materialize the
        shared inputs, then launches bootstrap and the supervisor directly.
        """
        from ..parallel.rendering import render_parallel_local_prep_script

        os.makedirs(target_job_dir, exist_ok=True)
        self._persist_script(
            script,
            target_job_dir=target_job_dir,
            filename=f"slurm_job_{pre_submission_id}_script.sh",
        )
        prep_script = render_parallel_local_prep_script(prepared_submission)
        prep_script_path = self._persist_script(
            prep_script,
            target_job_dir=target_job_dir,
            filename=f"slurm_job_{pre_submission_id}_local_prep.sh",
        )

        env = os.environ.copy()
        env.update(self.env)
        env.update(dict(prepared_submission.environment_exports))
        env.update(dict(prepared_submission.python_runtime_exports))
        env["JOB_DIR"] = target_job_dir
        env["PY_EXEC_RESOLVED"] = sys.executable
        env["PYTHONUNBUFFERED"] = "1"

        prep_result = subprocess.run(  # nosec B603 - running our own generated prep script
            ["/bin/bash", prep_script_path],
            cwd=target_job_dir,
            env=env,
            capture_output=True,
            text=True,
        )
        if prep_result.returncode != 0:
            raise RuntimeError(
                "Local-mode parallel prep script failed with exit code "
                f"{prep_result.returncode}:\nstdout: {prep_result.stdout}\n"
                f"stderr: {prep_result.stderr}"
            )

        stdout_path = os.path.join(target_job_dir, f"slurm_{pre_submission_id}.out")
        stderr_path = os.path.join(target_job_dir, f"slurm_{pre_submission_id}.err")
        self._ensure_local_parallel_output_files(stdout_path, stderr_path)
        self._append_text_if_present(stdout_path, prep_result.stdout)
        self._append_text_if_present(stderr_path, prep_result.stderr)

        bootstrap_result = subprocess.run(  # nosec B603 - trusted SDK module invocation
            [
                sys.executable,
                "-m",
                "slurm.parallel.topology_bootstrap",
                "--job-dir",
                target_job_dir,
            ],
            cwd=target_job_dir,
            env=env,
            capture_output=True,
            text=True,
        )
        self._append_text_if_present(stdout_path, bootstrap_result.stdout)
        self._append_text_if_present(stderr_path, bootstrap_result.stderr)
        if bootstrap_result.returncode != 0:
            raise RuntimeError(
                "Local-mode parallel bootstrap failed with exit code "
                f"{bootstrap_result.returncode}:\nstdout: {bootstrap_result.stdout}\n"
                f"stderr: {bootstrap_result.stderr}"
            )

        stdout_fh = open(stdout_path, "ab")
        stderr_fh = open(stderr_path, "ab")
        try:
            proc = subprocess.Popen(  # nosec B603 - trusted SDK module invocation
                [
                    sys.executable,
                    "-m",
                    "slurm.parallel.topology_supervisor",
                    "--job-dir",
                    target_job_dir,
                    "--local-mode",
                ],
                cwd=target_job_dir,
                env=env,
                stdout=stdout_fh,
                stderr=stderr_fh,
                start_new_session=True,
            )
        except Exception:
            stdout_fh.close()
            stderr_fh.close()
            raise

        job_id = str(proc.pid)
        with self._local_parallel_lock:
            self._local_parallel_jobs[job_id] = {
                "process": proc,
                "target_job_dir": target_job_dir,
                "pre_submission_id": pre_submission_id,
                "stdout_fh": stdout_fh,
                "stderr_fh": stderr_fh,
                "start_time": time_mod.time(),
                "end_time": None,
            }

        logger.info(
            "Parallel job launched locally (dedicated prep path) as PID %s in %s",
            job_id,
            target_job_dir,
        )
        return job_id

    def _ensure_local_parallel_output_files(
        self, stdout_path: str, stderr_path: str
    ) -> None:
        """Create the local-mode log files eagerly for tailing/polling."""
        for path in (stdout_path, stderr_path):
            if not os.path.exists(path):
                with open(path, "a"):
                    pass

    def _append_text_if_present(self, path: str, text: str) -> None:
        """Append captured text to ``path`` when there is anything to write."""
        if not text:
            return
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(text)

    def _local_job_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Return the bookkeeping dict for a locally-launched parallel job."""
        with self._local_parallel_lock:
            return self._local_parallel_jobs.get(job_id)

    def _status_for_local_parallel(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize a scontrol-style status dict for a local parallel Popen."""
        proc: subprocess.Popen = info["process"]
        rc = proc.poll()
        if rc is None:
            return {
                "JobId": str(proc.pid),
                "JobState": "RUNNING",
                "ExitCode": "0:0",
                "Partition": "local",
            }
        # Terminal — cache end_time and translate the return code.
        if info.get("end_time") is None:
            info["end_time"] = time_mod.time()
        if rc == 0:
            state = "COMPLETED"
            exit_code = "0:0"
        elif rc < 0:
            # Killed by signal — shell convention.
            state = "CANCELLED"
            exit_code = f"0:{-rc}"
        else:
            state = "FAILED"
            exit_code = f"{rc}:0"
        # Close the captured stdio handles once the process is terminal so
        # file descriptors don't leak for long-running test sessions.
        for key in ("stdout_fh", "stderr_fh"):
            fh = info.get(key)
            if fh is not None and not fh.closed:
                try:
                    fh.close()
                except Exception:
                    pass
        return {
            "JobId": str(proc.pid),
            "JobState": state,
            "ExitCode": exit_code,
            "Partition": "local",
        }

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
        info = self._local_job_info(job_id)
        if info is not None:
            return self._status_for_local_parallel(info)
        try:
            stdout, stderr, return_code = self._run_command(
                ["scontrol", "show", "job", job_id], check=False
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

            status = parse_scontrol_status(stdout)
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

    def get_job_accounting(self, job_id: str) -> Dict[str, Any]:
        """Get job information from Slurm accounting (for completed jobs)."""
        info = self._local_job_info(job_id)
        if info is not None:
            # Synthesize a minimal accounting record matching the sacct
            # parser's shape. End-of-life data is pulled from the Popen
            # bookkeeping — no Slurm involved.
            status = self._status_for_local_parallel(info)
            start = info.get("start_time")
            end = info.get("end_time")
            elapsed = (end - start) if (start and end) else 0
            return {
                "JobId": job_id,
                "State": status["JobState"],
                "JobState": status["JobState"],
                "ExitCode": status["ExitCode"],
                "Start": (
                    time_mod.strftime("%Y-%m-%dT%H:%M:%S", time_mod.localtime(start))
                    if start
                    else ""
                ),
                "End": (
                    time_mod.strftime("%Y-%m-%dT%H:%M:%S", time_mod.localtime(end))
                    if end
                    else ""
                ),
                "Elapsed": f"{int(elapsed // 3600):02d}:"
                f"{int((elapsed % 3600) // 60):02d}:{int(elapsed % 60):02d}",
            }
        try:
            result = subprocess.run(
                [
                    "sacct",
                    "-j",
                    job_id,
                    "--format=JobID,State,ExitCode,Start,End,Elapsed",
                    "--parsable2",
                    "--noheader",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to get accounting info for job {job_id}: {result.stderr}"
                )

            try:
                return parse_sacct_accounting(result.stdout, job_id)
            except ValueError as exc:
                raise RuntimeError(str(exc)) from exc
        except Exception as e:
            raise RuntimeError(f"Failed to get job accounting for {job_id}: {e}") from e

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
            List[Dict[str, Any]]: List of job dictionaries with fields.

        Raises:
            RuntimeError: If the command fails.
        """
        try:
            cmd = [
                "sacct",
                "-A",
                account,
                "-S",
                start_time,
                "-E",
                end_time,
                "--format=JobID,JobName,User,Account,State,ExitCode,AllocTRES,AllocNodes,Start,End,Elapsed,Partition",
                "--parsable2",
                "--noheader",
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to get jobs for account {account}: {result.stderr}"
                )

            if not result.stdout.strip():
                logger.info("No jobs found for account %s", account)
                return []

            jobs = parse_sacct_account_jobs(result.stdout)
            logger.debug("Found %d jobs for account %s", len(jobs), account)
            return jobs

        except Exception as e:
            logger.error("Failed to get jobs for account %s: %s", account, e)
            raise RuntimeError(f"Failed to get jobs for account {account}: {e}") from e

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

        info = self._local_job_info(job_id)
        if info is not None:
            proc: subprocess.Popen = info["process"]
            try:
                # Terminate the supervisor's process group so its peer
                # children receive SIGTERM too. The supervisor's own
                # handler triggers the local-mode shutdown cascade.
                import signal as _signal

                os.killpg(os.getpgid(proc.pid), _signal.SIGTERM)
            except ProcessLookupError:
                pass
            except Exception as exc:
                logger.debug("killpg for local parallel job %s failed: %s", job_id, exc)
                try:
                    proc.terminate()
                except Exception:
                    pass
            return True

        stdout, stderr, return_code = self._run_command(
            ["scancel", job_id], check=False
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
            # %A=JobID, %j=Name, %T=State, %u=User, %S=StartTime, %M=TimeUsed,
            # %l=TimeLimit, %P=Partition, %a=Account, %D=NumNodes, %r=Reason
            # Using list format (shell=False) - format string doesn't need shell quoting
            stdout, stderr, return_code = self._run_command(
                ["squeue", "-h", "-o", "%A|%j|%T|%u|%S|%M|%l|%P|%a|%D|%r"], check=False
            )

            if return_code != 0:
                logger.warning("Failed to get queue: %s", stderr)
                return []

            jobs = parse_squeue_output(stdout)
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
            # Using list format (shell=False) - format string doesn't need shell quoting
            stdout, stderr, return_code = self._run_command(
                ["sinfo", "-h", "-o", "%R|%a|%l|%D|%T"], check=False
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

            partitions = parse_sinfo_output(stdout)
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
        import shlex

        stdout, stderr, return_code = self._run_command(
            shlex.split(command), check=False
        )

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

    def write_file(self, file_path: str, content: str) -> None:
        """Write string content to a local file."""
        try:
            parent = os.path.dirname(file_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(file_path, "w", encoding="utf-8", newline="\n") as f:
                f.write(content)
        except Exception as e:
            raise RuntimeError(f"Failed to write file {file_path}: {e}") from e

    def is_remote(self) -> bool:
        """Return False since local backend uses direct file access."""
        return False

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
        """Stream lines from a local file.

        Args:
            path: Absolute path to the file.
            follow: If True, keep reading new content (like tail -f).
                If False, read last N lines and return.
            lines: Number of initial lines to emit.
            on_line: Callback invoked for each line of text.
            stop_event: Threading event to signal the method to stop following.
            poll_interval: Seconds between re-reads when following.
        """
        stop_event = stop_event or threading.Event()

        while not os.path.exists(path):
            if stop_event.is_set():
                return
            time_mod.sleep(1.0)

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
            all_lines = content.splitlines()
            for line in all_lines[-lines:]:
                on_line(line)

            if not follow:
                return

            position = f.tell()
            buffer = ""

        while not stop_event.is_set():
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(position)
                new_data = f.read()
                position = f.tell()

            if new_data:
                buffer += new_data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    on_line(line)

            time_mod.sleep(poll_interval)

        # Final read after stop_event is set
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(position)
            new_data = f.read()

        if new_data:
            buffer += new_data
            for line in buffer.splitlines():
                on_line(line)
