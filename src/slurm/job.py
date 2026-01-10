import logging
import time
import pickle
import os
import threading

from typing import Optional, Any, Dict, TYPE_CHECKING, TypeVar, Generic

# Import backend types needed for isinstance checks
from .api.ssh import SSHCommandBackend

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from .cluster import Cluster

from .errors import BackendError

from .rendering import RESULT_FILENAME


logger = logging.getLogger(__name__)

# Type variable for Job's generic return type
T = TypeVar("T")


class Job(Generic[T]):
    """Represents a submitted Slurm job, providing status tracking and result retrieval.

    A Job instance is returned when you submit a task via `cluster.submit()`. It provides
    methods to monitor the job's progress, wait for completion, and retrieve the return
    value of your task function.

    Jobs track their own metadata (working directory, output paths) to enable result
    retrieval even after the scheduler has purged the job from its queue.

    Job State Machine:

    ```mermaid
    stateDiagram-v2
        [*] --> PENDING: submit()
        PENDING --> RUNNING: SLURM schedules
        RUNNING --> COMPLETED: Success
        RUNNING --> FAILED: Error/Exception
        RUNNING --> TIMEOUT: Time limit
        RUNNING --> CANCELLED: cancel()
        RUNNING --> NODE_FAIL: Node failure
        PENDING --> CANCELLED: cancel()

        COMPLETED --> [*]
        FAILED --> [*]
        TIMEOUT --> [*]
        CANCELLED --> [*]
        NODE_FAIL --> [*]
    ```

    Terminal states: COMPLETED, FAILED, CANCELLED, TIMEOUT, NODE_FAIL

    Examples:
        Basic job lifecycle:

            >>> job = submitter(arg1, arg2)
            >>> print(f"Job ID: {job.id}")
            >>> job.wait()
            >>> result = job.get_result()

        Check status without blocking:

            >>> if job.is_completed():
            ...     if job.is_successful():
            ...         result = job.get_result()
            ...     else:
            ...         print(f"Job failed: {job.get_status()}")
            ... elif job.is_running():
            ...     print("Still running...")

        Wait with timeout:

            >>> success = job.wait(timeout=300)  # 5 minutes
            >>> if not success:
            ...     print("Timeout! Job still running.")
            ...     job.cancel()

    Attributes:
        id: The Slurm job ID (string).
        cluster: The Cluster instance this job was submitted to.
        target_job_dir: Working directory on the cluster containing job artifacts.
        pre_submission_id: SDK-generated unique ID for this job submission.
    """

    TERMINAL_STATES = {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "NODE_FAIL",
    }

    def __init__(
        self,
        id: str,
        cluster: Optional["Cluster"] = None,
        task_func=None,
        args=(),
        kwargs=None,
        target_job_dir: Optional[str] = None,
        pre_submission_id: Optional[str] = None,
        sbatch_options: Optional[Dict[str, Any]] = None,
        stdout_path: Optional[str] = None,
        stderr_path: Optional[str] = None,
    ):
        """
        Initialize a Job object.

        Args:
            id: The Slurm job ID
            cluster: The cluster the job is running on
            task_func: The task function that was submitted
            args: The positional arguments passed to the task function
            kwargs: The keyword arguments passed to the task function
            target_job_dir: The absolute path to the directory containing this job's files.
            pre_submission_id: The unique ID used when creating the directory and filenames.
            sbatch_options: The SBATCH options used for this job's submission.
            stdout_path: Scheduler stdout path for this job, if known.
            stderr_path: Scheduler stderr path for this job, if known.

        Raises:
            ValueError: If id is not a non-empty string or if cluster is None.
        """
        if not id or not isinstance(id, str) or not id.strip():
            raise ValueError(
                f"job id must be a non-empty string, got {type(id).__name__}: {id!r}"
            )

        self.id = id
        logger.debug("[%s] Initializing Job", self.id)
        if cluster is None:
            raise ValueError(
                "Cluster instance is required. The global cluster pattern has been removed. "
                "Pass the cluster explicitly or use the Job returned from cluster.submit()."
            )
        self.cluster = cluster
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs or {}

        self.target_job_dir = target_job_dir
        self.pre_submission_id = pre_submission_id
        self.sbatch_options = dict(sbatch_options or {})
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self._completed_context_emitted = False
        self._completed_context_lock = threading.Lock()
        if not self.target_job_dir or not self.pre_submission_id:
            logger.warning(
                "[%s] Job initialized without complete metadata. "
                "Result retrieval may fail. This typically occurs when reconstructing "
                "jobs via get_job(); Cluster.get_job() attempts to hydrate metadata when possible.",
                self.id,
            )

        self._status_cache = None
        self._status_cache_time = 0
        self._completed = False

        self._result_filename = (
            f"slurm_job_{self.pre_submission_id}_{RESULT_FILENAME}"
            if self.pre_submission_id
            else None
        )

    def _update_status_cache(
        self,
        status: Dict[str, Any],
        timestamp: Optional[float] = None,
    ) -> None:
        """Update cached status metadata and derived telemetry."""

        self._status_cache = status
        self._status_cache_time = timestamp or time.time()
        self._update_status_telemetry(status, self._status_cache_time)
        if status.get("JobState") in self.TERMINAL_STATES:
            self._completed = True

    def _update_status_telemetry(
        self, status: Dict[str, Any], timestamp: float
    ) -> None:
        state = status.get("JobState")
        if state == "RUNNING" and self.started_at is None:
            self.started_at = timestamp
        if state in self.TERMINAL_STATES:
            self.finished_at = self.finished_at or timestamp

    @property
    def result_path(self) -> Optional[str]:
        if not self.target_job_dir or not self._result_filename:
            return None
        return os.path.join(self.target_job_dir, self._result_filename)

    def get_status(self) -> Dict[str, Any]:
        """Query the current status of the job from the Slurm scheduler.

        This method caches the status for 1 second to avoid excessive scheduler queries.
        Common job states include: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, TIMEOUT.

        Returns:
            Dictionary with job status and metadata. Always includes "JobState" key.
            Common keys:
            - "JobState": Current state string (e.g., "RUNNING", "COMPLETED")
            - "ExitCode": Exit code string (e.g., "0:0" for success)
            - "WorkDir": Job working directory
            - "Error": Error message if query failed

            Returns {"JobState": "UNKNOWN", "Error": "..."} if the status cannot
            be retrieved.

        Examples:
            >>> status = job.get_status()
            >>> print(status["JobState"])
            RUNNING
            >>> if status.get("ExitCode", "").startswith("0:0"):
            ...     print("Job succeeded")
        """
        current_time = time.time()
        if self._status_cache and current_time - self._status_cache_time < 1:
            return self._status_cache

        try:
            status = self.cluster.backend.get_job_status(self.id)
            timestamp = time.time()
            self._update_status_cache(status, timestamp)

            if status.get("JobState") in self.TERMINAL_STATES and hasattr(
                self.cluster, "_emit_completed_context"
            ):
                try:
                    self.cluster._emit_completed_context(self, status, timestamp)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.debug(
                        "Error emitting completed context for job %s: %s",
                        self.id,
                        exc,
                    )

            return status
        except Exception as e:
            logger.error("Error getting job status for job %s: %s", self.id, e)
            raise BackendError(
                f"Failed to get status for job {self.id}.\n\n"
                f"Error: {e}\n\n"
                "This error usually indicates a communication issue with the SLURM cluster.\n\n"
                "Possible causes:\n"
                "  1. Network or SSH connection was lost\n"
                "  2. SLURM controller is down\n"
                "  3. Job information is temporarily unavailable\n\n"
                "To diagnose:\n"
                "  1. Check if you can still reach the cluster\n"
                "  2. Try: cluster.backend.get_job_status('{job_id}') to see backend error\n"
                "  3. Check SLURM directly: squeue -j {job_id} or sacct -j {job_id}".format(
                    job_id=self.id
                )
            ) from e

    def is_running(self) -> bool:
        """Check if the job is currently executing.

        Returns:
            True if the job state is "RUNNING", False otherwise (including
            PENDING, COMPLETED, FAILED, etc.).
        """
        status = self.get_status()
        return status.get("JobState") == "RUNNING"

    def is_completed(self) -> bool:
        """Check if the job has reached a terminal state.

        Terminal states include: COMPLETED, FAILED, CANCELLED, TIMEOUT, NODE_FAIL.
        A completed job is no longer running and will not transition to another state.

        Returns:
            True if the job is in a terminal state, False if still pending or running.
        """
        if self._completed:
            return True

        status = self.get_status()

        if status.get("JobState") in self.TERMINAL_STATES:
            self._completed = True
            return True

        return False

    def is_successful(self) -> bool:
        """Check if the job completed with a successful exit code.

        A job is successful if its state is COMPLETED and its exit code is 0:0
        (no errors). Jobs that failed, were cancelled, or timed out return False.

        Returns:
            True if the job completed successfully with exit code 0:0, False otherwise.

        Note:
            This does not automatically wait for completion. If the job is still
            running, this returns False. Use `job.wait()` first to ensure completion.
        """
        status = self.get_status()
        return status.get("JobState") == "COMPLETED" and status.get(
            "ExitCode", ""
        ).startswith("0:0")

    def _get_job_specific_output_path(self) -> Optional[str]:
        """Gets the absolute path to the job-specific output directory.

        Returns None if metadata is missing, which can happen when the Job
        is reconstructed via Cluster.get_job() and hydration fails.
        """
        if not hasattr(self, "target_job_dir") or not self.target_job_dir:
            logger.error(
                "Cannot determine job output path for %s. target_job_dir is missing.",
                self.id,
            )
            return None
        return self.target_job_dir

    @staticmethod
    def _tail_text(text: str, *, max_lines: int = 200, max_chars: int = 10_000) -> str:
        """Return a tail slice of the given text for error messages."""
        if not text:
            return ""

        original_len = len(text)
        truncated_chars = False
        if original_len > max_chars:
            text = text[-max_chars:]
            truncated_chars = True

        lines = text.splitlines()
        truncated_lines = False
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
            truncated_lines = True

        rendered = "\n".join(lines)
        if truncated_chars or truncated_lines:
            note_parts = []
            if truncated_chars:
                note_parts.append(f"chars>{max_chars}")
            if truncated_lines:
                note_parts.append(f"lines>{max_lines}")
            note = ", ".join(note_parts)
            return f"[... output truncated ({note}) ...]\n{rendered}"
        return rendered

    def _format_failure_diagnostics(self) -> str:
        parts: list[str] = []

        if self.target_job_dir:
            parts.append(f"Check job output in: {self.target_job_dir}")

        if self.stdout_path:
            try:
                stdout = self.get_stdout()
                stdout_tail = self._tail_text(stdout)
                parts.append(
                    "--- Remote Stdout (tail) ---\n" + (stdout_tail or "[empty]")
                )
            except Exception as exc:
                parts.append(f"--- Remote Stdout Unavailable: {exc} ---")

        if self.stderr_path:
            try:
                stderr = self.get_stderr()
                stderr_tail = self._tail_text(stderr)
                parts.append(
                    "--- Remote Stderr (tail) ---\n" + (stderr_tail or "[empty]")
                )
            except Exception as exc:
                parts.append(f"--- Remote Stderr Unavailable: {exc} ---")

        return "\n".join(parts)

    def get_result(self, timeout: Optional[float] = None) -> T:
        """Retrieve the return value from the completed job.

        This method waits for job completion if necessary, then downloads and
        deserializes the pickled result file from the cluster.

        The result must be pickle-serializable. Complex objects like open file
        handles, database connections, or objects with custom serialization
        requirements may not work.

        Args:
            timeout: Optional timeout in seconds to wait for job completion.
                If None (default), waits indefinitely. If the timeout is exceeded,
                raises DownloadError.

        Returns:
            The object returned by your task function, deserialized from the
            remote result file. Type matches your function's return type annotation.

        Raises:
            DownloadError: If the job did not complete successfully, or if the
                result file cannot be found or downloaded. Common causes:
                - Job failed (non-zero exit code)
                - Job timed out while waiting for completion
                - Job metadata missing (when using `get_job()`)
                - Network issues during download
                - Unpickling errors (incompatible Python versions, missing dependencies)

        Examples:
            >>> job = submitter(x=42)
            >>> result = job.get_result()  # Blocks until complete

            With timeout:

            >>> try:
            ...     result = job.get_result(timeout=300)  # 5 minutes max
            ... except DownloadError as e:
            ...     print(f"Failed or timed out: {e}")

            Handle potential failures:

            >>> try:
            ...     result = job.get_result()
            ... except DownloadError as e:
            ...     print(f"Failed: {e}")
            ...     # Check job output files for debugging

        Note:
            For SSH backends, this downloads the result file to a temporary location
            then loads it. For local backends, it reads the file directly.
        """
        if not self.is_completed():
            self.wait(timeout=timeout)
            if not self.is_completed():
                from .errors import DownloadError

                status = self.get_status()
                state = status.get("JobState")
                diagnostics = self._format_failure_diagnostics()
                message = (
                    f"Job {self.id} did not reach a terminal state within timeout.\n"
                    f"State: {state}\n"
                )
                if diagnostics:
                    message += diagnostics
                raise DownloadError(message)

        status = self.get_status()
        if not self.is_successful():
            from .errors import DownloadError

            diagnostics = self._format_failure_diagnostics()
            reason = status.get("Reason")
            raise DownloadError(
                f"Job {self.id} did not succeed.\n"
                f"State: {status.get('JobState')}\n"
                f"Exit Code: {status.get('ExitCode')}\n"
                + (f"Reason: {reason}\n" if reason else "")
                + (diagnostics if diagnostics else "")
            )

        try:
            job_specific_dir = self._get_job_specific_output_path()
            if not job_specific_dir:
                raise RuntimeError(
                    f"Could not determine job directory path for job {self.id}"
                )

            if not self._result_filename:
                raise RuntimeError(
                    f"Could not determine result filename for job {self.id}.\n"
                    f"Missing pre_submission_id. This job may have been created via get_job() "
                    f"which cannot always recover all metadata. "
                    f"For full result access, use the Job object returned from submit()."
                )

            result_file_path = os.path.join(job_specific_dir, self._result_filename)
            logger.debug("[%s] Expecting result file at: %s", self.id, result_file_path)

        except Exception as e:
            raise RuntimeError(
                f"Failed to determine result file path for job {self.id}.\n"
                f"Error: {e}\n\n"
                "This usually means:\n"
                "  1. The job metadata is incomplete or corrupted\n"
                "  2. The job directory structure is unexpected\n"
                "  3. The result file naming convention has changed\n\n"
                "Ensure the job completed successfully before calling get_result()."
            ) from e

        if isinstance(self.cluster.backend, SSHCommandBackend):
            import tempfile

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                local_temp_path = temp_file.name

            try:
                logger.debug(
                    "[%s] Downloading remote result %s to %s",
                    self.id,
                    result_file_path,
                    local_temp_path,
                )
                self.cluster.backend.download_file(result_file_path, local_temp_path)

                with open(local_temp_path, "rb") as f:
                    return pickle.load(f)
            except FileNotFoundError as e:
                logger.error(
                    "[%s] Remote result file not found: %s", self.id, result_file_path
                )
                from .errors import DownloadError

                raise DownloadError(
                    f"Failed to download job result: File not found on remote cluster.\n\n"
                    f"Job ID: {self.id}\n"
                    f"Expected result file: {result_file_path}\n\n"
                    "This usually means:\n"
                    "  1. Job hasn't finished writing its result yet (still running)\n"
                    "  2. Job failed before writing result file\n"
                    "  3. Result file was deleted or moved\n"
                    "  4. Job directory path is incorrect\n\n"
                    "To diagnose:\n"
                    "  1. Check job status: job.get_status() or squeue/sacct -j {job_id}\n"
                    "  2. Check job output/error logs in: {job_dir}\n"
                    "  3. Verify job completed successfully: job.is_successful()\n"
                    "  4. SSH to cluster and check: ls -la {result_file}".format(
                        job_id=self.id,
                        job_dir=self.target_job_dir
                        if self.target_job_dir
                        else "job directory",
                        result_file=result_file_path,
                    )
                ) from e
            except Exception as e:
                logger.error(
                    "[%s] Error downloading or loading result from %s: %s",
                    self.id,
                    result_file_path,
                    e,
                )
                logger.debug("Exception detail:", exc_info=True)
                from .errors import DownloadError

                raise DownloadError(
                    f"Failed to download or load job result.\n\n"
                    f"Job ID: {self.id}\n"
                    f"Result file: {result_file_path}\n"
                    f"Error: {e}\n\n"
                    "Possible causes:\n"
                    "  1. Network error during download (SSH connection lost)\n"
                    "  2. Result file is corrupted\n"
                    "  3. Pickle deserialization failed (incompatible Python versions)\n"
                    "  4. Permission issues accessing the file\n\n"
                    "To diagnose:\n"
                    "  1. Check SSH connection: ssh {hostname} echo 'connected'\n"
                    "  2. Verify file exists on cluster: ssh {hostname} ls -la {result_file}\n"
                    "  3. Check file size: ssh {hostname} du -h {result_file}\n"
                    "  4. Check Python versions match between local and cluster\n"
                    "  5. Review job output logs for errors: {job_dir}/*.out".format(
                        hostname=getattr(self.cluster.backend, "hostname", "cluster"),
                        result_file=result_file_path,
                        job_dir=self.target_job_dir
                        if self.target_job_dir
                        else "job directory",
                    )
                ) from e
        else:
            try:
                local_result_path = result_file_path
                if not os.path.isabs(local_result_path):
                    local_result_path = os.path.abspath(local_result_path)

                logger.debug("[%s] Loading local result %s", self.id, local_result_path)
                with open(local_result_path, "rb") as f:
                    return pickle.load(f)
            except FileNotFoundError as e:
                logger.error(
                    "[%s] Local result file not found: %s", self.id, local_result_path
                )
                from .errors import DownloadError

                raise DownloadError(
                    f"Failed to load job result: File not found locally.\n\n"
                    f"Job ID: {self.id}\n"
                    f"Expected file: {local_result_path}\n\n"
                    "This usually means:\n"
                    "  1. Job hasn't finished yet (still running or pending)\n"
                    "  2. Job failed before writing result\n"
                    "  3. Result file was deleted\n"
                    "  4. Job directory path is wrong\n\n"
                    "To diagnose:\n"
                    "  1. Check job status: job.get_status()\n"
                    "  2. Verify job completed: job.is_successful()\n"
                    "  3. Check files in job directory: ls -la {job_dir}\n"
                    "  4. Review job logs: cat {job_dir}/*.out {job_dir}/*.err".format(
                        job_dir=self.target_job_dir
                        if self.target_job_dir
                        else "job directory"
                    )
                ) from e
            except Exception as e:
                logger.error(
                    "[%s] Error loading local result from %s: %s",
                    self.id,
                    local_result_path,
                    e,
                )
                logger.debug("Exception detail:", exc_info=True)
                from .errors import DownloadError

                raise DownloadError(
                    f"Failed to load or deserialize local result from {local_result_path}: {e}"
                ) from e

    def wait(self, timeout: Optional[float] = None, poll_interval: float = 5.0) -> bool:
        """Block until the job reaches a terminal state (completed, failed, cancelled, etc.).

        This method polls the scheduler at regular intervals until the job finishes
        or the timeout is reached. It's useful when you need to wait for a job before
        proceeding with subsequent tasks.

        Args:
            timeout: Maximum time to wait in seconds. If None (default), waits indefinitely
                until the job completes. Use a timeout to prevent indefinite blocking if
                the job hangs.
            poll_interval: Seconds between status checks. Default is 5 seconds. Lower
                values provide faster response but increase scheduler load. Higher values
                reduce load but delay detection of completion.

        Returns:
            True if the job completed successfully (state COMPLETED, exit code 0:0).
            False if the job failed, was cancelled, timed out, or the wait timeout
            was exceeded.

        Examples:
            Wait indefinitely for success:

                >>> if job.wait():
                ...     result = job.get_result()
                ... else:
                ...     print("Job failed!")

            Wait with timeout:

                >>> if job.wait(timeout=300, poll_interval=10):
                ...     result = job.get_result()
                ... else:
                ...     print("Timeout or failure")
                ...     job.cancel()  # Clean up

        Note:
            This method checks `is_completed()` and `is_successful()` at each interval.
            It does not check for result file existence - only job state from the scheduler.
        """
        start_time = time.time()

        while True:
            if self.is_completed():
                success = self.is_successful()
                logger.debug(
                    "[%s] Completed with status: %s",
                    self.id,
                    self.get_status().get("JobState"),
                )
                logger.debug(
                    "[%s] Exit code: %s", self.id, self.get_status().get("ExitCode")
                )

                return success

            if timeout is not None and time.time() - start_time > timeout:
                logger.error("[%s] Timeout waiting for job", self.id)
                return False

            time.sleep(poll_interval)

    def cancel(self) -> bool:
        """Cancel the job via `scancel`.

        Attempts to cancel the job if it's pending or running. Has no effect if
        the job has already completed.

        Returns:
            True if the cancellation command succeeded, False if it failed.
            Note that True only indicates the command was issued, not that the
            job was necessarily cancelled (it may have already completed).

        Examples:
            >>> if not job.wait(timeout=60):
            ...     print("Timeout! Cancelling...")
            ...     job.cancel()
        """
        try:
            return self.cluster.backend.cancel_job(self.id)
        except Exception as e:
            logger.error("Error cancelling job: %s", e)
            return False

    def get_stdout(self) -> str:
        """Retrieve the standard output from the job.

        This is a debug helper that reads the scheduler's stdout file for this job.
        Useful for debugging job failures or inspecting output without waiting for
        completion.

        Returns:
            The contents of the job's stdout file as a string.

        Raises:
            FileNotFoundError: If the stdout file doesn't exist yet (job may not have started).
            RuntimeError: If stdout_path metadata is missing.

        Examples:
            >>> try:
            ...     output = job.get_stdout()
            ...     print(output)
            ... except FileNotFoundError:
            ...     print("Job hasn't produced output yet")

        Note:
            For jobs that are still running, this returns the output written so far.
            The file may be incomplete until the job completes.
        """
        if not self.stdout_path:
            raise RuntimeError(
                f"Cannot retrieve stdout for job {self.id}: stdout_path is not set.\n"
                f"This typically occurs when reconstructing jobs via get_job(). "
                f"For full debugging access, use the Job object returned from submit()."
            )

        return self._read_remote_file(self.stdout_path, "stdout")

    def get_stderr(self) -> str:
        """Retrieve the standard error output from the job.

        This is a debug helper that reads the scheduler's stderr file for this job.
        Useful for debugging job failures or inspecting error messages without waiting
        for completion.

        Returns:
            The contents of the job's stderr file as a string.

        Raises:
            FileNotFoundError: If the stderr file doesn't exist yet (job may not have started).
            RuntimeError: If stderr_path metadata is missing.

        Examples:
            >>> try:
            ...     errors = job.get_stderr()
            ...     if errors:
            ...         print(f"Job errors: {errors}")
            ... except FileNotFoundError:
            ...     print("Job hasn't produced errors yet")

        Note:
            For jobs that are still running, this returns the errors written so far.
            The file may be incomplete until the job completes.
        """
        if not self.stderr_path:
            raise RuntimeError(
                f"Cannot retrieve stderr for job {self.id}: stderr_path is not set.\n"
                f"This typically occurs when reconstructing jobs via get_job(). "
                f"For full debugging access, use the Job object returned from submit()."
            )

        return self._read_remote_file(self.stderr_path, "stderr")

    def get_script(self) -> str:
        """Retrieve the generated SLURM batch script for this job.

        This is a debug helper that reads the actual sbatch script that was submitted.
        Useful for understanding exactly what commands are being executed and debugging
        environment or packaging issues.

        Returns:
            The contents of the job's batch script as a string.

        Raises:
            FileNotFoundError: If the script file doesn't exist.
            RuntimeError: If job metadata is missing.

        Examples:
            >>> script = job.get_script()
            >>> print(script)
            #!/bin/bash
            #SBATCH --job-name=my_task
            ...

        Note:
            The script includes all SBATCH directives, setup commands, the task execution,
            and cleanup commands.
        """
        if not self.target_job_dir or not self.pre_submission_id:
            raise RuntimeError(
                f"Cannot retrieve script for job {self.id}: job metadata is incomplete.\n"
                f"This typically occurs when reconstructing jobs via get_job(). "
                f"For full debugging access, use the Job object returned from submit()."
            )

        script_filename = f"slurm_job_{self.pre_submission_id}_script.sh"
        script_path = os.path.join(self.target_job_dir, script_filename)

        return self._read_remote_file(script_path, "script")

    def _read_remote_file(self, remote_path: str, file_type: str) -> str:
        """Read a file from the cluster (SSH or local).

        Args:
            remote_path: Absolute path to the file on the cluster.
            file_type: Description of file type for error messages (e.g., "stdout", "stderr", "script").

        Returns:
            File contents as a string.

        Raises:
            FileNotFoundError: If the file doesn't exist.
        """
        if isinstance(self.cluster.backend, SSHCommandBackend):
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w+", delete=False, suffix=f"_{file_type}"
            ) as temp_file:
                local_temp_path = temp_file.name

            try:
                logger.debug(
                    "[%s] Downloading %s from %s to %s",
                    self.id,
                    file_type,
                    remote_path,
                    local_temp_path,
                )
                self.cluster.backend.download_file(remote_path, local_temp_path)

                with open(local_temp_path, "r") as f:
                    return f.read()
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f"File not found on remote cluster: {remote_path}\n"
                    f"Job ID: {self.id}\n\n"
                    "This usually means:\n"
                    "  1. Job hasn't started yet (file not created)\n"
                    "  2. Job directory was cleaned up\n"
                    "  3. Path is incorrect\n\n"
                    f"Job directory: {self.target_job_dir}"
                ) from e
            finally:
                if os.path.exists(local_temp_path):
                    os.unlink(local_temp_path)
        else:
            # Local backend - read file directly
            try:
                local_path = remote_path
                if not os.path.isabs(local_path):
                    local_path = os.path.abspath(local_path)

                logger.debug(
                    "[%s] Reading local %s from %s", self.id, file_type, local_path
                )
                with open(local_path, "r") as f:
                    return f.read()
            except FileNotFoundError as e:
                raise FileNotFoundError(
                    f"File not found locally: {local_path}\n"
                    f"Job ID: {self.id}\n\n"
                    "This usually means:\n"
                    "  1. Job hasn't started yet (file not created)\n"
                    "  2. Job directory was cleaned up\n"
                    "  3. Path is incorrect\n\n"
                    f"Job directory: {self.target_job_dir}"
                ) from e
