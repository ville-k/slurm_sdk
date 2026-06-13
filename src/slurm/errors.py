"""Custom error types for the slurm SDK."""

from __future__ import annotations

from typing import Any, Dict, Optional


class PackagingError(Exception):
    """Raised when packaging (build/upload/install) fails.

    Packaging is the process of preparing your code to run on the cluster. This can
    involve building a Python wheel, creating/pulling a container image, or uploading
    artifacts.

    Common causes:
        - Missing pyproject.toml for wheel packaging
        - No container runtime (docker/podman) available for container packaging
        - Container image not found or registry authentication issues
        - Network failures during wheel upload to cluster
        - Invalid packaging configuration (unknown type, missing required keys)
        - Build tool (uv, pip, docker) not installed or not working

    What to check:
        - Verify your packaging configuration in the @task decorator or Slurmfile
        - For wheel packaging: ensure pyproject.toml exists in a parent directory
        - For container packaging: verify docker/podman is installed and the
          image is accessible
        - Check network connectivity for uploads
        - Review the full error message for specific tool output (build failures, etc.)

    Examples:
        Typical wheel packaging error:

            >>> @task(time="01:00:00", packaging={"type": "wheel"})
            ... def my_task():
            ...     pass
            >>> submitter = cluster.submit(my_task)
            >>> job = submitter()  # May raise PackagingError if no pyproject.toml

        Container image not found:

            >>> @task(packaging={"type": "container", "image": "nonexistent:latest"})
            ... def my_task():
            ...     pass
            >>> submitter = cluster.submit(my_task)  # May raise PackagingError
    """


class SubmissionError(Exception):
    """Raised when job submission to the Slurm scheduler fails.

    This error occurs after successful packaging but during the actual sbatch
    submission process. It indicates the scheduler rejected the job or the
    submission command failed.

    Common causes:
        - Invalid SBATCH parameters (unknown partition, invalid time format, etc.)
        - Account/partition permission denied
        - Requested resources exceed partition limits
        - Network issues preventing SSH communication
        - Scheduler temporarily unavailable or overloaded
        - Malformed job script (SDK bug - please report)

    What to check:
        - Verify partition and account names are correct
        - Check resource requests don't exceed partition limits (time, CPUs, memory)
        - Ensure you have access to the requested partition
        - Verify cluster connectivity (SSH connection still active)
        - Check cluster status (is the scheduler running?)

    Attributes:
        message: Error description
        script: The rendered job script that failed to submit (if available)
        metadata: Dictionary with submission context (backend, account, partition, etc.)

    Examples:
        Invalid partition:

            >>> submitter = cluster.submit(task, partition="nonexistent")
            >>> job = submitter()  # Raises SubmissionError

        Time limit exceeds partition maximum:

            >>> submitter = cluster.submit(task, time="999:00:00")  # Too long
            >>> job = submitter()  # Raises SubmissionError
    """

    def __init__(
        self,
        message: str,
        *,
        script: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.script = script
        self.metadata = metadata or {}
        self._syntax: Optional[Any] = self._build_syntax(script)

    @staticmethod
    def _build_syntax(script: Optional[str]) -> Optional[Any]:
        if not script:
            return None
        try:
            from rich.syntax import Syntax

            return Syntax(script, "bash", theme="monokai", line_numbers=True)
        except Exception:
            return None

    def __str__(self) -> str:  # pragma: no cover - exercised via runtime failures
        parts = [self.message]

        if self.metadata:
            formatted = ", ".join(
                f"{key}={value}" for key, value in self.metadata.items()
            )
            parts.append(f"metadata: {formatted}")

        if self.script:
            parts.append("rendered sbatch script:\n" + self.script)

        return "\n\n".join(parts)

    def __rich_console__(self, console, options):  # pragma: no cover
        yield self.message
        if self.metadata:
            formatted = ", ".join(
                f"{key}={value}" for key, value in self.metadata.items()
            )
            yield f"metadata: {formatted}"
        if self._syntax is not None:
            yield "rendered sbatch script:"
            yield self._syntax
        elif self.script:
            yield "rendered sbatch script:"
            yield self.script


class DownloadError(Exception):
    """Raised when retrieving job results or artifacts fails.

    This error occurs when attempting to download and deserialize the result file
    after a job completes. It's distinct from job execution failures.

    Common causes:
        - Job failed/was cancelled (non-zero exit code) - no result file created
        - Job metadata missing (using `get_job()` instead of the returned Job object)
        - Result file not found on cluster (job may not have written it)
        - Network errors during file download (SSH connection issues)
        - Pickle deserialization failures (version mismatches, missing dependencies)
        - Insufficient permissions to read the result file

    What to check:
        - Verify the job completed successfully: `job.is_successful()`
        - Check job output/error files for task function exceptions
        - Ensure your task function returns a pickle-serializable object
        - Verify Python versions match between client and cluster
        - For `get_job()` usage: use the Job object returned from `submit()` instead

    Examples:
        Job failed, no result available:

            >>> job = submitter(invalid_args)
            >>> job.wait()
            >>> result = job.get_result()  # Raises DownloadError

        Unpicklable return value:

            >>> @task(time="00:10:00")
            ... def bad_task():
            ...     return open("file.txt")  # File handles can't be pickled
            >>> job = submitter()
            >>> result = job.get_result()  # Raises DownloadError
    """


class BackendError(Exception):
    """Base class for errors originating from cluster communication backends.

    Subclasses represent specific backend failure modes (timeouts, command errors).
    """


class BackendTimeout(BackendError, TimeoutError):
    """Raised when a backend command times out.

    Backend commands (SSH execution, squeue queries, etc.) have configurable
    timeouts. This error indicates the command didn't complete within the limit.

    Common causes:
        - Cluster unresponsive or overloaded
        - Network latency or connectivity issues
        - Command genuinely taking too long (e.g., large file transfer)

    What to check:
        - Verify cluster is responsive
        - Check network connectivity
        - Consider increasing timeout parameters if the operation is legitimately slow
    """


class BackendCommandError(BackendError):
    """Raised when a backend command fails to execute.

    The command was issued but returned a non-zero exit code or encountered
    an execution error (not a timeout).

    Common causes:
        - SSH authentication failures
        - Command not found on remote system
        - Permission denied errors
        - Cluster-side software issues

    What to check:
        - Verify SSH credentials and connectivity
        - Ensure required commands (sbatch, squeue, etc.) are available on cluster
        - Check file permissions on cluster
        - Review the error message for specific command output
    """


class SlurmfileError(Exception):
    """Base class for Slurmfile configuration errors.

    Slurmfiles are TOML configuration files that define cluster connections
    and environment-specific settings. These errors indicate problems with
    the Slurmfile itself.
    """


class SlurmfileNotFoundError(SlurmfileError):
    """Raised when a Slurmfile cannot be located.

    The SDK searches for Slurmfile, Slurmfile.toml, slurmfile, or slurmfile.toml
    in the current directory and parent directories.

    Common causes:
        - No Slurmfile exists in the project
        - Running from a directory outside the project
        - Typo in explicit Slurmfile path

    What to check:
        - Ensure a Slurmfile exists in your project
        - Run from within the project directory
        - Set the SLURMFILE environment variable to specify an explicit path
        - Use `Cluster()` constructor directly if you don't want to use a Slurmfile
    """


class SlurmfileInvalidError(SlurmfileError):
    """Raised when a Slurmfile contains invalid TOML syntax or schema.

    The Slurmfile was found but couldn't be parsed or doesn't match the
    expected structure.

    Common causes:
        - TOML syntax errors (unclosed brackets, invalid escaping, etc.)
        - Missing required sections (e.g., [cluster])
        - Invalid data types (string where dict expected, etc.)
        - Malformed callback specifications

    What to check:
        - Validate TOML syntax (use a TOML linter)
        - Ensure required sections exist: [cluster] with 'backend' key
        - Review example Slurmfiles in the SDK documentation
        - Check the error message for specific line numbers or keys
    """


class SlurmfileEnvironmentNotFoundError(SlurmfileError):
    """Raised when a requested environment is missing from the Slurmfile.

    You specified an environment name that doesn't exist in the [environments]
    section of the Slurmfile.

    Common causes:
        - Typo in environment name
        - Environment not defined in Slurmfile
        - Using wrong Slurmfile

    What to check:
        - Verify the environment name exists in your Slurmfile
        - Check the SLURM_ENV environment variable (it may override your code)
        - List available environments: open the Slurmfile and check [environments]

    Examples:
        >>> cluster = Cluster.from_env("producton")  # Typo!
        SlurmfileEnvironmentNotFoundError: Environment 'producton' not defined in Slurmfile
    """


class TopologyError(Exception):
    """Raised when a ``parallel(...)`` submission has an invalid topology.

    Topology errors surface before the job is submitted. They indicate
    declarative problems — pool / peer / resource conflicts — that the SDK
    can detect at spec-validation time. Users see a single aggregated error
    listing every problem found, so a 24-hour RL job never fails after 30
    seconds because a pool was mis-sized.

    Common causes:
        - Peer references a pool name that does not exist in ``Topology.pools``
        - Sum of peer resource claims exceeds a pool's per-node capacity
        - Two peer names collide
        - ``leader=True`` combined with ``on_failure="continue"``
        - ``Peer.replicas(args=...)`` length does not match ``count``

    What to check:
        - The error message lists each problem with the offending peer/pool name
          and, where possible, a concrete suggestion
        - Inspect ``Topology.pools`` and each ``Peer``'s ``pool=`` reference
        - For capacity overflow, either shrink the peer's resource claim,
          grow the pool, or move the peer to a larger pool
    """

    def __init__(
        self,
        message: str,
        *,
        problems: Optional[list[str]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.problems = list(problems) if problems is not None else []


class PeerFailureError(Exception):
    """Raised to describe a single peer's fatal failure in a ``parallel()`` job.

    Used by the supervisor and ``ParallelJob`` to report which peer's exit was
    propagated upward. Never raised directly at the top level — always wrapped
    in a :class:`CompositeJobError` via ``ParallelJob.get_results()`` when one
    or more fatal peers died.

    Attributes:
        peer_name: Name of the peer that failed.
        replica_index: Replica index (0-based) for replica sets, else ``None``.
        exit_code: Process exit code reported by the supervisor.
    """

    def __init__(
        self,
        peer_name: str,
        replica_index: Optional[int],
        exit_code: int,
        message: str,
    ) -> None:
        super().__init__(message)
        self.peer_name = peer_name
        self.replica_index = replica_index
        self.exit_code = exit_code

    def __str__(self) -> str:
        suffix = f"[{self.replica_index}]" if self.replica_index is not None else ""
        return (
            f"peer {self.peer_name!r}{suffix} failed with exit code "
            f"{self.exit_code}: {self.args[0] if self.args else ''}"
        )


class CompositeJobError(Exception):
    """Raised by :meth:`ParallelJob.get_results` when fatal peer(s) died.

    "Fatal" means the peer's configured ``on_failure`` resolved to ``"kill"``
    — either directly or after a restart budget was exhausted. Peers with
    ``on_failure="continue"`` do **not** contribute to this error; they surface
    via ``ParallelJob.peer_outcomes()`` as tolerated failures and leave a
    ``None`` in the results dict.

    Attributes:
        failures: List of :class:`PeerFailureError` describing each fatal peer.
    """

    def __init__(self, failures: "list[PeerFailureError]") -> None:
        lines = [f"  - {failure}" for failure in failures]
        super().__init__(
            f"parallel job had {len(failures)} fatal peer failure(s):\n"
            + "\n".join(lines)
        )
        self.failures = list(failures)
