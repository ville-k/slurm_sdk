"""
This module provides the Cluster class for submitting and managing jobs on SLURM clusters.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple, Union
import sys
import traceback
import uuid
import logging
import os
import re
import time
import threading
import json
from datetime import datetime

from .job import Job
from .api import create_backend
from .callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
    PackagingBeginContext,
    PackagingEndContext,
    SubmitBeginContext,
    SubmitEndContext,
    WorkflowTaskSubmitContext,
)
from .task import SlurmTask, normalize_sbatch_options
from .packaging import get_packaging_strategy
from .config import load_environment
from .errors import (
    PackagingError,
    SubmissionError,
    SlurmfileInvalidError,
)


logger = logging.getLogger(__name__)


class _JobStatusPoller(threading.Thread):
    """Background thread that emits JobStatusUpdated callbacks."""

    def __init__(
        self,
        cluster: "Cluster",
        job: "Job",
        subscriptions: List[Tuple[BaseCallback, float]],
    ) -> None:
        super().__init__(
            daemon=True,
            name=f"slurm-job-poller-{job.id}",
        )
        self.cluster = cluster
        self.job = job
        self.subscriptions = subscriptions
        self._stop = threading.Event()
        self._last_emit: Dict[BaseCallback, float] = {}
        self._previous_state: Optional[str] = None
        self._interval = min((interval for _, interval in subscriptions), default=5.0)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:  # pragma: no cover - background thread
        try:
            while not self._stop.is_set():
                timestamp = time.time()
                try:
                    status = self.cluster.backend.get_job_status(self.job.id)
                except Exception as exc:  # pragma: no cover - backend errors
                    status = {"JobState": "UNKNOWN", "Error": str(exc)}

                self.job._update_status_cache(status, timestamp)

                current_state = status.get("JobState") or "UNKNOWN"
                is_terminal = current_state in self.job.TERMINAL_STATES

                ctx = JobStatusUpdatedContext(
                    job=self.job,
                    job_id=self.job.id,
                    status=status,
                    timestamp=timestamp,
                    previous_state=self._previous_state,
                    is_terminal=is_terminal,
                )

                for callback, interval in self.subscriptions:
                    if not callback.should_run_on_client("on_job_status_update_ctx"):
                        continue

                    last_emit = self._last_emit.get(callback, 0.0)
                    should_emit = (
                        (timestamp - last_emit) >= interval
                        or self._previous_state != current_state
                        or is_terminal
                    )

                    if not should_emit:
                        continue

                    try:
                        callback.on_job_status_update_ctx(ctx)
                    except Exception as exc:  # pragma: no cover - callback errors
                        logger.debug(
                            "Callback %s failed during polling: %s",
                            type(callback).__name__,
                            exc,
                        )
                    self._last_emit[callback] = timestamp

                self._previous_state = current_state

                if is_terminal:
                    self.cluster._emit_completed_context(
                        self.job,
                        status,
                        timestamp,
                    )
                    break

                self._stop.wait(self._interval)
        finally:
            self.cluster._on_poller_finished(self.job.id)


def _looks_like_path(value: str) -> bool:
    if not value:
        return False
    separators = {os.sep}
    if os.altsep:
        separators.add(os.altsep)
    return (
        any(sep in value for sep in separators)
        or value.startswith(".")
        or value.startswith("~")
        or value.endswith(".toml")
    )


def _generate_timestamp_id() -> tuple[str, str]:
    """Generate timestamp and unique ID for hierarchical directory structure.

    Returns:
        tuple: (timestamp, unique_id) where timestamp is YYYYMMDD_HHMMSS format
            and unique_id is an 8-character hex string.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return timestamp, unique_id


def _sanitize_task_name(name: str) -> str:
    """Sanitize task name for filesystem use.

    Args:
        name: Raw task name.

    Returns:
        Sanitized task name safe for filesystem paths.

    Examples:
        >>> _sanitize_task_name("Train Model")
        'train_model'
        >>> _sanitize_task_name("model:v2")
        'model_v2'
    """
    name = name.lower()
    name = re.sub(r"[^\w\-]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name or "task"


class Cluster:
    """Represents a connection to a Slurm cluster for job submission and management.

    The Cluster class is your primary interface for interacting with a Slurm cluster.
    It handles job submission, status tracking, and result retrieval through a
    pluggable backend system.

    You can initialize a Cluster directly with connection parameters, or use the
    `from_env()` class method to load configuration from a Slurmfile.

    Examples:
        Direct SSH connection to a cluster:

            >>> cluster = Cluster(
            ...     backend_type="ssh",
            ...     hostname="hpc.example.edu",
            ...     username="myuser"
            ... )

        Load from a Slurmfile environment:

            >>> cluster = Cluster.from_env("production")
            >>> cluster = Cluster.from_env("path/to/Slurmfile.toml", env="dev")

    Attributes:
        backend_type: The type of backend in use (e.g., "ssh").
        backend: The backend instance handling cluster communication.
        callbacks: List of callback objects for lifecycle event hooks.
    """

    def __init__(
        self,
        backend_type: str = "ssh",
        callbacks: Optional[List[BaseCallback]] = None,
        job_base_dir: Optional[str] = None,
        **backend_kwargs,
    ):
        """Initialize a cluster connection.

        Args:
            backend_type: Backend implementation to use. Currently supports "ssh".
                Defaults to "ssh".
            callbacks: List of callback instances to receive lifecycle events
                (packaging, submission, execution). Callbacks enable observability
                and custom integrations. Pass a `LoggerCallback` with a Rich
                console to enable interactive terminal feedback. Defaults to an
                empty list.
            job_base_dir: Base directory on the target machine where job artifacts
                (scripts, outputs, results) will be stored. Each job gets a unique
                subdirectory. Defaults to "~/slurm_jobs" on the target.
            **backend_kwargs: Additional arguments passed to the backend constructor.
                For SSH backend: hostname, username, password, key_filename, port,
                and other SSH connection parameters.

        Raises:
            ValueError: If an unsupported backend_type is specified.
            RuntimeError: If backend initialization fails (e.g., SSH connection issues).

        Note:
            For production use, prefer `Cluster.from_env()` which loads configuration
            from a Slurmfile, enabling environment-specific settings and keeping
            credentials out of code.
        """
        self.backend_type = backend_type
        self.callbacks = callbacks or []

        if job_base_dir is not None:
            backend_kwargs["job_base_dir"] = job_base_dir

        self.backend = create_backend(backend_type, **backend_kwargs)
        self._job_pollers: Dict[str, _JobStatusPoller] = {}

    @classmethod
    def from_env(
        cls,
        slurmfile_path_or_env: Optional[str] = None,
        *,
        env: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
        callbacks: Optional[List[BaseCallback]] = None,
    ) -> "Cluster":
        """Construct a Cluster from a Slurmfile environment configuration.

        This is the recommended way to initialize clusters in most scenarios. It separates
        environment-specific configuration from code, supporting multiple deployment targets
        (dev, staging, production) from a single codebase.

        The method performs automatic Slurmfile discovery by searching upward from the
        current directory, checks the SLURMFILE environment variable, or uses an explicit
        path when provided.

        Args:
            slurmfile_path_or_env: Either a path to a Slurmfile, or an environment name.
                The method automatically distinguishes between the two:
                - If it looks like a path (contains `/`, `.`, `~`, or ends with `.toml`):
                  treats it as a file path
                - Otherwise: treats it as an environment name to load from the discovered
                  Slurmfile
                - If None: uses the default environment from the discovered Slurmfile,
                  or the environment specified by the SLURM_ENV environment variable
            env: Explicit environment name to load from the Slurmfile. Overrides any
                environment name inferred from `slurmfile_path_or_env`.
            overrides: Dictionary of values to override from the Slurmfile configuration.
                Useful for runtime parameter injection (e.g., credentials from secrets).
                Supported keys: 'backend', 'backend_type', 'backend_config',
                'job_base_dir', 'callbacks', plus any backend-specific parameters.
            callbacks: Explicit callback list, overriding callbacks defined in the
                Slurmfile. Pass an empty list `[]` to disable Slurmfile callbacks.
                Include a `LoggerCallback` with an optional Rich console for
                interactive CLI output.

        Returns:
            Cluster instance configured from the Slurmfile environment.

        Raises:
            SlurmfileNotFoundError: If no Slurmfile can be discovered and none was
                specified explicitly.
            SlurmfileInvalidError: If the Slurmfile has invalid TOML syntax or is
                missing required sections (like `[cluster]`).
            SlurmfileEnvironmentNotFoundError: If the requested environment name
                doesn't exist in the Slurmfile.

        Examples:
            Use the default environment from the discovered Slurmfile:

                >>> cluster = Cluster.from_env()

            Load a specific environment by name:

                >>> cluster = Cluster.from_env("production")
                >>> cluster = Cluster.from_env(env="staging")

            Use an explicit Slurmfile path:

                >>> cluster = Cluster.from_env("path/to/Slurmfile.toml")
                >>> cluster = Cluster.from_env("./configs/slurm.toml", env="dev")

            Override configuration at runtime:

                >>> cluster = Cluster.from_env(
                ...     "production",
                ...     overrides={"backend_config": {"hostname": "hpc2.example.edu"}}
                ... )

        Note:
            Slurmfile discovery searches upward from the current directory looking for
            files named: `Slurmfile`, `Slurmfile.toml`, `slurmfile`, or `slurmfile.toml`.
            Set the `SLURMFILE` environment variable to specify an explicit path without
            modifying code.
        """

        overrides = overrides or {}
        env_hint = env
        slurmfile_hint: Optional[str] = None

        if slurmfile_path_or_env:
            candidate = Path(slurmfile_path_or_env).expanduser()
            if candidate.exists() or _looks_like_path(slurmfile_path_or_env):
                slurmfile_hint = str(candidate)
            else:
                env_hint = slurmfile_path_or_env

        environment = load_environment(slurmfile_hint, env=env_hint)

        cluster_section = environment.config.get("cluster")
        if not isinstance(cluster_section, dict):
            raise SlurmfileInvalidError(
                f"Environment '{environment.name}' in '{environment.path}' requires a [cluster] table."
            )

        backend_type = (
            overrides.get("backend")
            or overrides.get("backend_type")
            or cluster_section.get("backend")
        )
        if not backend_type:
            raise SlurmfileInvalidError(
                f"Environment '{environment.name}' must define 'cluster.backend'."
            )

        raw_backend_config = cluster_section.get("backend_config", {})
        if raw_backend_config and not isinstance(raw_backend_config, dict):
            raise SlurmfileInvalidError(
                "cluster.backend_config must be a table of key/value pairs."
            )
        backend_config: Dict[str, Any] = dict(raw_backend_config or {})

        job_base_dir = overrides.get(
            "job_base_dir", cluster_section.get("job_base_dir")
        )
        if isinstance(job_base_dir, str):
            job_base_dir = os.path.expanduser(job_base_dir)

        backend_config_override = overrides.get("backend_config") or {}
        if backend_config_override:
            if not isinstance(backend_config_override, dict):
                raise SlurmfileInvalidError("'backend_config' override must be a dict.")
            backend_config.update(backend_config_override)

        extra_backend = {
            key: value
            for key, value in overrides.items()
            if key
            not in {
                "backend",
                "backend_type",
                "backend_config",
                "job_base_dir",
                "callbacks",
            }
        }
        if extra_backend:
            backend_config.update(extra_backend)

        if "callbacks" in overrides:
            callbacks_override = overrides["callbacks"]
            if callbacks_override is None:
                callback_list: List[BaseCallback] = []
            elif isinstance(callbacks_override, list):
                callback_list = list(callbacks_override)
            else:
                raise SlurmfileInvalidError(
                    "'callbacks' override must be a list of callbacks."
                )
        elif callbacks is not None:
            callback_list = list(callbacks)
        else:
            callback_list = list(environment.callbacks)

        cluster_instance = cls(
            backend_type=str(backend_type),
            callbacks=callback_list,
            job_base_dir=job_base_dir,
            **backend_config,
        )

        cluster_instance.env_name = environment.name  # type: ignore[attr-defined]
        cluster_instance.slurmfile_path = str(environment.path)  # type: ignore[attr-defined]
        cluster_instance.environment_config = environment.config  # type: ignore[attr-defined]
        cluster_instance.packaging_defaults = environment.config.get(  # type: ignore[attr-defined]
            "packaging",
        )
        cluster_instance.submit_defaults = environment.config.get(  # type: ignore[attr-defined]
            "submit",
        )

        return cluster_instance

    def submit(
        self,
        task_func: SlurmTask,
        packaging_config: Optional[Dict[str, Any]] = None,
        after: Optional[Union[Job, List[Job]]] = None,
        **sbatch_options,
    ) -> Callable[..., Job]:
        """Prepare a task for submission to the cluster.

        This method implements a two-phase submission pattern: it returns a callable
        ("submitter") that, when invoked with your task's runtime arguments, performs
        the actual job submission. This separation allows you to configure SBATCH
        parameters and packaging once, then submit multiple jobs with different arguments.

        The submission process:
        1. Packages your code (builds wheel or container as configured)
        2. Generates a bash job script with SBATCH directives
        3. Uploads the script and artifacts to the cluster
        4. Submits the job via `sbatch`
        5. Returns a Job object for tracking and result retrieval

        Args:
            task_func: A SlurmTask instance (created via the `@task` decorator).
                This encapsulates the Python function to execute remotely.
            packaging_config: Override packaging configuration for this submission.
                By default, uses packaging config from the Slurmfile or the task's
                decorator. Common keys: `{"type": "wheel"}` or `{"type": "container",
                "image": "myimage:latest"}`.
            after: Job dependency. If provided, this task will wait for the specified
                job(s) to complete successfully before running. Can be a single Job
                or a list of Jobs. Creates an "afterok" dependency.
            **sbatch_options: SBATCH directive overrides (e.g., `account="myaccount"`,
                `partition="gpu"`, `time="02:00:00"`). These override both the task
                decorator's defaults and Slurmfile settings. Use underscores in
                parameter names; they're automatically converted to dashes
                (e.g., `cpus_per_task=4` becomes `--cpus-per-task=4`).

        Returns:
            A submitter callable with signature `(*args, **kwargs) -> Job`.
            Call this with your task function's arguments to submit the job.

        Raises:
            ValueError: If task_func is not a SlurmTask instance (missing @task decorator).
            PackagingError: If code packaging fails (e.g., missing pyproject.toml for
                wheel packaging, docker/podman not found for containers).
            SubmissionError: If job submission fails (e.g., sbatch command error,
                network issues).

        Examples:
            Basic submission with default settings:

                >>> @task(time="01:00:00", cpus_per_task=2)
                ... def train_model(epochs: int):
                ...     return train(epochs)
                >>> submitter = cluster.submit(train_model)
                >>> job = submitter(epochs=100)
                >>> result = job.get_result()

            Override SBATCH parameters at submission time:

                >>> submitter = cluster.submit(
                ...     train_model,
                ...     partition="gpu",
                ...     gpus_per_node=4,
                ...     account="research"
                ... )
                >>> job = submitter(epochs=200)

            Override packaging configuration:

                >>> submitter = cluster.submit(
                ...     train_model,
                ...     packaging_config={"type": "container", "image": "ml:latest"}
                ... )
                >>> job = submitter(epochs=100)

            Submit multiple jobs with same configuration:

                >>> submitter = cluster.submit(train_model, partition="gpu")
                >>> jobs = [submitter(epochs=e) for e in [10, 50, 100]]
                >>> results = [job.get_result() for job in jobs]

        Note:
            The submitter captures the cluster reference, task, and all SBATCH overrides.
            You can store it and use it multiple times to submit related jobs with the
            same resource configuration but different runtime arguments.
        """
        from .rendering import render_job_script

        func_to_render: Callable
        task_defaults: Dict[str, Any] = {}
        normalized_overrides = normalize_sbatch_options(sbatch_options)

        # Process dependency parameter (after)
        if after is not None:
            # Convert Job or List[Job] to dependency string
            job_ids = []
            if isinstance(after, list):
                job_ids = [job.id for job in after]
            else:
                job_ids = [after.id]

            # Create dependency string (afterok means after successful completion)
            if job_ids:
                dependency_str = "afterok:" + ":".join(job_ids)
                normalized_overrides["dependency"] = dependency_str

        if not isinstance(task_func, SlurmTask):
            raise ValueError(
                f"Expected SlurmTask instance, got {type(task_func).__name__}. "
                f"Did you forget to use the @task decorator?"
            )

        func_to_render = task_func.func
        task_defaults = dict(getattr(task_func, "sbatch_options", {}) or {})

        def submitter(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Job:
            """The function returned by Cluster.submit, captures overrides and defaults."""

            submit_overrides = dict(normalized_overrides)

            def _prepare_packaging() -> Any:
                effective_packaging_config: Optional[Dict[str, Any]] = packaging_config
                if effective_packaging_config is None:
                    effective_packaging_config = getattr(
                        self, "packaging_defaults", None
                    )

                logger.debug(
                    "Effective packaging config: %s", effective_packaging_config
                )

                packaging_start = time.time()
                begin_ctx = PackagingBeginContext(
                    task=task_func,
                    packaging_config=effective_packaging_config,
                    cluster=self,
                    timestamp=packaging_start,
                )

                for callback in self.callbacks:
                    if not callback.should_run_on_client("on_begin_package_ctx"):
                        continue
                    try:
                        callback.on_begin_package_ctx(begin_ctx)
                    except Exception as exc:
                        logger.debug(
                            "Callback %s failed in on_begin_package_ctx: %s",
                            type(callback).__name__,
                            exc,
                        )

                strategy = get_packaging_strategy(effective_packaging_config)
                logger.debug(
                    "[%s] Using packaging strategy: %s", "n/a", type(strategy).__name__
                )
                try:
                    result = strategy.prepare(task=task_func, cluster=self)
                    logger.debug("[%s] Packaging prepared: %s", "n/a", result)
                except Exception as exc:
                    logger.error("[%s] Packaging preparation failed: %s", "n/a", exc)
                    traceback.print_exc(file=sys.stderr)
                    pkg_type = (
                        effective_packaging_config.get("type")
                        if effective_packaging_config
                        else "none"
                    )
                    raise PackagingError(
                        f"Packaging preparation failed for task '{getattr(task_func, '__name__', 'unknown')}'\n"
                        f"Packaging type: {pkg_type}\n"
                        f"Original error: {exc}"
                    ) from exc

                packaging_end = time.time()
                end_ctx = PackagingEndContext(
                    task=task_func,
                    packaging_result=strategy,
                    cluster=self,
                    timestamp=packaging_end,
                    duration=packaging_end - packaging_start,
                )

                for callback in self.callbacks:
                    if not callback.should_run_on_client("on_end_package_ctx"):
                        continue
                    try:
                        callback.on_end_package_ctx(end_ctx)
                    except Exception as exc:
                        logger.debug(
                            "Callback %s failed in on_end_package_ctx: %s",
                            type(callback).__name__,
                            exc,
                        )

                return strategy

            packaging_strategy = _prepare_packaging()

            # Generate timestamp and unique ID for hierarchical structure
            timestamp, unique_id = _generate_timestamp_id()
            pre_submission_id = f"{timestamp}_{unique_id}"

            # Get task name and sanitize it
            task_name = task_defaults.get("job_name", task_func.func.__name__)
            sanitized_task_name = _sanitize_task_name(task_name)

            resolved_job_base_dir = getattr(self.backend, "job_base_dir", None)
            if resolved_job_base_dir is None:
                import tempfile
                import os as _os

                resolved_job_base_dir = _os.path.join(
                    tempfile.gettempdir(), "slurm_jobs"
                )
                try:
                    _os.makedirs(resolved_job_base_dir, exist_ok=True)
                except Exception:
                    pass

            # Check if we're in a workflow context for nested structure
            from .context import get_active_context
            from .workflow import WorkflowContext

            ctx = get_active_context()
            if isinstance(ctx, WorkflowContext):
                # Nested in workflow: {workflow_dir}/tasks/{task_name}/{timestamp}_{unique_id}/
                target_job_dir = os.path.join(
                    str(ctx.workflow_job_dir),
                    "tasks",
                    sanitized_task_name,
                    f"{timestamp}_{unique_id}",
                )
            else:
                # Regular task: {job_base_dir}/{task_name}/{timestamp}_{unique_id}/
                target_job_dir = os.path.join(
                    resolved_job_base_dir,
                    sanitized_task_name,
                    f"{timestamp}_{unique_id}",
                )
            logger.debug(
                "[%s] Target job directory path: %s", pre_submission_id, target_job_dir
            )

            # Build effective SBATCH options with proper precedence:
            # 1. Slurmfile [submit] defaults (lowest priority)
            # 2. Task decorator defaults
            # 3. Runtime overrides (highest priority)
            effective_sbatch_options: Dict[str, Any] = {}

            # Add Slurmfile submit defaults if available
            slurmfile_submit_defaults = getattr(self, "submit_defaults", None)
            if slurmfile_submit_defaults and isinstance(
                slurmfile_submit_defaults, dict
            ):
                # Normalize the Slurmfile submit defaults
                effective_sbatch_options.update(
                    normalize_sbatch_options(slurmfile_submit_defaults)
                )

            # Add task defaults
            effective_sbatch_options.update(task_defaults)

            # Add runtime overrides
            effective_sbatch_options.update(submit_overrides)

            stdout_path = effective_sbatch_options.get("output")
            if not stdout_path:
                stdout_path = f"{target_job_dir}/slurm_{pre_submission_id}.out"

            stderr_path = effective_sbatch_options.get("error")
            if not stderr_path:
                stderr_path = f"{target_job_dir}/slurm_{pre_submission_id}.err"

            submit_begin_ctx = SubmitBeginContext(
                task=task_func,
                sbatch_options=dict(effective_sbatch_options),
                pre_submission_id=pre_submission_id,
                target_job_dir=target_job_dir,
                cluster=self,
                packaging_strategy=packaging_strategy,
                backend_type=self.backend_type,
            )

            for callback in self.callbacks:
                if not callback.should_run_on_client("on_begin_submit_job_ctx"):
                    continue
                try:
                    callback.on_begin_submit_job_ctx(submit_begin_ctx)
                except Exception as exc:
                    logger.debug(
                        "Callback %s failed in on_begin_submit_job_ctx: %s",
                        type(callback).__name__,
                        exc,
                    )

            script = render_job_script(
                task_func=func_to_render,
                task_args=args,
                task_kwargs=kwargs,
                task_definition=task_defaults,
                sbatch_overrides=dict(submit_overrides),
                packaging_strategy=packaging_strategy,
                target_job_dir=target_job_dir,
                pre_submission_id=pre_submission_id,
                callbacks=self.callbacks,
                cluster=self,  # Pass cluster for workflow support
            )
            logger.debug(
                "[%s] --- RENDERED SBATCH SCRIPT ---\n%s\n[%s] --- END RENDERED SCRIPT ---",
                pre_submission_id,
                script,
                pre_submission_id,
            )

            # Get account and partition from effective options (which includes Slurmfile defaults)
            submit_account = effective_sbatch_options.get("account")
            submit_partition = effective_sbatch_options.get("partition")
            logger.debug("[%s] submit_account: %s", pre_submission_id, submit_account)
            logger.debug(
                "[%s] submit_partition: %s", pre_submission_id, submit_partition
            )
            submission_message = (
                f"[{pre_submission_id}] Submitting job via {self.backend_type} backend"
            )
            try:
                logger.info(submission_message)
                job_submission_result = self.backend.submit_job(
                    script,
                    target_job_dir=target_job_dir,
                    pre_submission_id=pre_submission_id,
                    account=submit_account,
                    partition=submit_partition,
                )
            except Exception as e:
                raise SubmissionError(
                    f"Failed to submit job via backend '{self.backend_type}': {e}",
                    script=script,
                    metadata={
                        "backend": self.backend_type,
                        "account": submit_account,
                        "partition": submit_partition,
                        "pre_submission_id": pre_submission_id,
                        "target_job_dir": target_job_dir,
                    },
                ) from e

            if isinstance(job_submission_result, str):
                job_id = job_submission_result
            elif (
                isinstance(job_submission_result, tuple)
                and len(job_submission_result) == 2
            ):
                job_id, _ = job_submission_result
            else:
                raise TypeError(
                    f"Unexpected return type from backend.submit_job: {type(job_submission_result)}"
                )

            job = Job(
                id=job_id,
                cluster=self,
                task_func=task_func,
                args=args,
                kwargs=kwargs,
                target_job_dir=target_job_dir,
                pre_submission_id=pre_submission_id,
                sbatch_options=dict(effective_sbatch_options),
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            )

            # Generate metadata.json
            is_workflow = getattr(task_func, "_is_workflow", False)
            logger.debug(
                "[%s] is_workflow=%s (from task_func attribute)",
                pre_submission_id,
                is_workflow,
            )
            metadata = {
                "job_id": job_id,
                "pre_submission_id": pre_submission_id,
                "task_name": sanitized_task_name,
                "timestamp": timestamp,
                "submitted_at": time.time(),
                "status": "PENDING",
                "is_workflow": is_workflow,
            }

            # Check if we're in a workflow context (for parent_workflow tracking)
            from .context import get_active_context
            from .workflow import WorkflowContext

            ctx = get_active_context()
            if isinstance(ctx, WorkflowContext):
                metadata["parent_workflow"] = ctx.workflow_job_id

                # Emit workflow task submission event
                logger.debug("Calling on_workflow_task_submitted callbacks...")
                try:
                    from pathlib import Path as PathType

                    submit_ctx = WorkflowTaskSubmitContext(
                        parent_workflow_id=ctx.workflow_job_id,
                        parent_workflow_dir=ctx.workflow_job_dir,
                        parent_workflow_name=task_func.func.__name__
                        if hasattr(task_func, "func")
                        else str(ctx.workflow_job_id).split("_")[0],
                        child_job_id=job_id,
                        child_job_dir=PathType(target_job_dir),
                        child_task_name=sanitized_task_name,
                        child_is_workflow=is_workflow,
                        timestamp=time.time(),
                        cluster=self,
                    )
                    for callback in self.callbacks:
                        if not callback.should_run_on_client(
                            "on_workflow_task_submitted_ctx"
                        ):
                            continue
                        try:
                            callback.on_workflow_task_submitted_ctx(submit_ctx)
                        except Exception as exc:
                            logger.debug(
                                "Callback %s failed in on_workflow_task_submitted_ctx: %s",
                                type(callback).__name__,
                                exc,
                            )
                except Exception as e:
                    logger.warning(
                        f"Error calling workflow task submitted callbacks: {e}"
                    )
            else:
                metadata["parent_workflow"] = None

            # Write metadata file via backend
            metadata_path = os.path.join(target_job_dir, "metadata.json")
            try:
                # For SSH backend, use _upload_string_to_file
                if hasattr(self.backend, "_upload_string_to_file"):
                    self.backend._upload_string_to_file(
                        json.dumps(metadata, indent=2), metadata_path
                    )
                elif hasattr(self.backend, "write_file"):
                    # Future: if we add a write_file() method to backends
                    self.backend.write_file(
                        metadata_path, json.dumps(metadata, indent=2)
                    )
                else:
                    # For local backends, write directly
                    import os as _os

                    _os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
                    with open(metadata_path, "w") as f:
                        json.dump(metadata, f, indent=2)
            except Exception as exc:
                logger.warning(
                    "[%s] Failed to write metadata.json: %s", pre_submission_id, exc
                )

            # Upload Slurmfile for workflow support
            logger.debug(
                "[%s] Checking workflow Slurmfile upload: is_workflow=%s",
                pre_submission_id,
                is_workflow,
            )
            if is_workflow:
                slurmfile_path = getattr(self, "slurmfile_path", None)
                logger.debug(
                    "[%s] slurmfile_path=%s, exists=%s",
                    pre_submission_id,
                    slurmfile_path,
                    os.path.exists(slurmfile_path) if slurmfile_path else False,
                )
                if slurmfile_path and os.path.exists(slurmfile_path):
                    try:
                        with open(slurmfile_path, "r") as f:
                            slurmfile_content = f.read()

                        # Modify Slurmfile for workflow execution inside cluster
                        # Replace external hostname/port with internal cluster hostname and standard SSH port
                        import re

                        # Get environment name to modify the correct section
                        env_name = getattr(self, "env_name", "default")

                        # Determine the hostname to use inside the cluster
                        # Query the actual cluster to get its hostname
                        try:
                            result = self.backend.execute_command("hostname")
                            internal_hostname = result.strip()
                            if not internal_hostname:
                                internal_hostname = "localhost"
                        except:
                            internal_hostname = "localhost"

                        # Create a workflow-specific Slurmfile with internal SSH config
                        modified_content = slurmfile_content

                        # Replace hostname value for SSH backend
                        modified_content = re.sub(
                            r'(hostname\s*=\s*)"[^"]*"',
                            f'\\1"{internal_hostname}"',
                            modified_content
                        )

                        # Replace port with standard SSH port 22
                        modified_content = re.sub(
                            r'(\bport\s*=\s*)\d+',
                            r'\g<1>22',
                            modified_content
                        )

                        # Remove password authentication, use key-based
                        modified_content = re.sub(
                            r'password\s*=\s*"[^"]*"\s*\n',
                            '',
                            modified_content
                        )

                        # Enable key-based authentication
                        modified_content = re.sub(
                            r'look_for_keys\s*=\s*false',
                            'look_for_keys = true',
                            modified_content
                        )

                        logger.debug(
                            "[%s] Modified Slurmfile for workflow: hostname=%s, port=22, key-based auth",
                            pre_submission_id,
                            internal_hostname,
                        )

                        remote_slurmfile_path = os.path.join(
                            target_job_dir, "Slurmfile.toml"
                        )
                        if hasattr(self.backend, "_upload_string_to_file"):
                            self.backend._upload_string_to_file(
                                modified_content, remote_slurmfile_path
                            )
                            logger.debug(
                                "[%s] Uploaded modified Slurmfile to %s",
                                pre_submission_id,
                                remote_slurmfile_path,
                            )
                        else:
                            logger.warning(
                                "[%s] Backend does not have _upload_string_to_file method",
                                pre_submission_id,
                            )
                    except Exception as exc:
                        logger.warning(
                            "[%s] Failed to upload Slurmfile: %s",
                            pre_submission_id,
                            exc,
                        )
                else:
                    logger.debug(
                        "[%s] Skipping Slurmfile upload: path missing or doesn't exist",
                        pre_submission_id,
                    )

            submit_end_ctx = SubmitEndContext(
                job=job,
                job_id=str(job_id),
                pre_submission_id=pre_submission_id,
                target_job_dir=target_job_dir,
                sbatch_options=dict(effective_sbatch_options),
                cluster=self,
                backend_type=self.backend_type,
            )

            for callback in self.callbacks:
                if not callback.should_run_on_client("on_end_submit_job_ctx"):
                    continue
                try:
                    callback.on_end_submit_job_ctx(submit_end_ctx)
                except Exception as exc:
                    logger.debug(
                        "Callback %s failed in on_end_submit_job_ctx: %s",
                        type(callback).__name__,
                        exc,
                    )

            self._maybe_start_job_poller(job)

            return job

        return submitter

    def _maybe_start_job_poller(self, job: Job) -> None:
        subscriptions: List[Tuple[BaseCallback, float]] = []
        for callback in self.callbacks:
            interval = callback.get_poll_interval()
            if interval is None:
                continue
            if not callback.should_run_on_client("on_job_status_update_ctx"):
                continue
            subscriptions.append((callback, interval))

        if not subscriptions:
            return

        if not hasattr(self, "_job_pollers"):
            self._job_pollers: Dict[str, _JobStatusPoller] = {}

        poller = _JobStatusPoller(self, job, subscriptions)
        self._job_pollers[job.id] = poller
        poller.start()

    def _emit_completed_context(
        self,
        job: Job,
        status: Dict[str, Any],
        timestamp: Optional[float],
        *,
        error_payload: Optional[Dict[str, Optional[str]]] = None,
        emitted_by: ExecutionLocus = ExecutionLocus.CLIENT,
    ) -> None:
        if not hasattr(job, "_completed_context_lock"):
            return

        with job._completed_context_lock:
            if getattr(job, "_completed_context_emitted", False):
                return
            job._completed_context_emitted = True

        finished_at = timestamp or time.time()
        job.finished_at = job.finished_at or finished_at
        start_time = job.started_at or job.created_at
        duration: Optional[float] = None
        if job.finished_at is not None and start_time is not None:
            duration = job.finished_at - start_time

        context = CompletedContext(
            job=job if emitted_by is ExecutionLocus.CLIENT else None,
            job_id=job.id,
            job_dir=job.target_job_dir,
            job_state=status.get("JobState"),
            exit_code=status.get("ExitCode"),
            reason=status.get("Reason")
            or status.get("Error")
            or status.get("StateDesc"),
            stdout_path=job.stdout_path,
            stderr_path=job.stderr_path,
            start_time=start_time,
            end_time=job.finished_at,
            duration=duration,
            status=status,
            error_type=error_payload.get("error_type") if error_payload else None,
            error_message=error_payload.get("error_message") if error_payload else None,
            traceback=error_payload.get("traceback") if error_payload else None,
            result_path=job.result_path,
            emitted_by=emitted_by,
        )

        for callback in self.callbacks:
            if not callback.should_run_on_client("on_completed_ctx"):
                continue
            try:
                callback.on_completed_ctx(context)
            except Exception as exc:  # pragma: no cover - callback errors
                logger.debug(
                    "Callback %s failed in on_completed_ctx: %s",
                    type(callback).__name__,
                    exc,
                )

    def _on_poller_finished(self, job_id: str) -> None:
        pollers = getattr(self, "_job_pollers", None)
        if pollers is not None:
            pollers.pop(job_id, None)

    def get_job(self, job_id: str) -> Job:
        """Retrieve a Job object for an existing Slurm job by its ID.

        This method attempts to reconstruct job metadata (working directory, result paths)
        by querying the scheduler. However, for full functionality (especially result
        retrieval), prefer using the Job object returned directly from `submit()`.

        Args:
            job_id: The Slurm job ID (numeric string, e.g., "12345").

        Returns:
            Job instance for tracking status and retrieving results. Some metadata
            may be missing if the scheduler doesn't provide it.

        Examples:
            >>> job = cluster.get_job("12345")
            >>> status = job.get_status()
            >>> if job.is_completed():
            ...     result = job.get_result()  # May fail if metadata missing
        """
        target_job_dir: Optional[str] = None
        pre_submission_id: Optional[str] = None
        stdout_path: Optional[str] = None
        stderr_path: Optional[str] = None
        status: Dict[str, Any] = {}

        try:
            status = self.backend.get_job_status(job_id) or {}
            work_dir = status.get("WorkDir") or status.get("WorkDirectory")
            if isinstance(work_dir, str) and work_dir.strip():
                target_job_dir = work_dir.strip()

            stdout_path_candidate = (
                status.get("StdOut")
                or status.get("StdOutFile")
                or status.get("StdOutPath")
            )
            if isinstance(stdout_path_candidate, str) and stdout_path_candidate:
                stdout_path = stdout_path_candidate
            stderr_path_candidate = (
                status.get("StdErr")
                or status.get("StdErrFile")
                or status.get("StdErrPath")
            )
            if isinstance(stderr_path_candidate, str) and stderr_path_candidate:
                stderr_path = stderr_path_candidate
            if isinstance(stdout_path, str) and stdout_path:
                base_name = os.path.basename(stdout_path)
                m = re.match(r"^slurm_([A-Za-z0-9]+)\.out$", base_name)
                if m:
                    pre_submission_id = m.group(1)
        except Exception:
            pass

        return Job(
            job_id,
            self,
            target_job_dir=target_job_dir,
            pre_submission_id=pre_submission_id,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

    def get_jobs(self) -> List[Job]:
        """Get all jobs currently in the Slurm queue for the current user.

        Returns:
            List of Job instances for all jobs in the queue (pending, running, etc.).
            Completed jobs that have left the queue are not included.

        Examples:
            >>> jobs = cluster.get_jobs()
            >>> running = [j for j in jobs if j.is_running()]
            >>> print(f"Running jobs: {len(running)}")
        """
        queue = self.backend.get_queue() or []
        job_ids: List[str] = []
        for entry in queue:
            job_id = (
                entry.get("JOBID")
                or entry.get("JobID")
                or entry.get("ID")
                or entry.get("job_id")
            )
            if job_id:
                job_ids.append(str(job_id))
        return [Job(job_id, self) for job_id in job_ids]

    def get_queue(self) -> List[Dict[str, Any]]:
        """Get raw queue information from the Slurm scheduler.

        Returns:
            List of dictionaries containing job information from `squeue`.
            Each dictionary has keys like JOBID, NAME, STATE, USER, TIME, etc.
            The exact keys depend on the backend implementation.
        """
        return self.backend.get_queue()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the cluster configuration.

        Returns:
            Dictionary containing cluster information from `sinfo`, including
            partitions, node availability, and limits. Structure depends on
            the backend implementation.
        """
        return self.backend.get_cluster_info()

    def __enter__(self) -> "Cluster":
        """Enter cluster context - enables submitless execution.

        When used as a context manager, tasks and workflows can be called
        directly without explicit .submit() calls. The context is tracked
        using contextvars for async/thread safety.

        Returns:
            The Cluster instance itself.

        Example:
            >>> with Cluster.from_env() as cluster:
            ...     job = my_task("arg")  # Automatically submits
            ...     result = job.get_result()
        """
        from .context import set_active_context

        self._context_token = set_active_context(self)
        return self

    def __exit__(self, *args) -> bool:
        """Exit cluster context - restore previous context.

        Args:
            *args: Exception info (exc_type, exc_value, traceback) if an
                exception occurred, or (None, None, None) for normal exit.

        Returns:
            False to propagate any exception that occurred.
        """
        from .context import reset_active_context

        if hasattr(self, "_context_token"):
            reset_active_context(self._context_token)
            delattr(self, "_context_token")
        return False
