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


class SubmittableWorkflow:
    """Wrapper for workflow submissions that supports pre-building dependent task containers.

    This class is returned by `cluster.submit()` when submitting a workflow, allowing
    users to specify dependent tasks that need their containers built before the workflow runs.
    """

    def __init__(
        self,
        cluster: "Cluster",
        submitter: Callable[..., Job],
        task_func: "SlurmTask",
        packaging_config: Optional[Dict[str, Any]],
        container_dependencies: Optional[List["SlurmTask"]] = None,
    ):
        self._cluster = cluster
        self._submitter = submitter
        self._task_func = task_func
        self._packaging_config = packaging_config
        self._dependent_tasks: List["SlurmTask"] = container_dependencies or []
        self._prebuilt_images: Dict[str, str] = {}

    def with_dependencies(self, tasks: List["SlurmTask"]) -> "SubmittableWorkflow":
        """Specify dependent tasks that need their containers pre-built.

        When the workflow is submitted, containers for these tasks will be built
        and pushed before the workflow starts. The workflow's child tasks can then
        use these pre-built images instead of inheriting the parent's container.

        Args:
            tasks: List of SlurmTask instances that will be submitted by the workflow.
                   These can include tasks with overrides (e.g., task.with_options(...)).

        Returns:
            Self for method chaining.

        Example:
            >>> job = cluster.submit(my_workflow).with_dependencies([
            ...     task_a,
            ...     task_b.with_options(packaging_container_tag="v2"),
            ... ])(input_data)
        """
        self._dependent_tasks = list(tasks)
        return self

    def _build_dependency_containers(self) -> Dict[str, str]:
        """Build containers for all dependent tasks.

        Returns:
            Dict mapping task function qualified names to their pre-built image references.
        """
        prebuilt_images: Dict[str, str] = {}

        logger.info(
            f"Building dependency containers for {len(self._dependent_tasks)} tasks"
        )

        for task in self._dependent_tasks:
            if not isinstance(task, SlurmTask):
                logger.warning(f"Skipping non-SlurmTask dependency: {type(task)}")
                continue

            # Get the task's packaging config
            task_packaging = getattr(task, "packaging", None)
            logger.debug(f"Task packaging config: {task_packaging}")
            if not task_packaging:
                task_packaging = {}

            # Merge with any overrides from with_options
            effective_packaging = dict(task_packaging)

            # Determine the packaging type
            pkg_type = effective_packaging.get("type", "auto")
            logger.debug(f"Packaging type: {pkg_type}")

            # For auto or inherit, use cluster defaults
            if pkg_type in ("auto", "inherit", None):
                if self._cluster.packaging_defaults:
                    effective_packaging = dict(self._cluster.packaging_defaults)
                    effective_packaging.update(task_packaging)

            # Only build containers
            if effective_packaging.get("type") != "container":
                logger.debug(
                    f"Skipping non-container packaging: {effective_packaging.get('type')}"
                )
                continue

            # Get task identifier
            func = task.func
            task_name = f"{func.__module__}.{func.__qualname__}"

            logger.info(f"Building container for dependent task: {task_name}")
            logger.debug(f"Effective packaging: {effective_packaging}")

            # Prepare the packaging strategy
            packaging_strategy = self._cluster._prepare_packaging_strategy(
                task, effective_packaging
            )

            if packaging_strategy and hasattr(packaging_strategy, "_image_reference"):
                image_ref = packaging_strategy._image_reference
                if image_ref:
                    prebuilt_images[task_name] = image_ref
                    logger.info(f"Pre-built image for {task_name}: {image_ref}")
                else:
                    logger.warning(f"No image reference set for {task_name}")
            else:
                logger.warning("Packaging strategy has no _image_reference attribute")

        logger.info(
            f"Built {len(prebuilt_images)} pre-built images: {list(prebuilt_images.keys())}"
        )
        return prebuilt_images

    def __call__(self, *args, **kwargs) -> Job:
        """Submit the workflow, building dependency containers first if specified."""
        # Build containers for dependencies
        if self._dependent_tasks:
            self._prebuilt_images = self._build_dependency_containers()

        # If we have pre-built images, pass them to the workflow via environment
        if self._prebuilt_images:
            # Store pre-built images in cluster's environment for rendering
            if not hasattr(self._cluster, "_prebuilt_dependency_images"):
                self._cluster._prebuilt_dependency_images = {}
            self._cluster._prebuilt_dependency_images.update(self._prebuilt_images)

        # Call the original submitter
        return self._submitter(*args, **kwargs)


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
        default_packaging: Optional[str] = None,
        default_account: Optional[str] = None,
        default_partition: Optional[str] = None,
        **backend_kwargs,
    ):
        """Initialize a cluster connection.

        Args:
            backend_type: Backend implementation to use. Currently supports "ssh" and "local".
                Defaults to "ssh".
            callbacks: List of callback instances to receive lifecycle events
                (packaging, submission, execution). Callbacks enable observability
                and custom integrations. Pass a `LoggerCallback` with a Rich
                console to enable interactive terminal feedback. Defaults to an
                empty list.
            job_base_dir: Base directory on the target machine where job artifacts
                (scripts, outputs, results) will be stored. Each job gets a unique
                subdirectory. Defaults to "~/slurm_jobs" on the target.
            default_packaging: Default packaging strategy for all tasks submitted through
                this cluster. Tasks can override this. Options: "auto" (default), "wheel",
                "none", "container:IMAGE:TAG". Defaults to None (no cluster-wide default).
            default_account: Default SLURM account for all jobs submitted through this
                cluster. Tasks/submissions can override this. Defaults to None.
            default_partition: Default SLURM partition for all jobs submitted through this
                cluster. Tasks/submissions can override this. Defaults to None.
            **backend_kwargs: Additional arguments passed to the backend constructor.
                For SSH backend: hostname, username, password, key_filename, port,
                and other SSH connection parameters.

                For packaging defaults (all optional, prefixed with `default_packaging_`):
                - `default_packaging_python_version`: Python version for wheel packaging (e.g., "3.11")
                - `default_packaging_build_tool`: Build tool to use (e.g., "uv", "pip")
                - `default_packaging_dockerfile`: Path to Dockerfile for building container
                - `default_packaging_context`: Docker build context directory (default: ".")
                - `default_packaging_registry`: Container registry URL
                - `default_packaging_push`: Whether to push container to registry (bool)
                - `default_packaging_runtime`: Container runtime (e.g., "docker", "podman")
                - `default_packaging_platform`: Target platform (e.g., "linux/amd64")
                - `default_packaging_mounts`: List of volume mounts for container
                - `default_packaging_srun_args`: Additional srun arguments for container

        Raises:
            ValueError: If an unsupported backend_type is specified.
            RuntimeError: If backend initialization fails (e.g., SSH connection issues).

        Example:
            Create cluster with defaults:

                >>> cluster = Cluster(
                ...     backend_type="ssh",
                ...     hostname="hpc.example.edu",
                ...     username="myuser",
                ...     default_packaging="container",
                ...     default_packaging_platform="linux/amd64",
                ...     default_packaging_registry="myregistry.io/myproject/",
                ...     default_packaging_push=True,
                ...     default_account="research",
                ...     default_partition="cpu"
                ... )
        """
        # Validate backend type early for clearer error messages
        valid_backends = ["ssh", "local"]
        if backend_type not in valid_backends:
            raise ValueError(
                f"Invalid backend_type: {backend_type!r}. "
                f"Must be one of: {', '.join(valid_backends)}"
            )

        self.backend_type = backend_type
        self.callbacks = callbacks or []
        self.default_packaging = default_packaging
        self.default_account = default_account
        self.default_partition = default_partition

        # Extract default_packaging_* kwargs and store them separately
        self.default_packaging_kwargs: Dict[str, Any] = {}
        backend_only_kwargs = {}
        for key, value in backend_kwargs.items():
            if key.startswith("default_packaging_"):
                # Remove the "default_packaging_" prefix for storage
                self.default_packaging_kwargs[key[18:]] = value
            else:
                backend_only_kwargs[key] = value

        if job_base_dir is not None:
            backend_only_kwargs["job_base_dir"] = job_base_dir

        self._backend_kwargs = dict(backend_only_kwargs)
        self.backend = create_backend(backend_type, **backend_only_kwargs)
        self._job_pollers: Dict[str, _JobStatusPoller] = {}

    def _render_workflow_slurmfile(self, env_name: str) -> str:
        """Render a minimal Slurmfile for nested workflow execution.

        This is used as a fallback when the Cluster was created without a Slurmfile
        (e.g. via `Cluster.from_args()`), but workflow jobs still need to recreate a
        Cluster instance on the runner side.
        """
        env_name = env_name or "default"
        backend_config = dict(self._backend_kwargs)
        job_base_dir = backend_config.pop("job_base_dir", None)
        if job_base_dir is None:
            job_base_dir = getattr(self.backend, "job_base_dir", None) or "~/slurm_jobs"

        lines: list[str] = []
        lines.append(f"[{env_name}.cluster]")
        lines.append(f'backend = "{self.backend_type}"')
        lines.append(f'job_base_dir = "{job_base_dir}"')
        lines.append("")

        if self.backend_type == "ssh":
            lines.append(f"[{env_name}.cluster.backend_config]")
            for key in sorted(backend_config.keys()):
                value = backend_config[key]
                if value is None:
                    continue
                if isinstance(value, bool):
                    rendered = "true" if value else "false"
                elif isinstance(value, (int, float)):
                    rendered = str(value)
                else:
                    rendered = str(value).replace('"', '\\"')
                    rendered = f'"{rendered}"'
                lines.append(f"{key} = {rendered}")
            lines.append("")

        from .decorators import _parse_packaging_config

        packaging_config = _parse_packaging_config(self.default_packaging or "auto", {})
        packaging_config = packaging_config or {}
        packaging_config.update(self.default_packaging_kwargs)
        if packaging_config:
            lines.append(f"[{env_name}.packaging]")
            for key, value in sorted(packaging_config.items()):
                if value is None:
                    continue
                if isinstance(value, bool):
                    rendered = "true" if value else "false"
                elif isinstance(value, (int, float)):
                    rendered = str(value)
                else:
                    rendered = str(value).replace('"', '\\"')
                    rendered = f'"{rendered}"'
                lines.append(f"{key} = {rendered}")
            lines.append("")

        submit_defaults: dict[str, Any] = {}
        if self.default_account:
            submit_defaults["account"] = self.default_account
        if self.default_partition:
            submit_defaults["partition"] = self.default_partition
        if submit_defaults:
            lines.append(f"[{env_name}.submit]")
            for key, value in sorted(submit_defaults.items()):
                rendered = str(value).replace('"', '\\"')
                lines.append(f'{key} = "{rendered}"')
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

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

    @classmethod
    def from_file(cls, config_path: str, **extra_kwargs) -> "Cluster":
        """Create a Cluster instance from a flat TOML configuration file.

        This method provides explicit, simple configuration loading without auto-discovery.
        The config file should use a flat structure with direct key-value pairs.

        Args:
            config_path: Explicit path to the TOML configuration file.
            **extra_kwargs: Additional parameters to override or supplement the config file.
                These are passed directly to the Cluster constructor.

        Returns:
            Cluster instance configured from the file.

        Raises:
            FileNotFoundError: If config_path does not exist.
            ValueError: If the TOML file is invalid or missing required fields.

        Example config file (flat structure):
            ```toml
            backend = "ssh"
            hostname = "slurm.example.com"
            username = "myuser"
            job_base_dir = "/scratch/jobs"
            default_packaging = "auto"
            default_account = "my-account"
            default_partition = "compute"
            ```

        Example usage:
            >>> cluster = Cluster.from_file("config.toml")
            >>> cluster = Cluster.from_file("prod.toml", default_partition="gpu")
        """
        import tomllib
        from pathlib import Path

        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                f"Please provide an explicit path to your config file."
            )

        try:
            with open(config_file, "rb") as f:
                config = tomllib.load(f)
        except Exception as e:
            raise ValueError(
                f"Failed to parse TOML configuration file '{config_path}': {e}\n"
                f"Please ensure the file contains valid TOML syntax."
            ) from e

        # Extract backend configuration
        backend_type = config.get("backend", "ssh")

        # Build kwargs for Cluster.__init__
        cluster_kwargs = {}

        # Direct passthrough for simple fields
        passthrough_fields = [
            "hostname",
            "username",
            "password",
            "ssh_key_path",
            "job_base_dir",
            "default_packaging",
            "default_account",
            "default_partition",
        ]
        for field in passthrough_fields:
            if field in config:
                cluster_kwargs[field] = config[field]

        cluster_kwargs["backend_type"] = backend_type

        # Merge with extra_kwargs (extra_kwargs take precedence)
        cluster_kwargs.update(extra_kwargs)

        return cls(**cluster_kwargs)

    @staticmethod
    def add_argparse_args(parser) -> None:
        """Add common cluster configuration arguments to an argparse parser.

        This is a convenience method for building CLI tools that create Cluster instances.
        It adds standard arguments for SSH connection and cluster defaults.

        Args:
            parser: An argparse.ArgumentParser instance to add arguments to.

        Example:
            >>> import argparse
            >>> from slurm import Cluster
            >>>
            >>> parser = argparse.ArgumentParser()
            >>> Cluster.add_argparse_args(parser)
            >>> args = parser.parse_args()
            >>> cluster = Cluster.from_args(args)
        """
        parser.add_argument(
            "--hostname", help="Hostname of the SLURM cluster (for SSH backend)"
        )
        parser.add_argument(
            "--port",
            type=int,
            default=22,
            help="SSH port for the SLURM cluster (default: 22)",
        )
        parser.add_argument(
            "--banner-timeout",
            type=int,
            default=30,
            help="Timeout for waiting for SSH banner (seconds)",
        )
        parser.add_argument(
            "--username",
            default=os.getenv("USER"),
            help="Username for SSH connection (default: $USER)",
        )
        parser.add_argument(
            "--password",
            help="Password for SSH connection (if key-based auth is not used)",
        )
        parser.add_argument(
            "--backend",
            default="ssh",
            choices=["ssh", "local"],
            help="Backend type (default: ssh)",
        )
        parser.add_argument(
            "--job-base-dir",
            help="Base directory for job artifacts on cluster (default: ~/slurm_jobs)",
        )
        parser.add_argument(
            "--account", help="Default SLURM account for job submissions"
        )
        parser.add_argument(
            "--partition", help="Default SLURM partition for job submissions"
        )
        parser.add_argument(
            "--packaging",
            default="auto",
            help="Default packaging strategy: auto, wheel, none, or container:IMAGE:TAG (default: auto)",
        )
        parser.add_argument(
            "--packaging-registry",
            help="Container registry for pushing images.",
        )
        parser.add_argument(
            "--packaging-platform",
            default="linux/amd64",
            help="Container platform for building images (default: linux/amd64)",
        )
        parser.add_argument(
            "--packaging-tls-verify",
            default=True,
            type=lambda x: str(x).lower() in {"1", "true", "yes"},
            help="Whether to verify TLS when pushing/pulling containers (default: true)",
        )

    @classmethod
    def from_args(cls, args, **extra_kwargs) -> "Cluster":
        """Create a Cluster instance from argparse arguments.

        This method works with arguments added by `add_argparse_args()` to create
        a cluster from command-line arguments.

        Args:
            args: Parsed arguments from argparse (argparse.Namespace).
            **extra_kwargs: Additional keyword arguments to pass to Cluster.__init__(),
                which override values from args. Useful for programmatically setting
                callbacks, backend_kwargs, etc.

        Returns:
            Cluster instance configured from the arguments.

        Example:
            >>> import argparse
            >>> from slurm import Cluster
            >>> from slurm.callbacks import LoggerCallback
            >>>
            >>> parser = argparse.ArgumentParser()
            >>> Cluster.add_argparse_args(parser)
            >>> args = parser.parse_args()
            >>>
            >>> cluster = Cluster.from_args(
            ...     args,
            ...     callbacks=[LoggerCallback()]
            ... )
        """
        # Extract cluster configuration from args
        kwargs = {}

        # Backend configuration
        if hasattr(args, "backend") and args.backend:
            kwargs["backend_type"] = args.backend
        if hasattr(args, "hostname") and args.hostname:
            kwargs["hostname"] = args.hostname
        if hasattr(args, "port") and args.port:
            kwargs["port"] = args.port
        if hasattr(args, "username") and args.username:
            kwargs["username"] = args.username
        if hasattr(args, "password") and args.password:
            kwargs["password"] = args.password
        if hasattr(args, "job_base_dir") and args.job_base_dir:
            kwargs["job_base_dir"] = args.job_base_dir

        # Default parameters
        if hasattr(args, "packaging") and args.packaging:
            kwargs["default_packaging"] = args.packaging
        if hasattr(args, "packaging_registry") and args.packaging_registry:
            kwargs["default_packaging_registry"] = args.packaging_registry
        if hasattr(args, "packaging_platform") and args.packaging_platform:
            kwargs["default_packaging_platform"] = args.packaging_platform
        if hasattr(args, "packaging_tls_verify"):
            kwargs["default_packaging_tls_verify"] = args.packaging_tls_verify
        if hasattr(args, "account") and args.account:
            kwargs["default_account"] = args.account
        if hasattr(args, "partition") and args.partition:
            kwargs["default_partition"] = args.partition

        # Merge in any extra_kwargs (these take precedence)
        kwargs.update(extra_kwargs)

        return cls(**kwargs)

    def _prepare_packaging_strategy(
        self,
        task_func: SlurmTask,
        packaging_config: Optional[Dict[str, Any]],
    ) -> Any:
        """Prepare the packaging strategy for a task submission.

        Determines the effective packaging configuration by checking:
        1. Provided packaging_config parameter
        2. Task's packaging configuration
        3. Cluster's default packaging
        4. Legacy Slurmfile packaging defaults

        Args:
            task_func: The task to package
            packaging_config: Override packaging configuration

        Returns:
            The prepared packaging strategy
        """
        from .decorators import _parse_packaging_config

        effective_packaging_config: Optional[Dict[str, Any]] = packaging_config

        # Check if there's a pre-built image for this task (takes priority over all other packaging)
        prebuilt_images = getattr(self, "_prebuilt_dependency_images", None)
        if prebuilt_images:
            func = task_func.func
            task_name = f"{func.__module__}.{func.__qualname__}"
            logger.debug(
                f"Checking pre-built images for {task_name}. Available: {list(prebuilt_images.keys())}"
            )
            if task_name in prebuilt_images:
                prebuilt_image = prebuilt_images[task_name]
                logger.info(f"Using pre-built image for {task_name}: {prebuilt_image}")
                effective_packaging_config = {
                    "type": "container",
                    "image": prebuilt_image,
                    "push": False,
                }
            else:
                logger.debug(f"No pre-built image found for {task_name}")

        # Use provided packaging_config, else task's packaging, else cluster defaults
        if effective_packaging_config is None:
            # Check if task has packaging configuration
            task_packaging = task_func.packaging
            # Skip task packaging if it's "auto" or "inherit" to allow cluster defaults to take precedence
            # This allows workflows to override child task packaging:
            # - "auto": use whatever the cluster/workflow default is
            # - "inherit": for container workflows, reuse parent's container image
            #              for wheel workflows, use inherit strategy with parent_job_dir
            if task_packaging and task_packaging.get("type") not in (
                "auto",
                "inherit",
                None,
            ):
                effective_packaging_config = task_packaging
            else:
                # Use cluster default_packaging (new string-based system)
                if self.default_packaging:
                    # Start with cluster-level packaging defaults
                    merged_kwargs = dict(self.default_packaging_kwargs)
                    # Overlay task-specific packaging_* kwargs
                    if task_packaging:
                        task_packaging_kwargs = {
                            k: v for k, v in task_packaging.items() if k != "type"
                        }
                        merged_kwargs.update(task_packaging_kwargs)
                    effective_packaging_config = _parse_packaging_config(
                        self.default_packaging, merged_kwargs
                    )
                else:
                    # Fall back to old Slurmfile packaging_defaults for compatibility
                    effective_packaging_config = getattr(
                        self, "packaging_defaults", None
                    )
                    # If still using auto from task, resolve it now
                    if effective_packaging_config is None and task_packaging:
                        effective_packaging_config = task_packaging

        logger.debug("Effective packaging config: %s", effective_packaging_config)

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

    def _setup_job_directory(
        self, task_func: SlurmTask, task_defaults: Dict[str, Any]
    ) -> tuple[str, str, str, str]:
        """Setup job directory structure and return identifiers.

        Args:
            task_func: The task being submitted
            task_defaults: Task default options

        Returns:
            Tuple of (pre_submission_id, sanitized_task_name, target_job_dir, timestamp)
        """
        # Generate timestamp and unique ID for hierarchical structure
        timestamp, unique_id = _generate_timestamp_id()
        pre_submission_id = f"{timestamp}_{unique_id}"

        # Get task name and sanitize it
        task_name = task_defaults.get("job_name", task_func.func.__name__)
        sanitized_task_name = _sanitize_task_name(task_name)

        resolved_job_base_dir = getattr(self.backend, "job_base_dir", None)
        if resolved_job_base_dir is None:
            import tempfile

            resolved_job_base_dir = os.path.join(tempfile.gettempdir(), "slurm_jobs")
            try:
                os.makedirs(resolved_job_base_dir, exist_ok=True)
            except (OSError, IOError) as e:
                logger.debug(f"Failed to create job base directory: {e}")

        # Check if we're in a workflow context for nested structure
        from .context import _get_active_context
        from .workflow import WorkflowContext

        ctx = _get_active_context()
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

        return pre_submission_id, sanitized_task_name, target_job_dir, timestamp

    def _merge_sbatch_options(
        self,
        task_defaults: Dict[str, Any],
        submit_overrides: Dict[str, Any],
        pre_submission_id: str,
        target_job_dir: str,
    ) -> tuple[Dict[str, Any], str, str]:
        """Merge SBATCH options with proper precedence.

        Precedence order (lowest to highest):
        1. Cluster defaults (default_account, default_partition)
        2. Slurmfile [submit] defaults (for backward compatibility)
        3. Task decorator defaults
        4. Runtime overrides

        Args:
            task_defaults: Options from task decorator
            submit_overrides: Options from submit() call
            pre_submission_id: Job submission ID
            target_job_dir: Job directory path

        Returns:
            Tuple of (effective_sbatch_options, stdout_path, stderr_path)
        """
        effective_sbatch_options: Dict[str, Any] = {}

        # Add cluster-wide defaults (new string-based API)
        if self.default_account:
            effective_sbatch_options["account"] = self.default_account
        if self.default_partition:
            effective_sbatch_options["partition"] = self.default_partition

        # Add Slurmfile submit defaults if available (for backward compatibility)
        slurmfile_submit_defaults = getattr(self, "submit_defaults", None)
        if slurmfile_submit_defaults and isinstance(slurmfile_submit_defaults, dict):
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

        return effective_sbatch_options, stdout_path, stderr_path

    def _render_and_submit_to_backend(
        self,
        task_func: SlurmTask,
        func_to_render: Callable,
        args: tuple,
        kwargs: dict,
        task_defaults: Dict[str, Any],
        submit_overrides: Dict[str, Any],
        packaging_strategy: Any,
        pre_submission_id: str,
        target_job_dir: str,
        effective_sbatch_options: Dict[str, Any],
    ) -> str:
        """Render job script and submit to backend.

        Args:
            task_func: The SlurmTask being submitted
            func_to_render: The actual function to render in the script
            args: Task arguments
            kwargs: Task keyword arguments
            task_defaults: Task default options
            submit_overrides: Submit-time option overrides
            packaging_strategy: The packaging strategy to use
            pre_submission_id: Job submission ID
            target_job_dir: Job directory path
            effective_sbatch_options: Merged SBATCH options

        Returns:
            The job ID as a string
        """
        from .rendering import render_job_script

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
        logger.debug("[%s] submit_partition: %s", pre_submission_id, submit_partition)
        submission_message = (
            f"[{pre_submission_id}] Submitting job via {self.backend_type} backend"
        )
        try:
            logger.debug(submission_message)
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
            isinstance(job_submission_result, tuple) and len(job_submission_result) == 2
        ):
            job_id, _ = job_submission_result
        else:
            raise TypeError(
                f"Unexpected return type from backend.submit_job: {type(job_submission_result)}"
            )

        return job_id

    def _create_job_object(
        self,
        job_id: str,
        task_func: SlurmTask,
        args: tuple,
        kwargs: dict,
        target_job_dir: str,
        pre_submission_id: str,
        effective_sbatch_options: Dict[str, Any],
        stdout_path: str,
        stderr_path: str,
    ) -> Job:
        """Create a Job object from submission results.

        Args:
            job_id: The SLURM job ID
            task_func: The task being submitted
            args: Task arguments
            kwargs: Task keyword arguments
            target_job_dir: Job directory path
            pre_submission_id: Job submission ID
            effective_sbatch_options: Merged SBATCH options
            stdout_path: Path to stdout file
            stderr_path: Path to stderr file

        Returns:
            The created Job object
        """
        return Job(
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

    def _write_job_metadata(
        self,
        job_id: str,
        pre_submission_id: str,
        sanitized_task_name: str,
        timestamp: str,
        target_job_dir: str,
        task_func: SlurmTask,
    ) -> None:
        """Write job metadata file and emit workflow callbacks.

        Args:
            job_id: The SLURM job ID
            pre_submission_id: Job submission ID
            sanitized_task_name: Sanitized task name
            timestamp: Job timestamp
            target_job_dir: Job directory path
            task_func: The task being submitted
        """
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
        from .context import _get_active_context
        from .workflow import WorkflowContext

        ctx = _get_active_context()
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
                logger.warning(f"Error calling workflow task submitted callbacks: {e}")
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
                self.backend.write_file(metadata_path, json.dumps(metadata, indent=2))
            else:
                # For local backends, write directly
                os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
                with open(metadata_path, "w") as f:
                    json.dump(metadata, f, indent=2)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to write metadata.json: %s", pre_submission_id, exc
            )

    def _handle_workflow_slurmfile(
        self,
        task_func: SlurmTask,
        pre_submission_id: str,
        target_job_dir: str,
    ) -> None:
        """Handle workflow Slurmfile upload for nested workflow execution.

        Args:
            task_func: The task being submitted
            pre_submission_id: Job submission ID
            target_job_dir: Job directory path
        """
        # Upload Slurmfile for workflow support
        is_workflow = getattr(task_func, "_is_workflow", False)
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

            env_name = getattr(self, "env_name", None) or "default"
            try:
                if slurmfile_path and os.path.exists(slurmfile_path):
                    with open(slurmfile_path, "r", encoding="utf-8") as f:
                        slurmfile_content = f.read()
                else:
                    slurmfile_content = self._render_workflow_slurmfile(env_name)

                # Determine the hostname to use inside the cluster.
                try:
                    result = self.backend.execute_command("hostname")
                    internal_hostname = result.strip() or "localhost"
                except Exception:
                    internal_hostname = "localhost"

                section_header = f"[{env_name}.cluster.backend_config]"
                lines = slurmfile_content.splitlines()
                new_lines: List[str] = []
                in_section = False
                hostname_set = False
                port_set = False

                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith("[") and stripped.endswith("]"):
                        if in_section:
                            if not hostname_set:
                                new_lines.append(f'hostname = "{internal_hostname}"')
                            if not port_set:
                                new_lines.append("port = 22")
                        in_section = stripped == section_header
                        hostname_set = False
                        port_set = False
                        new_lines.append(line)
                        continue

                    if in_section:
                        if stripped.startswith("hostname"):
                            new_lines.append(f'hostname = "{internal_hostname}"')
                            hostname_set = True
                            continue
                        if stripped.startswith("port"):
                            new_lines.append("port = 22")
                            port_set = True
                            continue

                    new_lines.append(line)

                if in_section:
                    if not hostname_set:
                        new_lines.append(f'hostname = "{internal_hostname}"')
                    if not port_set:
                        new_lines.append("port = 22")

                newline_suffix = "\n" if slurmfile_content.endswith("\n") else ""
                modified_content = "\n".join(new_lines) + newline_suffix

                logger.debug(
                    "[%s] Prepared Slurmfile for workflow: hostname=%s, port=22",
                    pre_submission_id,
                    internal_hostname,
                )

                remote_slurmfile_path = os.path.join(target_job_dir, "Slurmfile.toml")
                if hasattr(self.backend, "_upload_string_to_file"):
                    self.backend._upload_string_to_file(
                        modified_content, remote_slurmfile_path
                    )
                    logger.debug(
                        "[%s] Uploaded workflow Slurmfile to %s",
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
                    "[%s] Failed to upload workflow Slurmfile: %s",
                    pre_submission_id,
                    exc,
                )

    def _finalize_job_submission(
        self,
        job: Job,
        job_id: str,
        pre_submission_id: str,
        target_job_dir: str,
        effective_sbatch_options: Dict[str, Any],
    ) -> Job:
        """Finalize job submission with callbacks and polling.

        Args:
            job: The Job object
            job_id: The SLURM job ID
            pre_submission_id: Job submission ID
            target_job_dir: Job directory path
            effective_sbatch_options: Merged SBATCH options

        Returns:
            The Job object
        """
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

    def submit(
        self,
        task_func: SlurmTask,
        packaging_config: Optional[Dict[str, Any]] = None,
        after: Optional[Union[Job, List[Job]]] = None,
        **sbatch_options,
    ) -> Union[Callable[..., Job], SubmittableWorkflow]:
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

            # Prepare packaging strategy
            packaging_strategy = self._prepare_packaging_strategy(
                task_func, packaging_config
            )

            # Setup job directory and get identifiers
            pre_submission_id, sanitized_task_name, target_job_dir, timestamp = (
                self._setup_job_directory(task_func, task_defaults)
            )

            # Merge SBATCH options with proper precedence
            effective_sbatch_options, stdout_path, stderr_path = (
                self._merge_sbatch_options(
                    task_defaults, submit_overrides, pre_submission_id, target_job_dir
                )
            )

            # Render script and submit to backend
            job_id = self._render_and_submit_to_backend(
                task_func,
                func_to_render,
                args,
                kwargs,
                task_defaults,
                submit_overrides,
                packaging_strategy,
                pre_submission_id,
                target_job_dir,
                effective_sbatch_options,
            )

            # Create Job object
            job = self._create_job_object(
                job_id,
                task_func,
                args,
                kwargs,
                target_job_dir,
                pre_submission_id,
                effective_sbatch_options,
                stdout_path,
                stderr_path,
            )

            # Write job metadata and emit workflow callbacks
            self._write_job_metadata(
                job_id,
                pre_submission_id,
                sanitized_task_name,
                timestamp,
                target_job_dir,
                task_func,
            )

            # Handle workflow Slurmfile upload
            self._handle_workflow_slurmfile(
                task_func,
                pre_submission_id,
                target_job_dir,
            )

            # Finalize submission and return job
            return self._finalize_job_submission(
                job,
                job_id,
                pre_submission_id,
                target_job_dir,
                effective_sbatch_options,
            )

        # For workflows, return SubmittableWorkflow to enable with_dependencies()
        is_workflow = (
            getattr(task_func, "_is_workflow", False)
            or getattr(task_func, "is_workflow", False)
            or (
                hasattr(task_func, "sbatch_options")
                and task_func.sbatch_options.get("is_workflow", False)
            )
        )
        if is_workflow:
            # Get container dependencies from the task (set via .with_dependencies())
            container_deps = getattr(task_func, "_container_dependencies", [])
            return SubmittableWorkflow(
                cluster=self,
                submitter=submitter,
                task_func=task_func,
                packaging_config=packaging_config,
                container_dependencies=container_deps,
            )

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
        except Exception as e:
            logger.debug(f"Failed to extract job metadata for {job_id}: {e}")

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
        """Enter cluster context for task execution.

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
        from .context import _set_active_context

        self._context_token = _set_active_context(self)
        return self

    def __exit__(self, *args) -> bool:
        """Exit cluster context - restore previous context.

        Args:
            *args: Exception info (exc_type, exc_value, traceback) if an
                exception occurred, or (None, None, None) for normal exit.

        Returns:
            False to propagate any exception that occurred.
        """
        from .context import _reset_active_context

        if hasattr(self, "_context_token"):
            _reset_active_context(self._context_token)
            delattr(self, "_context_token")
        return False

    def diagnose(self) -> Dict[str, Any]:
        """Run cluster diagnostics and return a summary of the cluster state.

        This is a debug helper that tests connectivity, retrieves cluster information,
        and provides a comprehensive summary of the cluster configuration and status.
        Useful for troubleshooting connection issues or understanding cluster availability.

        Returns:
            Dictionary containing diagnostic information:
            - "backend_type": Type of backend (e.g., "ssh", "local")
            - "backend_config": Backend configuration details
            - "connectivity": Connectivity test result
            - "cluster_info": Cluster configuration (partitions, nodes)
            - "queue": Current job queue
            - "errors": List of any errors encountered during diagnostics

        Examples:
            >>> diag = cluster.diagnose()
            >>> print(f"Backend: {diag['backend_type']}")
            >>> print(f"Connected: {diag['connectivity']['success']}")
            >>> if diag['errors']:
            ...     print(f"Errors: {diag['errors']}")

            Pretty-print full diagnostics:

            >>> import json
            >>> diag = cluster.diagnose()
            >>> print(json.dumps(diag, indent=2))

        Note:
            This method attempts to gather as much information as possible, even if
            some operations fail. Check the "errors" field for any issues encountered.
        """
        diag: Dict[str, Any] = {
            "backend_type": self.backend_type,
            "backend_config": {},
            "connectivity": {"success": False, "message": ""},
            "cluster_info": {},
            "queue": [],
            "errors": [],
        }

        # Gather backend configuration
        try:
            if hasattr(self.backend, "hostname"):
                diag["backend_config"]["hostname"] = self.backend.hostname
            if hasattr(self.backend, "username"):
                diag["backend_config"]["username"] = self.backend.username
            if hasattr(self.backend, "job_base_dir"):
                diag["backend_config"]["job_base_dir"] = self.backend.job_base_dir

            diag["backend_config"]["default_packaging"] = self.default_packaging
            diag["backend_config"]["default_account"] = self.default_account
            diag["backend_config"]["default_partition"] = self.default_partition
        except Exception as e:
            diag["errors"].append(f"Error gathering backend config: {e}")

        # Test connectivity
        try:
            from .api.ssh import SSHCommandBackend

            if isinstance(self.backend, SSHCommandBackend):
                # Test SSH connection by running a simple command
                test_output = self.backend.execute_command("echo 'slurm-sdk-test'")
                if "slurm-sdk-test" in test_output:
                    diag["connectivity"]["success"] = True
                    diag["connectivity"]["message"] = "SSH connection successful"
                else:
                    diag["connectivity"]["success"] = False
                    diag["connectivity"]["message"] = (
                        "SSH connection failed: unexpected output"
                    )
            else:
                # For local backends, just check if backend is available
                diag["connectivity"]["success"] = True
                diag["connectivity"]["message"] = "Local backend available"
        except Exception as e:
            diag["connectivity"]["success"] = False
            diag["connectivity"]["message"] = f"Connectivity test failed: {e}"
            diag["errors"].append(f"Connectivity error: {e}")

        # Get cluster information
        try:
            cluster_info = self.backend.get_cluster_info()
            diag["cluster_info"] = cluster_info
        except Exception as e:
            diag["errors"].append(f"Error getting cluster info: {e}")

        # Get queue information
        try:
            queue = self.backend.get_queue()
            diag["queue"] = queue
            diag["queue_summary"] = {
                "total_jobs": len(queue),
                "by_state": {},
            }
            # Count jobs by state
            for job in queue:
                state = job.get("STATE", "UNKNOWN")
                diag["queue_summary"]["by_state"][state] = (
                    diag["queue_summary"]["by_state"].get(state, 0) + 1
                )
        except Exception as e:
            diag["errors"].append(f"Error getting queue: {e}")

        # Test SLURM command availability
        try:
            from .api.ssh import SSHCommandBackend

            if isinstance(self.backend, SSHCommandBackend):
                # Test if sinfo is available
                self.backend.execute_command("which sbatch")
                diag["slurm_commands"] = {"sbatch": "available"}
            else:
                import subprocess

                result = subprocess.run(
                    ["which", "sbatch"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    diag["slurm_commands"] = {"sbatch": "available"}
                else:
                    diag["slurm_commands"] = {"sbatch": "not found"}
        except Exception as e:
            diag["errors"].append(f"Error checking SLURM commands: {e}")

        return diag
