"""
This module provides the Cluster class for submitting and managing jobs on SLURM clusters.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .api.base import BackendBase
import argparse
import logging
import os
import re
import threading

from .job import Job
from .api import create_backend
from .callbacks import (
    BaseCallback,
    ExecutionLocus,
)
from .task import SlurmTask, normalize_sbatch_options
from .config import load_environment
from .errors import SlurmfileInvalidError

# Internal modules extracted from this file for maintainability
from ._polling import (
    _JobStatusPoller,
    dispatch_callbacks,
    maybe_start_job_poller,
    emit_completed_context,
    on_poller_finished,
)
from ._submission import (
    prepare_packaging_strategy,
    setup_job_directory,
    merge_sbatch_options,
    render_and_submit_to_backend,
    create_job_object,
    finalize_job_submission,
)
from ._workflow import (
    SubmittableWorkflow,
    render_workflow_slurmfile,
    write_job_metadata,
    handle_workflow_slurmfile,
)


logger = logging.getLogger(__name__)


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
        self._job_pollers_lock = threading.Lock()

    @classmethod
    def from_backend(
        cls,
        backend: "BackendBase",
        *,
        backend_type: str = "local",
        callbacks: Optional[List[BaseCallback]] = None,
        default_packaging: Optional[str] = None,
        default_account: Optional[str] = None,
        default_partition: Optional[str] = None,
        default_packaging_kwargs: Optional[Dict[str, Any]] = None,
    ) -> "Cluster":
        """Create a Cluster with a pre-constructed backend.

        Use this when you already have a backend instance and want to skip
        the connection setup that ``__init__`` performs.  This is the
        recommended way to create Cluster instances in tests.

        Args:
            backend: A pre-constructed backend instance.
            backend_type: Label for the backend (e.g., "local", "ssh").
            callbacks: Lifecycle callback instances.
            default_packaging: Default packaging strategy name.
            default_account: Default SLURM account.
            default_partition: Default SLURM partition.
            default_packaging_kwargs: Extra kwargs forwarded to packaging strategies.

        Returns:
            A fully initialised Cluster that uses the given backend.
        """
        cluster = cls.__new__(cls)
        cluster.backend_type = backend_type
        cluster.backend = backend
        cluster.callbacks = callbacks or []
        cluster.default_packaging = default_packaging
        cluster.default_account = default_account
        cluster.default_partition = default_partition
        cluster.default_packaging_kwargs = default_packaging_kwargs or {}
        cluster._backend_kwargs = {}
        cluster._job_pollers = {}
        cluster._job_pollers_lock = threading.Lock()
        return cluster

    # -------------------------------------------------------------------
    # Workflow Slurmfile rendering (delegates to _workflow module)
    # -------------------------------------------------------------------

    def _render_workflow_slurmfile(self, env_name: str) -> str:
        """Render a minimal Slurmfile for nested workflow execution."""
        return render_workflow_slurmfile(self, env_name)

    # -------------------------------------------------------------------
    # Cluster construction from config
    # -------------------------------------------------------------------

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
    def from_file(cls, config_path: str, **extra_kwargs: Any) -> "Cluster":
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
    def add_argparse_args(parser: argparse.ArgumentParser) -> None:
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
            "--host-key-policy",
            choices=["auto", "warn", "reject"],
            default="warn",
            help="SSH host key verification policy: auto (accept all), warn (log warning), reject (strict). Default: warn",
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
    def from_args(cls, args: argparse.Namespace, **extra_kwargs: Any) -> "Cluster":
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
        if hasattr(args, "host_key_policy") and args.host_key_policy:
            kwargs["host_key_policy"] = args.host_key_policy
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

    # -------------------------------------------------------------------
    # Submission pipeline (delegates to _submission module)
    # -------------------------------------------------------------------

    def _prepare_packaging_strategy(
        self,
        task_func: SlurmTask,
        packaging_config: Optional[Dict[str, Any]],
    ) -> Any:
        """Prepare the packaging strategy for a task submission."""
        return prepare_packaging_strategy(self, task_func, packaging_config)

    def _setup_job_directory(
        self, task_func: SlurmTask, task_defaults: Dict[str, Any]
    ) -> tuple[str, str, str, str]:
        """Setup job directory structure and return identifiers."""
        return setup_job_directory(self, task_func, task_defaults)

    def _merge_sbatch_options(
        self,
        task_defaults: Dict[str, Any],
        submit_overrides: Dict[str, Any],
        pre_submission_id: str,
        target_job_dir: str,
    ) -> tuple[Dict[str, Any], str, str]:
        """Merge SBATCH options with proper precedence."""
        return merge_sbatch_options(
            self, task_defaults, submit_overrides, pre_submission_id, target_job_dir
        )

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
        """Render job script and submit to backend."""
        return render_and_submit_to_backend(
            self,
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
        """Create a Job object from submission results."""
        return create_job_object(
            self,
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

    # -------------------------------------------------------------------
    # Workflow metadata (delegates to _workflow module)
    # -------------------------------------------------------------------

    def _write_job_metadata(
        self,
        job_id: str,
        pre_submission_id: str,
        sanitized_task_name: str,
        timestamp: str,
        target_job_dir: str,
        task_func: SlurmTask,
    ) -> None:
        """Write job metadata file and emit workflow callbacks."""
        write_job_metadata(
            self,
            job_id,
            pre_submission_id,
            sanitized_task_name,
            timestamp,
            target_job_dir,
            task_func,
        )

    def _handle_workflow_slurmfile(
        self,
        task_func: SlurmTask,
        pre_submission_id: str,
        target_job_dir: str,
    ) -> None:
        """Handle workflow Slurmfile upload for nested workflow execution."""
        handle_workflow_slurmfile(self, task_func, pre_submission_id, target_job_dir)

    def _finalize_job_submission(
        self,
        job: Job,
        job_id: str,
        pre_submission_id: str,
        target_job_dir: str,
        effective_sbatch_options: Dict[str, Any],
    ) -> Job:
        """Finalize job submission with callbacks and polling."""
        return finalize_job_submission(
            self,
            job,
            job_id,
            pre_submission_id,
            target_job_dir,
            effective_sbatch_options,
        )

    # -------------------------------------------------------------------
    # Job submission (public API)
    # -------------------------------------------------------------------

    def submit(
        self,
        task_func: SlurmTask,
        packaging_config: Optional[Dict[str, Any]] = None,
        after: Optional[Union[Job, List[Job]]] = None,
        **sbatch_options: Any,
    ) -> Union[Callable[..., Job], "SubmittableWorkflow"]:
        """Prepare a task for submission to the cluster.

        This method implements a two-phase submission pattern: it returns a callable
        ("submitter") that, when invoked with your task's runtime arguments, performs
        the actual job submission. This separation allows you to configure SBATCH
        parameters and packaging once, then submit multiple jobs with different arguments.

        Two-Phase Submission Pattern:

        ```mermaid
        flowchart LR
            subgraph Phase1["Phase 1: Prepare"]
                A["cluster.submit(task)"] --> B[Configure packaging]
                B --> C[Set SBATCH options]
                C --> D["Return submitter"]
            end

            subgraph Phase2["Phase 2: Execute"]
                D --> E["submitter(args)"]
                E --> F[Package code]
                F --> G[Render script]
                G --> H[Upload & sbatch]
                H --> I[Return Job]
            end
        ```

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

    # -------------------------------------------------------------------
    # Callback dispatch and polling (delegates to _polling module)
    # -------------------------------------------------------------------

    def _dispatch_callbacks(self, method_name: str, context: Any) -> None:
        """Dispatch a lifecycle event to all registered callbacks."""
        dispatch_callbacks(self.callbacks, method_name, context)

    def _maybe_start_job_poller(self, job: Job) -> None:
        """Start a background poller for the job if any callback requests polling."""
        maybe_start_job_poller(self, job)

    def _emit_completed_context(
        self,
        job: Job,
        status: Dict[str, Any],
        timestamp: Optional[float],
        *,
        error_payload: Optional[Dict[str, Optional[str]]] = None,
        emitted_by: ExecutionLocus = ExecutionLocus.CLIENT,
    ) -> None:
        """Emit the CompletedContext callback for a finished job."""
        emit_completed_context(
            self.callbacks,
            job,
            status,
            timestamp,
            error_payload=error_payload,
            emitted_by=emitted_by,
        )

    def _on_poller_finished(self, job_id: str) -> None:
        """Remove a finished poller from the tracking dict."""
        on_poller_finished(
            getattr(self, "_job_pollers", None),
            getattr(self, "_job_pollers_lock", None),
            job_id,
        )

    # -------------------------------------------------------------------
    # Job retrieval and query (public API)
    # -------------------------------------------------------------------

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
                # Match pre_submission_id format: {timestamp}_{unique_id} (e.g., 20260118_082719_cc6a2876)
                m = re.match(r"^slurm_([A-Za-z0-9_]+)\.out$", base_name)
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

    # -------------------------------------------------------------------
    # Context manager
    # -------------------------------------------------------------------

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
        if hasattr(self, "backend"):
            self.backend.close()
        return False

    # -------------------------------------------------------------------
    # Diagnostics
    # -------------------------------------------------------------------

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
