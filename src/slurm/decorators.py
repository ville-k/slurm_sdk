from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    TypeVar,
    TYPE_CHECKING,
    Union,
    overload,
    ParamSpec,
)

from .task import SlurmTask, normalize_sbatch_options

# Type variables for generic signatures
P = ParamSpec("P")  # For parameter types
R = TypeVar("R")  # For return types

if TYPE_CHECKING:
    from .job import Job


# TYPE_CHECKING overloads for proper type hints
if TYPE_CHECKING:
    # Overload for @workflow without arguments
    @overload
    def workflow(func: Callable[P, R]) -> Callable[P, "Job[R]"]: ...

    # Overload for @workflow(time=..., ...)
    @overload
    def workflow(
        func: None = None,
        *,
        time: str = "01:00:00",
        **sbatch_kwargs: Any,
    ) -> Callable[[Callable[P, R]], Callable[P, "Job[R]"]]: ...


def workflow(
    func: Optional[Callable[..., Any]] = None,
    *,
    time: str = "01:00:00",
    **sbatch_kwargs: Any,
):
    """Decorator for workflow orchestrator tasks.

    Workflows are special tasks that can submit other tasks. They automatically
    receive a WorkflowContext parameter that provides access to the cluster,
    shared directory, and task management utilities.

    The @workflow decorator is essentially @task with workflow-specific defaults
    and a flag marking it as a workflow orchestrator.

    Args:
        func: The function to decorate. Typically not specified directly.
        time: Default time limit for the orchestrator (default: "01:00:00").
        **sbatch_kwargs: SBATCH directive parameters (same as @task).

    Returns:
        SlurmTask instance marked as a workflow.

    Examples:
        Basic workflow with context injection:

            >>> from slurm.workflow import WorkflowContext
            >>> @workflow(time="02:00:00")
            ... def my_workflow(data: list[str], ctx: WorkflowContext):
            ...     # Context is automatically injected by runner
            ...     jobs = [process(item) for item in data]
            ...     return [job.get_result() for job in jobs]

        Workflow using shared directory:

            >>> @workflow(time="04:00:00", mem="8G")
            ... def training_workflow(config: dict, ctx: WorkflowContext):
            ...     # Save config to shared directory
            ...     config_path = ctx.shared_dir / "config.pkl"
            ...     with open(config_path, "wb") as f:
            ...         pickle.dump(config, f)
            ...
            ...     # Submit tasks that can access shared directory
            ...     jobs = train.map([{"lr": lr} for lr in config["learning_rates"]])
            ...     return jobs.get_results()
    """

    # Use the @task decorator with workflow-specific settings
    def decorator(inner: Callable[..., Any]) -> SlurmTask:
        task_instance = task(inner, time=time, **sbatch_kwargs)
        # Mark this task as a workflow
        task_instance._is_workflow = True
        return task_instance

    if callable(func):
        return decorator(func)

    return decorator


# TYPE_CHECKING overloads for task decorator
if TYPE_CHECKING:
    # Overload for @task without arguments
    @overload
    def task(func: Callable[P, R]) -> Callable[P, "Job[R]"]: ...

    # Overload for @task(time=..., ...)
    @overload
    def task(
        func: None = None,
        *,
        packaging: str = "auto",
        **sbatch_kwargs: Any,
    ) -> Callable[[Callable[P, R]], Callable[P, "Job[R]"]]: ...


def task(
    func: Optional[Callable[..., Any]] = None,
    *,
    packaging: Union[str, Dict[str, Any]] = "auto",
    **sbatch_kwargs: Any,
):
    """Decorator for defining a Python function as a Slurm task.

    This decorator wraps your function in a SlurmTask object, capturing SBATCH
    directives and packaging configuration. The decorated function can still be
    called locally for testing, or submitted to a cluster via `cluster.submit()`.

    Can be used as a decorator (@task) or called as a function (task(func, ...))
    for dynamic task creation.

    All keyword arguments (except `packaging` and `packaging_*`) map directly to
    SBATCH directives. Parameter names use underscores (Python style) which are
    automatically converted to dashes (SBATCH style): e.g., `cpus_per_task=4`
    becomes `--cpus-per-task=4`.

    Args:
        func: The function to decorate. Typically not specified directly (decorator
            syntax handles this).
        packaging: Packaging strategy. Specifies how to package and deploy your code
            to the cluster. Common options:
            - `"auto"` (default) - Auto-detect: wheel if pyproject.toml exists, else none
            - `"wheel"` - Build and install a Python wheel
            - `"none"` - Assume code is already available on the cluster
            - `"container:IMAGE:TAG"` - Use existing container image
            - `"container:IMAGE"` - Use existing container (latest tag)
            For building from Dockerfile, use packaging_dockerfile parameter.
        **sbatch_kwargs: SBATCH directive parameters and packaging options. Common directives include:
            - `time`: Wall time limit (e.g., "01:30:00" for 1.5 hours)
            - `cpus_per_task`: CPU cores per task
            - `mem` or `memory`: Memory limit (e.g., "4G")
            - `partition`: Partition/queue name
            - `gpus` or `gres`: GPU resources (e.g., "gpu:1")
            - `account`: Billing account
            - `job_name`: Job name (defaults to function name)

            Packaging options (all optional, prefixed with `packaging_`):
            - `packaging_python_version`: Python version for wheel packaging (e.g., "3.11")
            - `packaging_build_tool`: Build tool to use (e.g., "uv", "pip")
            - `packaging_dockerfile`: Path to Dockerfile for building container
            - `packaging_context`: Docker build context directory (default: ".")
            - `packaging_registry`: Container registry URL
            - `packaging_push`: Whether to push container to registry (bool)
            - `packaging_runtime`: Container runtime (e.g., "docker", "podman")
            - `packaging_platform`: Target platform (e.g., "linux/amd64")
            - `packaging_mounts`: List of volume mounts for container
            - `packaging_srun_args`: Additional srun arguments for container

    Returns:
        SlurmTask instance that wraps the original function. The SlurmTask can be:
        - Called directly like the original function (runs locally)
        - Submitted via `cluster.submit(task)` to run remotely

    Examples:
        Auto-detect packaging (most common):

            >>> @task(time="02:00:00", cpus_per_task=8, mem="16G")
            ... def train_model(data_path: str, epochs: int) -> dict:
            ...     model = load_model()
            ...     return train(model, data_path, epochs)

        Explicit wheel packaging:

            >>> @task(time="01:00:00", packaging="wheel")
            ... def process_data(input_file: str):
            ...     return process(input_file)

        Container from existing image:

            >>> @task(
            ...     time="01:00:00",
            ...     packaging="container:pytorch/pytorch:2.0-cuda11.7"
            ... )
            ... def gpu_task(config: dict):
            ...     return train_with_gpus(config)

        Build container from Dockerfile:

            >>> @task(
            ...     time="01:00:00",
            ...     packaging="container:my-image:latest",
            ...     packaging_dockerfile="path/to/Dockerfile",
            ...     packaging_push=True,
            ...     packaging_registry="registry.example.com"
            ... )
            ... def custom_container_task(data: str):
            ...     return process(data)

        No packaging (code already on cluster):

            >>> @task(time="00:30:00", packaging="none")
            ... def analysis(results_path: str):
            ...     return analyze(results_path)

        Decorated function can be called locally:

            >>> result = train_model("local/data", epochs=5)  # Runs locally

        Or submitted remotely:

            >>> submitter = cluster.submit(train_model, partition="compute")
            >>> job = submitter("s3://data", epochs=100)  # Runs on cluster
            >>> result = job.get_result()

        Using task as a function for dynamic task creation:

            >>> def my_function(x: int) -> int:
            ...     return x * 2
            >>>
            >>> # Create a task dynamically
            >>> my_task = task(my_function, time="00:01:00", mem="2G")
            >>>
            >>> # Now my_task is a SlurmTask that can be submitted
            >>> job = cluster.submit(my_task)(42)
            >>> result = job.get_result()

        Dynamic task creation is useful for:
        - Wrapping existing functions without modifying their source
        - Creating tasks programmatically based on runtime conditions
        - Building task libraries where configuration varies

    Note:
        Parameter name conversion rules:
        - Underscores convert to dashes: `cpus_per_task` → `--cpus-per-task`
        - `memory` is aliased to `mem` for convenience
        - `name` is aliased to `job_name`
    """

    # Extract packaging_* kwargs from sbatch_kwargs
    packaging_kwargs = {}
    sbatch_only = {}
    for key, value in sbatch_kwargs.items():
        if key.startswith("packaging_"):
            # Remove the "packaging_" prefix for the packaging dict
            packaging_kwargs[key[10:]] = value
        else:
            sbatch_only[key] = value

    normalized_kwargs = normalize_sbatch_options(sbatch_only)

    # Build packaging configuration dict from string + kwargs
    effective_packaging = _parse_packaging_config(packaging, packaging_kwargs)

    def decorator(inner: Callable[..., Any]) -> SlurmTask:
        effective_options = dict(normalized_kwargs)
        if not effective_options.get("job_name"):
            effective_options["job_name"] = inner.__name__
        return SlurmTask(
            inner,
            effective_options,
            effective_packaging,
        )

    if callable(func):
        return decorator(func)

    return decorator


def _parse_packaging_config(
    packaging: Union[str, Dict[str, Any]], kwargs: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Parse packaging string and kwargs into a configuration dict.

    Args:
        packaging: Packaging strategy string ("auto", "wheel", "none", "container:image:tag")
                  or dict for backward compatibility
        kwargs: Additional packaging options from packaging_* parameters

    Returns:
        Packaging configuration dict, or None if packaging is None/empty
    """
    if not packaging:
        return None

    # Handle dict for backward compatibility (old API)
    if isinstance(packaging, dict):
        # Dict packaging completely overrides packaging_* kwargs
        # (for backward compatibility - old API didn't have packaging_* kwargs)
        return dict(packaging)

    # Handle string-based packaging (new API)
    config = dict(kwargs)  # Start with additional options

    if packaging == "auto":
        config["type"] = "auto"
    elif packaging == "wheel":
        config["type"] = "wheel"
    elif packaging == "none":
        config["type"] = "none"
    elif packaging == "inherit":
        config["type"] = "inherit"
    elif packaging.startswith("container:"):
        config["type"] = "container"
        # Parse "container:image:tag" or "container:image"
        image_part = packaging[10:]  # Remove "container:" prefix
        if image_part:
            config["image"] = image_part
    else:
        # Assume it's a raw container image reference without "container:" prefix
        # This is a fallback - users should use "container:..." for clarity
        config["type"] = "container"
        config["image"] = packaging

    return config if config else None
