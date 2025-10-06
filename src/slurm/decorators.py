from typing import Any, Callable, Dict, Optional

from .task import SlurmTask, normalize_sbatch_options


def task(
    func: Optional[Callable[..., Any]] = None,
    *,
    packaging: Optional[Dict[str, Any]] = None,
    container_file: Optional[str] = None,
    **sbatch_kwargs: Any,
):
    """Decorator for defining a Python function as a Slurm task.

    This decorator wraps your function in a SlurmTask object, capturing SBATCH
    directives and packaging configuration. The decorated function can still be
    called locally for testing, or submitted to a cluster via `cluster.submit()`.

    Can be used as a decorator (@task) or called as a function (task(func, ...))
    for dynamic task creation.

    All keyword arguments (except `packaging` and `container_file`) map directly to
    SBATCH directives. Parameter names use underscores (Python style) which are
    automatically converted to dashes (SBATCH style): e.g., `cpus_per_task=4`
    becomes `--cpus-per-task=4`.

    Args:
        func: The function to decorate. Typically not specified directly (decorator
            syntax handles this).
        packaging: Packaging configuration dictionary. Specifies how to package and
            deploy your code to the cluster. Common options:
            - `{"type": "wheel"}` - Build and install a Python wheel (default)
            - `{"type": "container", "image": "myimage:tag"}` - Run in a container
            - `{"type": "none"}` - Assume code is already available
        container_file: Path to a Dockerfile for container-based packaging. This is
            a convenience parameter equivalent to
            `packaging={"type": "container", "dockerfile": "path"}`. If both
            `container_file` and `packaging` are specified, `packaging` takes precedence.
        **sbatch_kwargs: SBATCH directive parameters. Common directives include:
            - `time`: Wall time limit (e.g., "01:30:00" for 1.5 hours)
            - `cpus_per_task`: CPU cores per task
            - `mem` or `memory`: Memory limit (e.g., "4G")
            - `partition`: Partition/queue name
            - `gpus` or `gres`: GPU resources (e.g., "gpu:1")
            - `account`: Billing account
            - `job_name`: Job name (defaults to function name)

    Returns:
        SlurmTask instance that wraps the original function. The SlurmTask can be:
        - Called directly like the original function (runs locally)
        - Submitted via `cluster.submit(task)` to run remotely

    Examples:
        Basic task with resource requirements:

            >>> @task(time="02:00:00", cpus_per_task=8, mem="16G")
            ... def train_model(data_path: str, epochs: int) -> dict:
            ...     model = load_model()
            ...     return train(model, data_path, epochs)

        GPU task with custom job name:

            >>> @task(
            ...     time="24:00:00",
            ...     gpus=4,
            ...     partition="gpu",
            ...     job_name="big_training_run"
            ... )
            ... def train_large_model(config: dict):
            ...     return train_with_gpus(config)

        Container-based task with container_file parameter:

            >>> @task(
            ...     time="01:00:00",
            ...     container_file="path/to/train.Dockerfile"
            ... )
            ... def train_model(dataset_path: str):
            ...     return train(dataset_path)

        Container-based task with packaging dict (alternative):

            >>> @task(
            ...     time="01:00:00",
            ...     packaging={"type": "container", "image": "pytorch:latest"}
            ... )
            ... def process_data(input_file: str):
            ...     return process(input_file)

        No-packaging task (code already on cluster):

            >>> @task(
            ...     time="00:30:00",
            ...     packaging={"type": "none"}
            ... )
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

    normalized_kwargs = normalize_sbatch_options(sbatch_kwargs)

    # Resolve packaging configuration
    effective_packaging = packaging
    if effective_packaging is None and container_file is not None:
        # container_file is a convenience shorthand for container packaging
        effective_packaging = {"type": "container", "dockerfile": container_file}

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
