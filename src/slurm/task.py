"""Task module for defining Slurm tasks."""

import functools
from typing import Any, Callable, Dict, TYPE_CHECKING

# Add this block for type hinting Cluster without causing circular import at runtime
if TYPE_CHECKING:
    from .cluster import Cluster


def normalize_sbatch_key(key: str) -> str:
    """Normalize SBATCH keyword-style input to our internal underscore form."""

    normalized = str(key or "").strip().lower().replace("-", "_")
    if normalized == "name":
        return "job_name"
    return normalized


def normalize_sbatch_options(options: Dict[str, Any] | None) -> Dict[str, Any]:
    """Return a copy of the provided mapping with normalized SBATCH keys."""

    normalized: Dict[str, Any] = {}
    if not options:
        return normalized

    for raw_key, value in options.items():
        if raw_key is None:
            continue
        key = normalize_sbatch_key(raw_key)
        normalized[key] = value

    if "memory" in normalized and "mem" not in normalized:
        normalized["mem"] = normalized.pop("memory")
    return normalized


class SlurmTask:
    """A wrapper around a Python function that can be executed on a Slurm cluster.

    SlurmTask instances are typically created via the `@task` decorator rather than
    directly. They encapsulate a function along with its SBATCH resource requirements
    and packaging configuration.

    The task can be called like a regular function (runs locally) or submitted to a
    cluster for remote execution.

    Attributes:
        func: The wrapped Python function.
        sbatch_options: SBATCH directive parameters (normalized with underscores).
        packaging: Packaging configuration dictionary.

    Examples:
        Created via decorator (recommended):

            >>> @task(time="01:00:00", cpus_per_task=4)
            ... def my_function(x: int) -> int:
            ...     return x * 2
            >>> isinstance(my_function, SlurmTask)
            True

        Direct instantiation (advanced):

            >>> def raw_function(x):
            ...     return x * 2
            >>> task = SlurmTask(
            ...     raw_function,
            ...     sbatch_options={"time": "01:00:00", "cpus_per_task": 4}
            ... )
    """

    def __init__(
        self,
        func: Callable,
        sbatch_options: Dict[str, Any] | None = None,
        packaging: Dict[str, Any] | None = None,
        **slurm_options,
    ):
        """Initialize a SlurmTask (typically done via @task decorator).

        Args:
            func: The Python function to execute remotely.
            sbatch_options: SBATCH directive dictionary (will be normalized).
            packaging: Packaging configuration. Defaults to
                `{"type": "wheel", "build_tool": "uv"}`.
            **slurm_options: Additional options (currently unused, reserved for
                future extensions).
        """
        self.func = func
        self.sbatch_options = normalize_sbatch_options(sbatch_options)
        self.packaging = packaging or {"type": "wheel", "build_tool": "uv"}
        # Copy function metadata
        functools.update_wrapper(self, func)

        self.slurm_options = slurm_options

    def __call__(self, *args, **kwargs):
        """
        Execute the task locally.

        Args:
            *args: Positional arguments to pass to the task function.
            **kwargs: Keyword arguments to pass to the task function.

        Returns:
            The result of the task function.
        """
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"SlurmTask(name={self.sbatch_options.get('job_name', self.func.__name__)!r})"

    def __str__(self) -> str:
        return self.sbatch_options.get("job_name", self.func.__name__)

    def submit(self, *, cluster: "Cluster", **kwargs) -> Callable:
        """Prepare this task for submission to a cluster.

        This is a convenience method equivalent to `cluster.submit(self, **kwargs)`.
        It's useful for a more object-oriented style where the task "knows" how
        to submit itself.

        Args:
            cluster: The Cluster instance to submit to. Required.
            **kwargs: SBATCH parameter overrides and packaging configuration.
                - SBATCH overrides: `account`, `partition`, `time`, etc.
                - Packaging override: `packaging={"type": "container", ...}`
                - Runtime arguments for the task function are passed when
                  calling the returned submitter, NOT here.

        Returns:
            A submitter callable with signature `(*args, **kwargs) -> Job`.
            Call this with your task function's arguments to submit the job.

        Raises:
            TypeError: If cluster is not a Cluster instance.

        Examples:
            Using task.submit():

                >>> @task(time="01:00:00")
                ... def process(data_file: str):
                ...     return process_file(data_file)
                >>> submitter = process.submit(cluster=my_cluster, partition="gpu")
                >>> job = submitter("data.csv")  # Runtime args passed here

            Equivalent using cluster.submit():

                >>> submitter = my_cluster.submit(process, partition="gpu")
                >>> job = submitter("data.csv")

        Note:
            The cluster must be passed as a keyword argument. This prevents
            accidental confusion between cluster references and task arguments.
        """
        # Import at runtime to avoid circular import at module load time
        from .cluster import Cluster as _Cluster

        if not isinstance(cluster, _Cluster):
            raise TypeError(
                f"Expected cluster to be a Cluster object, but got {type(cluster)}"
            )

        # Allow per-submit packaging override while keeping SBATCH overrides in kwargs
        packaging_override = kwargs.pop("packaging", None)
        effective_packaging_config = (
            packaging_override if packaging_override is not None else self.packaging
        )

        submitter_func = cluster.submit(
            self,
            packaging_config=effective_packaging_config,
            **kwargs,
        )

        return submitter_func
