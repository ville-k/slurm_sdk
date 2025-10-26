"""Task module for defining Slurm tasks."""

import functools
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

# Add this block for type hinting Cluster without causing circular import at runtime
if TYPE_CHECKING:
    from .cluster import Cluster


class JobResultPlaceholder:
    """Placeholder for a Job result that will be resolved at runtime.

    When a Job is passed as an argument to a task, we can't pickle the Job object
    itself (it contains threading locks). Instead, we replace it with this placeholder
    that contains just the job ID. The runner will resolve this placeholder by
    fetching the actual result from the job directory.

    Attributes:
        job_id: The Slurm job ID whose result should be loaded
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f"JobResultPlaceholder(job_id={self.job_id!r})"


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

        Raises:
            TypeError: If func is not callable.
        """
        if not callable(func):
            raise TypeError(
                f"func must be callable, got {type(func).__name__}: {func!r}"
            )

        self.func = func
        self.sbatch_options = normalize_sbatch_options(sbatch_options)
        self.packaging = packaging or {"type": "wheel", "build_tool": "uv"}
        # Copy function metadata
        functools.update_wrapper(self, func)

        self.slurm_options = slurm_options

        # Track explicit dependencies set via .after() (before task is called)
        self._pending_dependencies: list = []

    def __call__(self, *args, **kwargs):
        """
        Call the task - returns Job if in cluster context, raises otherwise.

        When called within a `with Cluster(...) as cluster:` context or a @workflow,
        automatically submits the task and returns a Job. For local execution outside
        of a context, use `.unwrapped(*args, **kwargs)` instead.

        Args:
            *args: Positional arguments to pass to the task function.
            **kwargs: Keyword arguments to pass to the task function.

        Returns:
            Job: A Job object representing the submitted task.

        Raises:
            RuntimeError: If called outside of a cluster or workflow context.
                Use `.unwrapped(*args, **kwargs)` for local execution.
        """
        from .context import get_active_context
        from .job import Job

        # Check if we're in a cluster or workflow context
        ctx = get_active_context()
        if ctx is None:
            raise RuntimeError(
                f"@task decorated function '{self.func.__name__}' must be "
                "called within a Cluster context or @workflow.\n"
                f"For local execution, use: {self.func.__name__}.unwrapped(...)\n"
                f"For cluster execution, use: with Cluster.from_env() as cluster: ..."
            )

        # Get the cluster from context (could be Cluster or WorkflowContext)
        from .cluster import Cluster

        if isinstance(ctx, Cluster):
            cluster = ctx
        else:
            # WorkflowContext has .cluster attribute
            cluster = getattr(ctx, "cluster", None)
            if cluster is None:
                raise RuntimeError(
                    f"Context {type(ctx).__name__} does not have a cluster attribute"
                )

        # Extract Job dependencies from arguments (for automatic dependency tracking)
        # Also replace Job objects with placeholders that will be resolved at runtime
        automatic_dependencies = []
        resolved_args = []
        resolved_kwargs = {}

        # Process positional arguments
        for arg in args:
            if isinstance(arg, Job):
                automatic_dependencies.append(arg)
                # Replace Job with a JobResultPlaceholder that will be resolved in the runner
                resolved_args.append(JobResultPlaceholder(arg.id))
            else:
                resolved_args.append(arg)

        # Process keyword arguments
        for key, value in kwargs.items():
            if isinstance(value, Job):
                automatic_dependencies.append(value)
                # Replace Job with a JobResultPlaceholder that will be resolved in the runner
                resolved_kwargs[key] = JobResultPlaceholder(value.id)
            else:
                resolved_kwargs[key] = value

        # Merge explicit dependencies (from .after()) with automatic dependencies
        all_dependencies = self._pending_dependencies + automatic_dependencies

        # Submit the task using the cluster's submit method
        # Pass dependencies if any were found
        submit_kwargs = {}
        if all_dependencies:
            submit_kwargs["after"] = all_dependencies

        submitter = cluster.submit(self, **submit_kwargs)
        # Use resolved args/kwargs (with placeholders instead of Job objects)
        job = submitter(*resolved_args, **resolved_kwargs)

        return job

    @property
    def unwrapped(self):
        """Access the original function for local testing.

        Use this property to call the task function locally without submitting
        to the cluster. This is especially useful for unit testing.

        Returns:
            The original unwrapped function.

        Examples:
            Local testing:

                >>> @task(time="01:00:00")
                ... def process(data: str) -> int:
                ...     return len(data)
                >>> result = process.unwrapped("test")  # Runs locally
                >>> assert result == 4
        """
        return self.func

    def map(self, items: List[Any], max_concurrent: Optional[int] = None):
        """Map task over items, creating an array job.

        This method provides a fluent API for submitting array jobs. Each item
        in the list becomes one task in the array. Items can be single values,
        tuples (unpacked as positional args), or dicts (unpacked as kwargs).

        Args:
            items: List of items to process. Each item can be:
                - Single value: passed as first positional arg
                - Tuple: unpacked as positional args
                - Dict: unpacked as keyword args
            max_concurrent: Maximum concurrent tasks (optional, limits parallelism).

        Returns:
            ArrayJob instance that can be used to get results or add dependencies.

        Examples:
            Map over simple values:

                >>> @task(time="00:10:00")
                ... def process(file: str) -> Result:
                ...     return load_and_process(file)
                >>> files = ["a.csv", "b.csv", "c.csv"]
                >>> results = process.map(files).get_results()

            Map over tuples (multiple args):

                >>> @task(time="00:10:00")
                ... def train(config: dict, seed: int) -> Model:
                ...     return train_model(config, seed)
                >>> params = [(config1, 0), (config1, 1), (config2, 0)]
                >>> models = train.map(params).get_results()

            Map over dicts (kwargs):

                >>> @task(time="00:30:00")
                ... def experiment(lr: float, batch_size: int) -> float:
                ...     return run_experiment(lr, batch_size)
                >>> configs = [
                ...     {"lr": 0.001, "batch_size": 32},
                ...     {"lr": 0.01, "batch_size": 64},
                ... ]
                >>> scores = experiment.map(configs).get_results()

            Array with dependencies:

                >>> prep_job = preprocess("data.csv")
                >>> train_jobs = train.map(configs).after(prep_job)
                >>> results = train_jobs.get_results()
        """
        from .context import get_active_context
        from .array_job import ArrayJob

        ctx = get_active_context()
        if ctx is None:
            raise RuntimeError(
                "Task.map() must be called within a Cluster context or @workflow.\n"
                "For local execution, use: [task.unwrapped(item) for item in items]"
            )

        # Get cluster from context
        from .cluster import Cluster

        if isinstance(ctx, Cluster):
            cluster = ctx
        else:
            cluster = getattr(ctx, "cluster", None)
            if cluster is None:
                raise RuntimeError(
                    f"Context {type(ctx).__name__} does not have a cluster attribute"
                )

        return ArrayJob(
            task=self, items=items, cluster=cluster, max_concurrent=max_concurrent
        )

    def after(self, *jobs):
        """Bind explicit dependencies to this task (pre-call dependency binding).

        Returns a new SlurmTask instance with the specified jobs as pending
        dependencies. When the returned task is called, these dependencies
        will be merged with any automatic dependencies from Job arguments.

        This enables the fluent pattern: task.after(job1, job2)(args)

        Args:
            *jobs: Job instances to depend on.

        Returns:
            New SlurmTask instance with bound dependencies.

        Examples:
            Explicit dependency without data flow:

                >>> @workflow
                ... def pipeline(ctx: WorkflowContext):
                ...     job1 = process1("data1.csv")
                ...     job2 = process2("data2.csv")
                ...     # Merge depends on both jobs, but doesn't use their results
                ...     job3 = merge.after(job1, job2)("combined.csv")

            Composing with .with_options():

                >>> gpu_job = train.after(prep).with_options(gpus=2)("model.pt")

            Composing with .map():

                >>> configs = [{"lr": 0.001}, {"lr": 0.01}]
                >>> train_jobs = train.after(prep).map(configs)
        """
        from .job import Job

        # Create a new SlurmTask with the same function and options
        new_task = SlurmTask(
            func=self.func,
            sbatch_options=self.sbatch_options.copy(),
            packaging=self.packaging.copy() if self.packaging else None,
            **self.slurm_options,
        )

        # Copy existing pending dependencies and add new ones
        new_task._pending_dependencies = self._pending_dependencies.copy()
        for job in jobs:
            if isinstance(job, Job):
                new_task._pending_dependencies.append(job)
            else:
                raise TypeError(
                    f".after() expects Job arguments, got {type(job).__name__}"
                )

        return new_task

    def with_options(self, **sbatch_options):
        """Create a variant of this task with different SBATCH options.

        Returns a new SlurmTask instance with updated SBATCH options while preserving
        the function, packaging, and any pending dependencies from .after().
        This is useful for dynamic resource allocation based on runtime conditions.

        Args:
            **sbatch_options: SBATCH parameter overrides (e.g., partition="gpu",
                gpus=1, mem="32GB"). These override the task decorator's defaults
                and Slurmfile settings.

        Returns:
            New SlurmTask instance with merged SBATCH options.

        Examples:
            Override partition for specific data:

                >>> @task(time="01:00:00")
                ... def process(data: str) -> Result:
                ...     return expensive_computation(data)
                >>> @workflow
                ... def my_workflow(ctx: WorkflowContext):
                ...     # Use GPU for large files
                ...     gpu_job = process.with_options(partition="gpu", gpus=1)("large.csv")
                ...     # Use standard partition for small files
                ...     cpu_job = process("small.csv")
                ...     return [gpu_job.get_result(), cpu_job.get_result()]

            Compose with .after():

                >>> gpu_job = train.after(prep).with_options(gpus=2)("model.pt")
                >>> # Or in reverse order
                >>> gpu_job = train.with_options(gpus=2).after(prep)("model.pt")
        """
        # Merge options: self.sbatch_options + new overrides
        merged_options = {**self.sbatch_options, **sbatch_options}

        # Create new SlurmTask with merged options
        new_task = SlurmTask(
            func=self.func,
            sbatch_options=merged_options,
            packaging=self.packaging.copy() if self.packaging else None,
            **self.slurm_options,
        )

        # Preserve pending dependencies from .after()
        new_task._pending_dependencies = self._pending_dependencies.copy()

        return new_task

    def __repr__(self) -> str:
        return f"SlurmTask(name={self.sbatch_options.get('job_name', self.func.__name__)!r})"

    def __str__(self) -> str:
        return self.sbatch_options.get("job_name", self.func.__name__)
