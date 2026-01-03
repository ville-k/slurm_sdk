"""Task module for defining Slurm tasks."""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

# Add this block for type hinting Cluster without causing circular import at runtime
if TYPE_CHECKING:
    pass


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


class SlurmTaskWithDependencies:
    """Wrapper for a SlurmTask with pre-specified dependencies.

    This class is returned by SlurmTask.after() and enables dependency
    specification before task submission. It supports both:
    - Calling directly: task.after(deps)(args) -> Job
    - Mapping: task.after(deps).map(items) -> ArrayJob

    The key benefit is that dependencies are specified BEFORE the operation
    (call or map), allowing immediate submission while including dependencies.

    This class exposes the same interface as SlurmTask for compatibility.

    Attributes:
        task: The underlying SlurmTask.
        dependencies: List of Job instances that must complete first.

    Examples:
        Regular task with dependencies:

            >>> prep = preprocess()
            >>> result = process.after(prep)("data.csv")

        Array job with dependencies (eager execution):

            >>> prep = preprocess()
            >>> array_job = process.after(prep).map(items)
    """

    def __init__(self, task: "SlurmTask", dependencies: List):
        """Initialize wrapper with task and dependencies.

        Args:
            task: The SlurmTask to wrap.
            dependencies: List of Job instances to depend on.
        """
        self.task = task
        self.dependencies = dependencies
        # Expose task attributes for compatibility
        self.func = task.func
        self.sbatch_options = task.sbatch_options.copy()
        self.packaging = task.packaging.copy() if task.packaging else None
        self.slurm_options = task.slurm_options
        # For compatibility with tests that check _pending_dependencies
        self._pending_dependencies = dependencies

    @property
    def __name__(self):
        """Return the wrapped function's name."""
        return self.func.__name__

    @property
    def __doc__(self):
        """Return the wrapped function's docstring."""
        return self.func.__doc__

    @property
    def unwrapped(self):
        """Access the original function for local testing.

        Returns:
            The original unwrapped function.
        """
        return self.func

    def __call__(self, *args, **kwargs):
        """Call the task with dependencies.

        Submits the task to the cluster with the pre-specified dependencies.
        This is equivalent to calling the task with after= parameter.

        Args:
            *args: Positional arguments for the task function.
            **kwargs: Keyword arguments for the task function.

        Returns:
            Job object for the submitted task.
        """
        from .context import _get_active_context
        from .job import Job

        # Check if we're in a cluster or workflow context
        ctx = _get_active_context()
        if ctx is None:
            raise RuntimeError(
                f"@task decorated function '{self.task.func.__name__}' must be "
                "called within a Cluster context or @workflow.\n"
                f"For local execution, use: {self.task.func.__name__}.unwrapped(...)"
            )

        # Get the cluster from context
        from .cluster import Cluster

        if isinstance(ctx, Cluster):
            cluster = ctx
        else:
            cluster = getattr(ctx, "cluster", None)
            if cluster is None:
                raise RuntimeError(
                    "Workflow context does not have an initialized cluster.\n"
                    "This typically means the cluster failed to initialize when the workflow started.\n"
                    "Check the workflow job logs for cluster initialization errors.\n"
                    "Common causes: Slurmfile not found, SSH connection timeout, or invalid backend configuration."
                )

        # Extract Job dependencies from arguments (automatic dependency tracking)
        automatic_dependencies = []
        resolved_args = []
        resolved_kwargs = {}

        # Process positional arguments
        for arg in args:
            if isinstance(arg, Job):
                automatic_dependencies.append(arg)
                from .task import JobResultPlaceholder

                resolved_args.append(JobResultPlaceholder(arg.id))
            else:
                resolved_args.append(arg)

        # Process keyword arguments
        for key, value in kwargs.items():
            if isinstance(value, Job):
                automatic_dependencies.append(value)
                from .task import JobResultPlaceholder

                resolved_kwargs[key] = JobResultPlaceholder(value.id)
            else:
                resolved_kwargs[key] = value

        # Expand ArrayJob objects to avoid pickling them in dependency metadata
        expanded_deps = []
        for dep in self.dependencies + automatic_dependencies:
            if hasattr(dep, "_jobs"):
                expanded_deps.extend(dep._jobs)
            else:
                expanded_deps.append(dep)

        submitter = cluster.submit(self.task, after=expanded_deps)
        job = submitter(*resolved_args, **resolved_kwargs)

        return job

    def map(self, items: List[Any], max_concurrent: Optional[int] = None):
        """Map task over items with dependencies, creating an eagerly-submitted array job.

        This method creates an array job that depends on the pre-specified
        dependencies and submits it immediately (eager execution).

        Args:
            items: List of items to process.
            max_concurrent: Maximum concurrent tasks (optional).

        Returns:
            ArrayJob instance (already submitted).

        Examples:
            >>> prep = preprocess()
            >>> array_job = process.after(prep).map(items)
            >>> results = array_job.get_results()
        """
        from .context import _get_active_context
        from .array_job import ArrayJob

        ctx = _get_active_context()
        if ctx is None:
            raise RuntimeError(
                "Task.map() must be called within a Cluster context or @workflow."
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

        # Create and eagerly submit array job with dependencies
        return ArrayJob(
            task=self.task,
            items=items,
            cluster=cluster,
            max_concurrent=max_concurrent,
            dependencies=self.dependencies,
        )

    def after(self, *jobs):
        """Add more dependencies (fluent chaining).

        Returns a new SlurmTaskWithDependencies with additional dependencies.

        Args:
            *jobs: Additional Job or ArrayJob instances to depend on.

        Returns:
            New SlurmTaskWithDependencies with combined dependencies.

        Examples:
            >>> job1 = task1()
            >>> job2 = task2()
            >>> job3 = task3.after(job1).after(job2)("data.csv")
        """
        from .job import Job
        from .array_job import ArrayJob

        # Flatten new dependencies
        flattened_deps = list(self.dependencies)  # Copy existing
        for job in jobs:
            if isinstance(job, ArrayJob):
                if not job._submitted:
                    job._submit()
                flattened_deps.extend(job._jobs)
            elif isinstance(job, Job):
                flattened_deps.append(job)
            else:
                raise TypeError(
                    f".after() expects Job or ArrayJob arguments, got {type(job).__name__}"
                )

        return SlurmTaskWithDependencies(task=self.task, dependencies=flattened_deps)

    def with_options(self, **sbatch_options):
        """Create variant with different SBATCH options.

        Returns a new SlurmTaskWithDependencies with updated options.

        Args:
            **sbatch_options: SBATCH parameter overrides.

        Returns:
            New SlurmTaskWithDependencies with merged options.

        Examples:
            >>> prep = preprocess()
            >>> gpu_job = train.after(prep).with_options(gpus=2).map(configs)
        """
        # Create new task with merged options
        merged_options = {**self.task.sbatch_options, **sbatch_options}
        new_task = SlurmTask(
            func=self.task.func,
            sbatch_options=merged_options,
            packaging=self.task.packaging.copy() if self.task.packaging else None,
            **self.task.slurm_options,
        )

        return SlurmTaskWithDependencies(task=new_task, dependencies=self.dependencies)

    def __repr__(self) -> str:
        task_name = self.task.sbatch_options.get("job_name", self.task.func.__name__)
        return f"SlurmTaskWithDependencies(task={task_name!r}, dependencies={len(self.dependencies)})"


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

        # Track container dependencies for workflows (tasks that need their containers pre-built)
        self._container_dependencies: list = []

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
        from .context import _get_active_context
        from .job import Job

        # Check if we're in a cluster or workflow context
        ctx = _get_active_context()
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
                    "Workflow context does not have an initialized cluster.\n"
                    "This typically means the cluster failed to initialize when the workflow started.\n"
                    "Check the workflow job logs for cluster initialization errors.\n"
                    "Common causes: Slurmfile not found, SSH connection timeout, or invalid backend configuration."
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

        # Expand ArrayJob objects to avoid pickling them in dependency metadata
        expanded_deps = []
        for dep in all_dependencies:
            if hasattr(dep, "_jobs"):
                expanded_deps.extend(dep._jobs)
            else:
                expanded_deps.append(dep)

        submit_kwargs = {}
        if expanded_deps:
            submit_kwargs["after"] = expanded_deps

        submitter = cluster.submit(self, **submit_kwargs)
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

        This method creates an array job where each item in the list becomes
        one task in the array. Items can be single values, tuples (unpacked
        as positional args), or dicts (unpacked as keyword args).

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

            Map over Job objects (automatic dependency tracking):

                >>> # Jobs are converted to placeholders and resolved at runtime
                >>> prep_jobs = [prepare_chunk(i) for i in range(10)]
                >>> process_jobs = process.map(prep_jobs)  # Depends on all prep_jobs
                >>> results = process_jobs.get_results()

            Jobs in tuples or dicts:

                >>> # Jobs can appear anywhere in items
                >>> data_jobs = [load_data(f) for f in files]
                >>> items = [(job, config) for job, config in zip(data_jobs, configs)]
                >>> results = train.map(items).get_results()
        """
        from .context import _get_active_context
        from .array_job import ArrayJob

        ctx = _get_active_context()
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

        Returns a SlurmTaskWithDependencies wrapper that can be called or mapped.
        When the returned wrapper is called or mapped, these dependencies will be
        included in the submission.

        This enables the fluent patterns:
        - task.after(job1, job2)(args) - for regular tasks
        - task.after(job1, job2).map(items) - for array jobs

        Args:
            *jobs: Job or ArrayJob instances to depend on.

        Returns:
            SlurmTaskWithDependencies wrapper with bound dependencies.

        Examples:
            Regular task with dependencies:

                >>> @workflow
                ... def pipeline(ctx: WorkflowContext):
                ...     job1 = process1("data1.csv")
                ...     job2 = process2("data2.csv")
                ...     # Merge depends on both jobs, but doesn't use their results
                ...     job3 = merge.after(job1, job2)("combined.csv")

            Array job with dependencies (eager execution):

                >>> configs = [{"lr": 0.001}, {"lr": 0.01}]
                >>> prep = preprocess()
                >>> train_jobs = train.after(prep).map(configs)

            Composing with .with_options():

                >>> gpu_job = train.after(prep).with_options(gpus=2)("model.pt")
        """
        from .job import Job
        from .array_job import ArrayJob

        # Validate and flatten dependencies
        flattened_deps = []
        for job in jobs:
            if isinstance(job, ArrayJob):
                # ArrayJob dependencies: ensure submitted and use all jobs
                if not job._submitted:
                    job._submit()
                flattened_deps.extend(job._jobs)
            elif isinstance(job, Job):
                flattened_deps.append(job)
            else:
                raise TypeError(
                    f".after() expects Job or ArrayJob arguments, got {type(job).__name__}"
                )

        # Return a wrapper that supports both __call__ and .map()
        return SlurmTaskWithDependencies(task=self, dependencies=flattened_deps)

    def with_options(self, **options):
        """Create a variant of this task with different SBATCH or packaging options.

        Returns a new SlurmTask instance with updated options while preserving
        the function and any pending dependencies from .after().
        This is useful for dynamic resource allocation based on runtime conditions.

        Args:
            **options: SBATCH parameter overrides (e.g., partition="gpu",
                gpus=1, mem="32GB") and packaging options (e.g., packaging_registry,
                packaging_dockerfile). These override the task decorator's defaults.

        Returns:
            New SlurmTask instance with merged options.

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

            Override packaging options:

                >>> containerized = task.with_options(
                ...     packaging_registry="registry.example.com/",
                ...     packaging_container_tag="v2",
                ... )

            Compose with .after():

                >>> gpu_job = train.after(prep).with_options(gpus=2)("model.pt")
                >>> # Or in reverse order
                >>> gpu_job = train.with_options(gpus=2).after(prep)("model.pt")
        """
        # Extract packaging_* options from other options
        packaging_overrides = {}
        sbatch_options = {}
        for key, value in options.items():
            if key.startswith("packaging_"):
                # Remove the "packaging_" prefix
                packaging_overrides[key[10:]] = value
            else:
                sbatch_options[key] = value

        # Merge sbatch options
        merged_sbatch = {**self.sbatch_options, **sbatch_options}

        # Merge packaging options
        merged_packaging = self.packaging.copy() if self.packaging else {}
        merged_packaging.update(packaging_overrides)

        # Create new SlurmTask with merged options
        new_task = SlurmTask(
            func=self.func,
            sbatch_options=merged_sbatch,
            packaging=merged_packaging,
            **self.slurm_options,
        )

        # Preserve pending dependencies from .after()
        new_task._pending_dependencies = self._pending_dependencies.copy()

        # Preserve container dependencies from .with_dependencies()
        new_task._container_dependencies = self._container_dependencies.copy()

        # Preserve workflow flag
        if hasattr(self, "_is_workflow"):
            new_task._is_workflow = self._is_workflow

        return new_task

    def with_dependencies(self, tasks):
        """Specify tasks that need their containers pre-built before this workflow runs.

        This method is used for workflows that call tasks with different container
        configurations. It ensures that child task containers are built before
        the workflow is submitted, so they are available when the workflow runs.

        Args:
            tasks: List of SlurmTask instances whose containers need to be pre-built.

        Returns:
            New SlurmTask instance with container dependencies set.

        Examples:
            Workflow with a child task that has its own container:

                >>> @task(packaging="container")
                ... def gpu_task(data: str) -> Result:
                ...     return process_on_gpu(data)
                >>> @workflow
                ... def my_workflow(ctx: WorkflowContext, data: str):
                ...     job = gpu_task(data)
                ...     return job.get_result()
                >>> with cluster:
                ...     # Pre-build gpu_task's container before submitting workflow
                ...     job = cluster.submit(my_workflow.with_dependencies([gpu_task]))("input.csv")
        """
        # Create a new SlurmTask with container dependencies
        new_task = SlurmTask(
            func=self.func,
            sbatch_options=self.sbatch_options.copy(),
            packaging=self.packaging.copy() if self.packaging else None,
            **self.slurm_options,
        )

        # Preserve pending dependencies from .after()
        new_task._pending_dependencies = self._pending_dependencies.copy()

        # Set container dependencies
        new_task._container_dependencies = list(tasks)

        # Preserve workflow flag
        if hasattr(self, "_is_workflow"):
            new_task._is_workflow = self._is_workflow

        return new_task

    def __repr__(self) -> str:
        return f"SlurmTask(name={self.sbatch_options.get('job_name', self.func.__name__)!r})"

    def __str__(self) -> str:
        return self.sbatch_options.get("job_name", self.func.__name__)
