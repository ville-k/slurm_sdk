"""Task module for defining Slurm tasks."""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, List, Optional, Protocol, TYPE_CHECKING, Union

# Add this block for type hinting Cluster without causing circular import at runtime
if TYPE_CHECKING:
    from .job import Job
    from .array_job import ArrayJob


class _NamedCallable(Protocol):
    """Protocol for callables that have standard function attributes."""

    __name__: str
    __qualname__: str
    __module__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


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
    """Normalize SBATCH keyword-style input to our internal underscore form.

    Converts SBATCH parameter names to a consistent internal format using
    underscores. This allows users to specify parameters using either
    SBATCH-style dashes or Python-style underscores.

    Args:
        key: The SBATCH parameter name to normalize. Can use dashes or
            underscores (e.g., "cpus-per-task" or "cpus_per_task").

    Returns:
        Normalized key with underscores, lowercase. The special case "name"
        is converted to "job_name" for SBATCH compatibility.

    Examples:
        >>> normalize_sbatch_key("cpus-per-task")
        'cpus_per_task'
        >>> normalize_sbatch_key("Job-Name")
        'job_name'
        >>> normalize_sbatch_key("name")
        'job_name'
    """
    normalized = str(key or "").strip().lower().replace("-", "_")
    if normalized == "name":
        return "job_name"
    return normalized


def normalize_sbatch_options(options: Dict[str, Any] | None) -> Dict[str, Any]:
    """Return a copy of the provided mapping with normalized SBATCH keys.

    Normalizes all keys in an options dictionary to use our internal
    underscore format. Also handles common aliases like "memory" -> "mem".

    Args:
        options: Dictionary of SBATCH options to normalize. Keys can use
            either dash or underscore style. None keys are skipped.

    Returns:
        New dictionary with all keys normalized to underscore format.
        Returns empty dict if options is None or empty.

    Examples:
        >>> normalize_sbatch_options({"cpus-per-task": 4, "memory": "8GB"})
        {'cpus_per_task': 4, 'mem': '8GB'}
        >>> normalize_sbatch_options(None)
        {}
    """
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

    Task API Flow:

    ```mermaid
    flowchart LR
        subgraph Creation
            A["@task decorator"] --> B[SlurmTask]
        end

        subgraph Modifiers["Modifiers (return new SlurmTask)"]
            B --> C[".with_options()"]
            C --> B
        end

        subgraph Dependencies
            B --> D[".after(jobs)"]
            D --> E["SlurmTask (with deps)"]
        end

        subgraph Execution
            B --> F["__call__(args)"]
            E --> F
            F --> G[Job]

            B --> H[".map(items)"]
            E --> H
            H --> I[ArrayJob]
        end

        subgraph Local
            B --> J[".unwrapped"]
            J --> K["Original function"]
        end
    ```

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
        func: _NamedCallable,
        sbatch_options: Dict[str, Any] | None = None,
        packaging: Dict[str, Any] | None = None,
    ):
        """Initialize a SlurmTask (typically done via @task decorator).

        Args:
            func: The Python function to execute remotely.
            sbatch_options: SBATCH directive dictionary (will be normalized).
            packaging: Packaging configuration. Defaults to
                `{"type": "wheel", "build_tool": "uv"}`.

        Raises:
            TypeError: If func is not callable.
        """
        if not callable(func):
            raise TypeError(
                f"func must be callable, got {type(func).__name__}: {func!r}"
            )

        self.func: _NamedCallable = func
        self.sbatch_options = normalize_sbatch_options(sbatch_options)
        self.packaging = packaging or {"type": "wheel", "build_tool": "uv"}
        functools.update_wrapper(self, func)

        # Track explicit dependencies set via .after() (before task is called)
        self._pending_dependencies: list = []

        # Track container dependencies for workflows (tasks that need their containers pre-built)
        self._container_dependencies: list = []

    @property
    def pending_dependencies(self) -> list:
        """Jobs that this task depends on, set via :meth:`after`."""
        return self._pending_dependencies

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

        Examples:
            Calling within a Cluster context:

                >>> with Cluster(backend_type="ssh", hostname="hpc") as cluster:
                ...     job = my_task(arg1, arg2)
                ...     result = job.get_result()

            Calling within a workflow:

                >>> @workflow(time="01:00:00")
                ... def my_pipeline(ctx: WorkflowContext):
                ...     job = my_task("data.csv")
                ...     return job.get_result()
        """
        from .context import _resolve_cluster
        from .job import Job

        cluster = _resolve_cluster(self.func.__name__)

        # Extract Job dependencies from arguments (for automatic dependency tracking)
        # Also replace Job objects with placeholders that will be resolved at runtime
        automatic_dependencies = []
        resolved_args = []
        resolved_kwargs = {}

        for arg in args:
            if isinstance(arg, Job):
                automatic_dependencies.append(arg)
                resolved_args.append(JobResultPlaceholder(arg.id))
            else:
                resolved_args.append(arg)

        for key, value in kwargs.items():
            if isinstance(value, Job):
                automatic_dependencies.append(value)
                resolved_kwargs[key] = JobResultPlaceholder(value.id)
            else:
                resolved_kwargs[key] = value

        # Merge explicit dependencies (from .after()) with automatic dependencies
        all_dependencies = list(self._pending_dependencies) + automatic_dependencies

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
    def unwrapped(self) -> Callable[..., Any]:
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

    def map(self, items: List[Any], max_concurrent: Optional[int] = None) -> "ArrayJob":
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
        from .context import _resolve_cluster
        from .array_job import ArrayJob

        cluster = _resolve_cluster(self.func.__name__)

        dependencies = (
            list(self._pending_dependencies) if self._pending_dependencies else None
        )

        return ArrayJob(
            task=self,
            items=items,
            cluster=cluster,
            max_concurrent=max_concurrent,
            dependencies=dependencies,
        )

    def after(self, *jobs: Union["Job", "ArrayJob"]) -> "SlurmTask":
        """Bind explicit dependencies to this task (pre-call dependency binding).

        Returns a new SlurmTask with bound dependencies that can be called or mapped.
        When the returned task is called or mapped, these dependencies will be
        included in the submission.

        This enables the fluent patterns:
        - task.after(job1, job2)(args) - for regular tasks
        - task.after(job1, job2).map(items) - for array jobs

        Args:
            *jobs: Job or ArrayJob instances to depend on.

        Returns:
            New SlurmTask with bound dependencies.

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
                if not job._submitted:
                    job._submit()
                flattened_deps.extend(job._jobs)
            elif isinstance(job, Job):
                flattened_deps.append(job)
            else:
                raise TypeError(
                    f".after() expects Job or ArrayJob arguments, got {type(job).__name__}"
                )

        new_task = type(self)(
            func=self.func,
            sbatch_options=self.sbatch_options.copy(),
            packaging=self.packaging.copy() if self.packaging else None,
        )

        if self._pending_dependencies:
            new_task._pending_dependencies = (
                list(self._pending_dependencies) + flattened_deps
            )
        else:
            new_task._pending_dependencies = flattened_deps

        new_task._container_dependencies = self._container_dependencies.copy()
        return new_task

    def with_options(self, **options: Any) -> "SlurmTask":
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

        new_task = type(self)(
            func=self.func,
            sbatch_options=merged_sbatch,
            packaging=merged_packaging,
        )
        new_task._pending_dependencies = self._pending_dependencies.copy()
        new_task._container_dependencies = self._container_dependencies.copy()
        return new_task

    def partial(self, *args: Any, **kwargs: Any) -> "BoundTask":
        """Capture args to use this task in a multi-peer API.

        Returns a :class:`BoundTask` wrapping this task plus the given args.
        A ``BoundTask`` is the canonical way to hand a pre-configured task to
        ``parallel(...)``, ``task.with_sidecars(...)``, or ``Peer(...)`` — it
        captures args without triggering a submission.

        Args:
            *args: Positional arguments to bind.
            **kwargs: Keyword arguments to bind.

        Returns:
            A :class:`BoundTask` for this task with the provided arguments
            pre-bound. Use ``.partial()`` again on the result to extend the
            binding.

        Example:
            >>> @task(time="01:00:00")
            ... def train(cfg: dict) -> dict: ...
            >>> job = parallel(
            ...     Peer(train.partial(cfg={"lr": 0.001}), leader=True),
            ...     Peer(metrics, on_failure="continue"),
            ... )
        """
        return BoundTask(task=self, args=args, kwargs=kwargs)

    def with_dependencies(self, tasks: List["SlurmTask"]) -> "SlurmTask":
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
        new_task = type(self)(
            func=self.func,
            sbatch_options=self.sbatch_options.copy(),
            packaging=self.packaging.copy() if self.packaging else None,
        )
        new_task._pending_dependencies = self._pending_dependencies.copy()
        new_task._container_dependencies = list(tasks)
        return new_task

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        name = self.sbatch_options.get("job_name", self.func.__name__)
        deps = len(self._pending_dependencies)
        if deps:
            return f"{cls_name}(name={name!r}, dependencies={deps})"
        return f"{cls_name}(name={name!r})"

    def __str__(self) -> str:
        return self.sbatch_options.get("job_name", self.func.__name__)


class WorkflowTask(SlurmTask):
    """A task that orchestrates other tasks.

    Users should not instantiate this class directly — decorate a function
    with ``@workflow`` instead. ``WorkflowTask`` is exported so that callers
    can type-check with ``isinstance(task, WorkflowTask)`` when they need to
    distinguish orchestrators from regular tasks (e.g. in custom callbacks).

    Behaves identically to :class:`SlurmTask` but is distinguishable via
    ``isinstance`` so the submission pipeline can apply workflow-specific
    handling (e.g. Slurmfile upload, nested cluster reconstruction).
    """

    pass


class BoundTask:
    """A :class:`SlurmTask` with pre-bound arguments.

    ``BoundTask`` is returned by :meth:`SlurmTask.partial`. It captures args
    without triggering submission — the canonical way to hand a pre-configured
    task to a multi-peer API (``parallel``, ``task.with_sidecars``, ``Peer``).

    A ``BoundTask`` is **not directly callable**. Calling it raises a
    ``TypeError`` steering the caller toward either the multi-peer primitives
    or the plain ``SlurmTask`` (which *is* callable and submits).

    ``.partial()`` composes: ``bt.partial(b=2)`` returns a new ``BoundTask``
    with the original binding plus the extra args.

    Attributes:
        task: The underlying ``SlurmTask``.
        args: Tuple of pre-bound positional arguments.
        kwargs: Dict of pre-bound keyword arguments.

    Examples:
        Bind once, use in several peer declarations:

            >>> bound = train.partial(lr=0.001, batch_size=64)
            >>> job = parallel(
            ...     Peer(bound, pool="gpu", leader=True),
            ...     Peer(metrics, on_failure="continue"),
            ... )

        Compose bindings:

            >>> base = train.partial(model="resnet50")
            >>> variant_a = base.partial(lr=0.001)
            >>> variant_b = base.partial(lr=0.01)
    """

    __slots__ = ("task", "args", "kwargs")

    def __init__(
        self,
        task: "SlurmTask",
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not isinstance(task, SlurmTask):
            raise TypeError(
                "BoundTask requires a SlurmTask, got "
                f"{type(task).__name__}. Use the @task decorator to create "
                "one."
            )
        self.task = task
        self.args = tuple(args)
        self.kwargs = dict(kwargs) if kwargs else {}

    def partial(self, *args: Any, **kwargs: Any) -> "BoundTask":
        """Extend the binding with additional args / kwargs.

        Returns a new ``BoundTask`` whose ``args`` are the existing tuple with
        ``args`` appended and whose ``kwargs`` are the existing dict updated
        with ``kwargs``. Later kwargs override earlier ones.

        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            New ``BoundTask`` composing both bindings.
        """
        return BoundTask(
            task=self.task,
            args=self.args + tuple(args),
            kwargs={**self.kwargs, **kwargs},
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """BoundTask is not directly callable.

        Raises:
            TypeError: Always. Use ``parallel(Peer(bound_task, ...), ...)``
                or call the underlying ``SlurmTask`` directly for a
                single-submission path.
        """
        raise TypeError(
            f"BoundTask for {self.task.func.__name__!r} is not directly "
            "callable. Use it inside parallel(Peer(bound_task, ...), ...), "
            "task.with_sidecars(...), or call the underlying SlurmTask "
            "for a single-job submission."
        )

    def __repr__(self) -> str:
        name = self.task.func.__name__
        return f"BoundTask({name}, args={self.args!r}, kwargs={self.kwargs!r})"
