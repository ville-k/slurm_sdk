# Context-Based Execution

## Overview

The Slurm SDK uses context-based execution where decorated functions automatically return `Job` futures when called within a cluster or workflow context. This makes workflow code fluent, Pythonic, and similar to regular function composition.

## Design Philosophy

**Key Insight**: If a function is decorated with `@task` or `@workflow`, it's meant for distributed execution. Calling it should naturally return a Future representing that distributed computation.

This is analogous to how `async def` functions always return coroutines—the decoration fundamentally changes the semantics.

## Core Concepts

### Context-Based Behavior

Tasks exhibit different behavior depending on the execution context:

```python
@task(time="00:30:00")
def process(data: str) -> Result:
    return expensive_computation(data)

# Outside context: Raises RuntimeError
try:
    job = process("data.csv")  # ❌ Error!
except RuntimeError as e:
    print(e)  # "@task decorated function 'process' must be called within a Cluster context..."

# Inside cluster context: Returns Job
with Cluster.from_env() as cluster:
    job = process("data.csv")  # ✅ Returns Job[Result]
    result = job.get_result()  # Returns Result

# Local testing: Use .unwrapped
result = process.unwrapped("data.csv")  # ✅ Runs locally, returns Result
```

### Context Tracking

The SDK uses Python's `contextvars` module for async-safe, thread-safe context management:

```python
import contextvars
from typing import Optional, Union

_cluster_context: contextvars.ContextVar[
    Optional[Union[Cluster, WorkflowContext]]
] = contextvars.ContextVar('cluster_context', default=None)
```

**Benefits of contextvars:**
- ✅ **Async/await support**: Context preserved across `await` points
- ✅ **Thread-safe**: Automatic inheritance to child threads
- ✅ **Better isolation**: Nested contexts work correctly
- ✅ **Token-based reset**: Prevents accidental context corruption

### Context Lifecycle

```python
# Context is set when entering cluster
with Cluster.from_env() as cluster:
    # _cluster_context.set(cluster) called in __enter__

    job = task("arg")  # Uses active context

    # _cluster_context.reset(token) called in __exit__
```

## Implementation Details

### Cluster as Context Manager

`Cluster` implements the context manager protocol:

```python
class Cluster:
    def __enter__(self) -> "Cluster":
        """Enter cluster context - enables execution."""
        from .context import set_active_context
        self._context_token = set_active_context(self)
        return self

    def __exit__(self, *args) -> bool:
        """Exit cluster context - restore previous context."""
        from .context import reset_active_context
        if hasattr(self, "_context_token"):
            reset_active_context(self._context_token)
            delattr(self, "_context_token")
        return False
```

### Task Call Transformation

`SlurmTask.__call__` checks for active context:

```python
def __call__(self, *args, **kwargs):
    """Call task - returns Job in context, raises outside."""
    from .context import get_active_context
    from .job import Job

    # Check if we're in a cluster or workflow context
    ctx = get_active_context()
    if ctx is None:
        raise RuntimeError(
            f"@task decorated function '{self.func.__name__}' must be "
            "called within a Cluster context or @workflow.\n"
            f"For local execution, use: {self.func.__name__}.unwrapped(...)"
        )

    # Get the cluster from context
    from .cluster import Cluster
    if isinstance(ctx, Cluster):
        cluster = ctx
    else:
        # WorkflowContext has .cluster attribute
        cluster = getattr(ctx, "cluster", None)

    # Extract Job dependencies from arguments
    job_dependencies = []
    for arg in args:
        if isinstance(arg, Job):
            job_dependencies.append(arg)
    for value in kwargs.values():
        if isinstance(value, Job):
            job_dependencies.append(value)

    # Submit the task using the cluster's submit method
    submit_kwargs = {}
    if job_dependencies:
        submit_kwargs["after"] = job_dependencies

    submitter = cluster.submit(self, **submit_kwargs)
    job = submitter(*args, **kwargs)

    return job
```

### Automatic Dependency Tracking

Jobs passed as arguments are automatically extracted and converted to dependencies:

```python
@task
def preprocess(data: str) -> str:
    return clean_data(data)

@task
def train(data: str, config: dict) -> Model:
    return train_model(data, config)

with Cluster.from_env() as cluster:
    # Preprocess data
    prep_job = preprocess("raw_data.csv")

    # Train automatically depends on prep_job
    train_job = train(prep_job, {"lr": 0.01})
```

**How it works:**
1. `SlurmTask.__call__` scans args/kwargs for Job instances
2. Job arguments are replaced with `JobResultPlaceholder` (for serialization)
3. Dependencies are extracted and passed as `after` parameter
4. Runner resolves placeholders by loading results from job directories

The dependency is created via the `after` parameter:

```python
# In cluster.submit()
if after is not None:
    job_ids = []
    if isinstance(after, list):
        job_ids = [job.id for job in after]
    else:
        job_ids = [after.id]

    if job_ids:
        dependency_str = "afterok:" + ":".join(job_ids)
        normalized_overrides["dependency"] = dependency_str
```

### Explicit Dependencies with `.after()`

For cases where you need dependencies but don't pass Job results as arguments:

```python
@task
def task_a(data: str) -> Result:
    return process_a(data)

@task
def task_b(data: str) -> Result:
    return process_b(data)

@task
def merge(output_path: str) -> None:
    """Merge results - depends on both tasks but doesn't use their results."""
    combine_outputs(output_path)

with Cluster.from_env() as cluster:
    job_a = task_a("data_a.csv")
    job_b = task_b("data_b.csv")

    # merge depends on both jobs (explicit) but doesn't use their results
    merge_job = merge.after(job_a, job_b)("combined.csv")
```

**Key Features:**
- **Stateless**: Returns new `SlurmTask` instance with bound dependencies
- **Composable**: Works with `.with_options()` and `.map()`
- **Fluent API**: `task.after(job1, job2)(args)`

**Implementation:**
```python
def after(self, *jobs):
    """Bind explicit dependencies to this task (pre-call dependency binding)."""
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
```

When the task is called, explicit and automatic dependencies are merged:

```python
# In SlurmTask.__call__
automatic_dependencies = []  # Extracted from args/kwargs
all_dependencies = self._pending_dependencies + automatic_dependencies

submit_kwargs = {}
if all_dependencies:
    submit_kwargs["after"] = all_dependencies
```

## Local Testing

The `.unwrapped` property provides access to the original function:

```python
@task(time="01:00:00")
def process(data: str) -> Result:
    return expensive_computation(data)

# Unit test
def test_process():
    result = process.unwrapped("test_data.csv")
    assert result.status == "success"
```

## Runtime Option Override

The `.with_options()` method enables dynamic resource allocation:

```python
@workflow
def adaptive_workflow(ctx: WorkflowContext):
    # Determine resource needs at runtime
    data_size = get_data_size()

    if data_size > 1_000_000:
        # Use GPU for large data
        job = process.with_options(partition="gpu", gpus=1)("large_data.csv")
    else:
        # Use standard CPU
        job = process("small_data.csv")

    return job.get_result()
```

**Key Features:**
- **Stateless**: Returns new `SlurmTask` instance with merged options
- **Preserves Dependencies**: Copies `_pending_dependencies` from `.after()`
- **Composable**: Works with `.after()` and `.map()`

Implementation:

```python
def with_options(self, **sbatch_options):
    """Create a variant of this task with different SBATCH options."""
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
```

**Composition Examples:**
```python
# Combine .after() and .with_options() - order doesn't matter
job1 = process.after(prep_job).with_options(mem="8GB")("data.csv")
job2 = process.with_options(mem="8GB").after(prep_job)("data.csv")

# Combine with .map()
array_job = process.with_options(partition="gpu").map(items)
```

## Workflow Context

Workflows automatically set context for nested task calls:

```python
@workflow(time="02:00:00")
def my_workflow(data: list[str], ctx: WorkflowContext):
    # ctx is set as active context by runner
    # Tasks called here automatically use ctx.cluster

    jobs = [process(item) for item in data]
    return [job.get_result() for job in jobs]
```

The workflow context is a dataclass:

```python
@dataclass
class WorkflowContext:
    cluster: Cluster
    workflow_job_id: str
    workflow_job_dir: Path
    shared_dir: Path
    local_mode: bool = False
```

When a workflow executes, the runner:
1. Creates a WorkflowContext instance
2. Sets it as the active context
3. Calls the workflow function
4. Resets the context on completion

## Nested Contexts

Contexts can be nested—inner contexts take precedence:

```python
cluster1 = Cluster(backend="ssh", hostname="cluster1")
cluster2 = Cluster(backend="ssh", hostname="cluster2")

with cluster1:
    job1 = task1()  # Submits to cluster1

    with cluster2:
        job2 = task2()  # Submits to cluster2

    job3 = task3()  # Back to cluster1
```

This works because `contextvars` maintains a context stack internally.

## Error Handling

### Outside Context
```python
@task
def process(data: str) -> Result:
    return expensive_computation(data)

# Calling outside context raises with helpful message
try:
    job = process("data.csv")
except RuntimeError as e:
    print(e)
    # "@task decorated function 'process' must be called within a Cluster
    #  context or @workflow.
    #  For local execution, use: process.unwrapped(...)
    #  For cluster execution, use: with Cluster.from_env() as cluster: ..."
```

### Invalid Context
```python
class FakeContext:
    pass

with set_active_context(FakeContext()):
    try:
        job = process("data.csv")
    except RuntimeError as e:
        print(e)
        # "Context FakeContext does not have a cluster attribute"
```

## Alternative: Explicit Submission

For cases requiring explicit control, the `.submit()` method is available:

```python
cluster = Cluster.from_env()
job = task.submit(cluster=cluster)("data.csv")
result = job.get_result()
```

This pattern can be useful for:
- Fine-grained control over submission timing
- Programmatic cluster selection
- Integration with existing code patterns

## Performance Considerations

### Overhead

The context check in `__call__` has minimal overhead:
- Single dictionary lookup in contextvars (O(1))
- Type check (isinstance)
- List comprehension for dependency extraction

This is negligible compared to network I/O for job submission.

### Optimization

For tight loops, the cluster context is automatically cached:

```python
with Cluster.from_env() as cluster:
    # Efficient - context is cached
    jobs = [process(item) for item in large_list]
```

## Type Safety

### Enhanced Type Hints

The SDK uses `TYPE_CHECKING` blocks with `@overload` declarations to provide proper type hints:

```python
from typing import TYPE_CHECKING, ParamSpec, TypeVar, overload, Callable

P = ParamSpec('P')  # Preserves parameter signatures
R = TypeVar('R')     # Return type variable

if TYPE_CHECKING:
    from .job import Job

    # Overload for @task without arguments
    @overload
    def task(func: Callable[P, R]) -> Callable[P, Job[R]]: ...

    # Overload for @task(time=..., ...)
    @overload
    def task(
        func: None = None,
        *,
        packaging: Optional[Dict[str, Any]] = None,
        **sbatch_kwargs: Any,
    ) -> Callable[[Callable[P, R]], Callable[P, Job[R]]]: ...
else:
    # Runtime implementation
    def task(func=None, **options):
        if func is None:
            return lambda f: task(f, **options)
        return SlurmTask(func, options)
```

This enables full type safety:

```python
@task
def process(data: str) -> Result:
    return expensive_computation(data)

# Type checker sees: process: (data: str) -> Job[Result]
with Cluster.from_env() as cluster:
    job = process("data.csv")  # Type: Job[Result]
    result = job.get_result()   # Type: Result ✅
```

### Generic Job[T]

The `Job` class is generic over its return type:

```python
from typing import TypeVar, Generic

T = TypeVar("T")

class Job(Generic[T]):
    def get_result(self, timeout: Optional[float] = None) -> T:
        """Retrieve the return value from the completed job."""
        # ... implementation
```

This provides IDE autocomplete and type checking for results:

```python
@task
def analyze(data: str) -> dict[str, float]:
    return {"accuracy": 0.95, "loss": 0.05}

job = analyze("data.csv")  # Type: Job[dict[str, float]]
result = job.get_result()   # Type: dict[str, float]
accuracy = result["accuracy"]  # ✅ Type checker knows this is float
```

### Union[T, Job[T]] Pattern

Tasks can accept either direct values or Job futures:

```python
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    @task
    def train(
        data: Union[str, Job[str]],
        config: Union[dict, Job[dict]]
    ) -> Model:
        ...
else:
    @task
    def train(data: str, config: dict) -> Model:
        return train_model(data, config)

# Both type-check correctly:
with Cluster.from_env() as cluster:
    # Direct values
    job1 = train("data.csv", {"lr": 0.01})

    # Job arguments (automatic dependency)
    prep_job = preprocess("raw.csv")
    config_job = load_config("config.json")
    job2 = train(prep_job, config_job)
```

## Debugging

### Context Issues

Debug context problems with `get_active_context()`:

```python
from slurm import get_active_context

with Cluster.from_env() as cluster:
    ctx = get_active_context()
    print(f"Active context: {ctx}")  # Active context: <Cluster ...>

    job = task("data.csv")

# Outside context
ctx = get_active_context()
print(f"Active context: {ctx}")  # Active context: None
```

### Dependency Tracking

Debug dependency extraction:

```python
@task
def task_with_deps(job1: Job, job2: Job, data: str):
    # Dependencies are automatically extracted
    pass

# In __call__, you can add logging:
job_dependencies = []
for arg in args:
    if isinstance(arg, Job):
        print(f"Found dependency: {arg.id}")
        job_dependencies.append(arg)
```

## Best Practices

### ✅ Do

1. **Use context managers for cluster access**
   ```python
   with Cluster.from_env() as cluster:
       job = task(args)
   ```

2. **Use `.unwrapped` for unit tests**
   ```python
   def test_task():
       result = task.unwrapped(test_data)
       assert result == expected
   ```

3. **Pass Jobs as arguments for automatic dependencies**
   ```python
   job1 = preprocess(data)
   job2 = train(job1, config)  # Automatic dependency
   ```

4. **Use `.after()` for explicit dependencies**
   ```python
   job1 = task_a(data1)
   job2 = task_b(data2)
   # merge depends on both but doesn't use their results
   merge_job = merge.after(job1, job2)(output_path)
   ```

5. **Use `.with_options()` for dynamic resources**
   ```python
   if large_data:
       job = task.with_options(mem="32GB")(data)
   ```

6. **Compose `.after()` and `.with_options()`**
   ```python
   # Order doesn't matter - both are stateless
   job = task.after(prep).with_options(partition="gpu")(data)
   ```

### ❌ Don't

1. **Don't call tasks outside context**
   ```python
   # ❌ Wrong
   job = task(args)

   # ✅ Right
   with Cluster.from_env() as cluster:
       job = task(args)
   ```

2. **Don't mix context-based and explicit patterns unnecessarily**
   ```python
   # ❌ Confusing
   with Cluster.from_env() as cluster:
       job1 = task1(args)  # Context-based
       job2 = task2.submit(cluster=cluster)(args)  # Explicit

   # ✅ Consistent
   with Cluster.from_env() as cluster:
       job1 = task1(args)
       job2 = task2(args)
   ```

3. **Don't forget `.unwrapped` in tests**
   ```python
   # ❌ Wrong - tries to submit in test
   def test_task():
       with Cluster.from_env() as cluster:
           result = task(test_data)

   # ✅ Right - runs locally
   def test_task():
       result = task.unwrapped(test_data)
   ```

## Summary

Context-based execution provides a Pythonic interface for distributed computing:

- ✅ **Intuitive**: Tasks look like regular function calls
- ✅ **Safe**: Explicit context requirements
- ✅ **Testable**: `.unwrapped` for local execution
- ✅ **Powerful**: Automatic dependency tracking
- ✅ **Flexible**: Runtime option overrides
- ✅ **Async-safe**: Built on contextvars

The approach enables workflow code that reads like sequential Python while executing distributed computations on Slurm clusters.
