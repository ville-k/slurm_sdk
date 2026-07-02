# Architecture

## Overview
The Slurm SDK provides a Pythonic interface for submitting Python functions as SLURM jobs. Tasks are decorated with `@task` or `@workflow` and automatically return `Job` futures when called within a cluster context. The SDK features context-based execution, hierarchical directory structures, workflow orchestration, and fluent array job APIs.

## Core Design Principles

1. **Context-Based Execution**: Tasks return Jobs automatically when called in cluster/workflow context
2. **Pythonic API**: Feels like regular Python function composition
3. **Type-Safe**: Proper type hints for IDE support
4. **Testable**: `.unwrapped` property for local execution
5. **Hierarchical**: Jobs organized by task name with sortable timestamps
6. **Async-Safe**: Context tracking uses Python's contextvars

## Public API Surface

### Core Exports
`slurm.__init__` exports:
- **Decorators**: `task`, `workflow`
- **Core Classes**: `Job`, `ArrayJob`, `Cluster`, `SlurmTask`
- **Context**: `JobContext`, `WorkflowContext`
- **Context Management**: `get_active_context`, `set_active_context`, `reset_active_context`

### Usage Pattern

```python
from slurm import task, workflow, Cluster
from slurm.workflow import WorkflowContext

@task(time="00:30:00", mem="4GB")
def process(data: str) -> Result:
    return expensive_computation(data)

@workflow(time="02:00:00")
def my_workflow(files: list[str], ctx: WorkflowContext):
    # Tasks return Jobs automatically
    jobs = [process(file) for file in files]
    return [job.get_result() for job in jobs]

# Context manager enables execution
with Cluster.from_env() as cluster:
    workflow_job = my_workflow(["a.csv", "b.csv", "c.csv"])
    results = workflow_job.get_result()
```

### Alternative: Explicit Submission

For cases where explicit control is preferred:

```python
cluster = Cluster.from_env()
job = process.submit(cluster=cluster)("data.csv")
result = job.get_result()
```

## Modules and Responsibilities

### `slurm.decorators`
- **`task(...)`**: Decorator for regular tasks
  - Collects SBATCH options and packaging config
  - Returns `SlurmTask` wrapper at runtime
  - Type signature (via `TYPE_CHECKING`): `Callable[P, R] -> Callable[P, Job[R]]`
  - Uses `@overload` declarations for type checker support
  - Default resources: nodes=1, ntasks=1, mem=1G, time=00:10:00

- **`workflow(...)`**: Decorator for workflow orchestrators
  - Wraps `@task` with workflow-specific defaults
  - Marks task with `_is_workflow=True` flag
  - Type signature: Same as `@task`, returns `Callable[P, Job[R]]`
  - Default time: 01:00:00
  - Enables WorkflowContext injection

**Type Signature Implementation:**
```python
# Type variables for generic signatures
P = ParamSpec("P")  # For parameter types
R = TypeVar("R")     # For return types

if TYPE_CHECKING:
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
```

### `slurm.task.SlurmTask`
Core task wrapper with multiple execution modes:

**Methods:**
- **`__call__(*args, **kwargs) -> Job`**: Context-based execution
  - Checks for active context (Cluster or WorkflowContext)
  - Raises `RuntimeError` if called outside context
  - Automatically extracts Job dependencies from arguments
  - Replaces Job arguments with `JobResultPlaceholder` for serialization
  - Merges explicit dependencies (from `.after()`) with automatic dependencies
  - Returns Job future immediately

- **`unwrapped`** (property): Access original function for local testing
  - Returns unwrapped function
  - Use for unit tests: `task.unwrapped(args)`

- **`after(*jobs) -> SlurmTask`**: Pre-call dependency binding (stateless)
  - Returns new SlurmTask instance with bound dependencies
  - Dependencies stored in `_pending_dependencies` list
  - Merged with automatic dependencies when task is called
  - Enables fluent pattern: `task.after(job1, job2)(args)`
  - Composes with `.with_options()` and `.map()`

- **`with_options(**sbatch_options) -> SlurmTask`**: Runtime option override (stateless)
  - Returns new SlurmTask instance with merged SBATCH options
  - Preserves `_pending_dependencies` from `.after()`
  - Useful for dynamic resource allocation
  - Example: `task.with_options(partition="gpu")(args)`
  - Composes with `.after()` and `.map()`

- **`map(items, max_concurrent=None) -> ArrayJob`**: Fluent array submission
  - Creates ArrayJob for batch processing
  - Items can be values, tuples, or dicts
  - Returns ArrayJob with `.after()` and `.get_results()` methods

- **`submit(cluster=..., **kwargs)`**: Explicit submission
  - Returns submitter callable
  - Alternative pattern for explicit control

**Internal Attributes:**
- **`_pending_dependencies: list`**: Explicit dependencies from `.after()`
  - Initialized as empty list in `__init__`
  - Copied when creating new instances via `.after()` or `.with_options()`
  - Merged with automatic dependencies from Job arguments in `__call__`

### `slurm.context`
Context tracking for context-based execution using Python's `contextvars`:

```python
import contextvars
from typing import Optional, Union

_cluster_context: contextvars.ContextVar[
    Optional[Union[Cluster, WorkflowContext]]
] = contextvars.ContextVar('cluster_context', default=None)
```

**Functions:**
- **`get_active_context()`**: Returns current Cluster or WorkflowContext
- **`set_active_context(context)`**: Sets context, returns token for reset
- **`reset_active_context(token)`**: Restores previous context

**Benefits:**
- ✅ Async/await support (context preserved across await)
- ✅ Thread-safe (automatic inheritance to child threads)
- ✅ Nested contexts (proper stack management)
- ✅ Token-based reset (prevents corruption)

### `slurm.cluster.Cluster`
Cluster management with context manager support:

**Context Manager Methods:**
- **`__enter__()`**: Enters cluster context, enables execution
  - Sets cluster as active context using contextvars
  - Returns self for `with` statement

- **`__exit__(...)`**: Exits cluster context, restores previous context
  - Resets context using token
  - Returns False (doesn't suppress exceptions)

**Directory Structure:**
- Jobs organized hierarchically: `{job_base_dir}/{task_name}/{timestamp}_{unique_id}/`
- Timestamp format: `YYYYMMDD_HHMMSS` (sortable, human-readable)
- Task name sanitization: lowercase, replace special chars with underscores
- Workflow nesting: `{workflow_dir}/tasks/{task_name}/{timestamp}_{unique_id}/`

**Dependency Tracking:**
- `submit()` accepts `after` parameter for job dependencies
- Converts Job or List[Job] to `--dependency=afterok:id1:id2`
- Automatic extraction from task arguments

**Metadata Generation:**
- Creates `metadata.json` in each job directory
- Contains: job_id, pre_submission_id, task_name, timestamp, status
- Tracks parent_workflow for nested structure
- Includes is_workflow flag

### `slurm.workflow.WorkflowContext`
Context for workflow orchestrators:

**Attributes:**
- `cluster`: Cluster instance for submitting tasks
- `workflow_job_id`: Job ID of the orchestrator
- `workflow_job_dir`: Workflow's own directory
- `shared_dir`: Shared data directory (`workflow_job_dir/shared/`)
- `local_mode`: Boolean flag for local testing

**Directory Properties:**
- `result_path`: Path to workflow result (`workflow_job_dir/result.pkl`)
- `metadata_path`: Path to metadata (`workflow_job_dir/metadata.json`)
- `tasks_dir`: Directory for worker tasks (`workflow_job_dir/tasks/`)

**Context Symmetry Methods:**
Enable easy consumption of task/workflow outputs:

- **`get_task_output_dir(task_name)`**: Get directory for all runs of a task
- **`list_task_runs(task_name)`**: List run directories, newest first
- **`get_latest_task_result(task_name)`**: Load result from most recent run
- **`load_workflow_result(workflow_path)`**: Load result from another workflow

**Initialization:**
- Auto-creates `shared/` and `tasks/` directories
- Converts paths to Path objects
- Called via `__post_init__` dataclass hook

### `slurm.array_job.ArrayJob`
Fluent array job API:

**Grouped Directory Structure:**
```
{task_name}/{timestamp}_{id}/
├── array_metadata.json       # Array-level metadata
├── tasks/                    # Individual array tasks
│   ├── 000/                  # Array index 0
│   ├── 001/                  # Array index 1
│   └── 002/                  # Array index 2
└── results/                  # Aggregated results
```

**Methods:**
- **`__len__()`**: Number of tasks in array
- **`__getitem__(index)`**: Get Job for specific array element
- **`__iter__()`**: Iterate over jobs
- **`after(*jobs)`**: Add dependencies (fluent API)
- **`get_results(timeout=None)`**: Wait and collect all results
- **`get_results_dir()`**: Path to aggregated results directory
- **`wait(timeout=None)`**: Wait for all tasks to complete

**Item Types:**
- **Single values**: Passed as first positional argument
- **Tuples**: Unpacked as positional arguments
- **Dicts**: Unpacked as keyword arguments

### `slurm.task.JobResultPlaceholder`
Serialization helper for Job arguments:

**Purpose:**
- Job objects contain threading locks and cannot be pickled
- When a Job is passed as argument, it's replaced with a placeholder
- Placeholder contains only the job ID (string)
- Runner resolves placeholder by loading result from job directory

**Attributes:**
- `job_id`: The Slurm job ID whose result should be loaded

**Usage Flow:**
1. In `SlurmTask.__call__`: Job arguments → JobResultPlaceholder
2. Arguments serialized with placeholders
3. In runner: Placeholder → load result from job directory
4. Function executes with actual result values

### `slurm.job.Job[T]`
Generic job future representing a submitted task:

**Type Parameter:**
- **`T`**: Return type of the task function
- Enables type-safe result retrieval: `job.get_result() -> T`

**Creation:**
- Created automatically by context-based `__call__`
- Supports automatic dependency extraction
- Works with ArrayJob for batch operations
- `target_job_dir` follows hierarchical structure

**Status & Result Methods:**
- `is_running()`, `is_completed()`, `is_successful()`
- `wait(timeout=None)`, `cancel()`
- `get_result(timeout=None) -> T`: Downloads and unpickles result
- `get_status()`: Queries scheduler state

### `slurm.rendering`
Job script rendering:

**Job Script Rendering:**
- Includes `#SBATCH` directives from task options
- Supports dependency parameter (`--dependency=afterok:...`)
- Defaults output/error to hierarchical job directory
- Defines `JOB_DIR` environment variable
- Injects packaging setup/cleanup commands
- Serializes args/kwargs/callbacks with pickle

**Directory Path Construction:**
- Generates timestamp and unique ID
- Sanitizes task name for filesystem
- Builds hierarchical paths
- Supports workflow nesting

### `slurm.runner`
Entry point executed on compute nodes:

**WorkflowContext Injection:**
- Inspects function signature for `WorkflowContext` parameter
- Auto-injects context if parameter type-annotated
- Sets active context for nested task calls
- Properly resets context on completion

**Execution Flow:**
1. Restores `sys.path` from pickled value
2. Loads args/kwargs/callbacks
3. Imports task module
4. Injects WorkflowContext if needed
5. Invokes function
6. Pickles result to output file
7. Runs completion callbacks

### `slurm.runtime`
- **`JobContext`**: Runtime metadata from SLURM environment
  - Includes `job_dir` and `shared_dir` properties
  - Symmetry with WorkflowContext

- **`current_job_context()`**: Lazily builds context from environment
- **`bind_job_context()`**: Auto-injects context via signature inspection

### `slurm.callbacks`
Callback system:
- `BaseCallback` with context-based lifecycle hooks
- Execution loci: client/runner/both
- Optional polling via `poll_interval_secs`
- Built-in: `LoggerCallback`, `RichLoggerCallback`, `BenchmarkCallback`

### `slurm.api`
Backend abstraction:
- **`BackendBase`**: Abstract interface
- **`SSHCommandBackend`**: SSH/SFTP implementation
  - Resolves remote `job_base_dir` (`~/slurm_jobs`)
  - Uploads scripts, ensures directories
  - Parses job IDs from sbatch output
  - Status/queue/cancel via scontrol/squeue/scancel

### `slurm.packaging`
Packaging strategies:
- **`NonePackagingStrategy`**: No-op
- **`WheelPackagingStrategy`**: Builds wheel with uv/pip
- **`ContainerPackagingStrategy`**: Container images with Enroot/Pyxis

## Directory Structure Examples

### Regular Task
```
~/slurm_jobs/
└── train_model/
    ├── 20250107_143022_a1b2c3d4/
    │   ├── job.sh
    │   ├── job.out
    │   ├── job.err
    │   ├── result.pkl
    │   └── metadata.json
    └── 20250108_092301_e5f6g7h8/
```

### Workflow with Nested Tasks
```
~/slurm_jobs/
└── hyperparameter_search/
    └── 20250107_140000_w1x2y3z4/
        ├── workflow.sh
        ├── workflow.out
        ├── result.pkl
        ├── metadata.json
        ├── shared/                  # ctx.shared_dir
        │   └── configs.pkl
        └── tasks/                   # ctx.tasks_dir
            ├── train_model/
            │   ├── 20250107_140102_t1a2b3c4/
            │   └── 20250107_140103_t5d6e7f8/
            └── evaluate_model/
                └── 20250107_141530_e1j2k3l4/
```

### Array Job (Grouped Structure)
```
~/slurm_jobs/
└── process_chunk/
    └── 20250107_143022_a1b2c3d4/
        ├── array_metadata.json
        ├── tasks/
        │   ├── 000/
        │   │   ├── job.sh
        │   │   ├── result.pkl
        │   │   └── metadata.json
        │   ├── 001/
        │   └── 002/
        └── results/
            └── aggregated.pkl
```

## Notable Features and Behaviors

### Context-Based Execution
- ✅ Tasks must be called within cluster/workflow context
- ✅ Raises `RuntimeError` if called outside context
- ✅ Use `.unwrapped` for local testing
- ✅ Automatic dependency tracking from Job arguments
- ✅ Explicit dependencies via `.after()` method

### Dependency Tracking
The SDK supports two forms of dependency tracking:

**Automatic Dependencies** (from Job arguments):
```python
prep_job = preprocess("data.csv")
# train_job automatically depends on prep_job
train_job = train(prep_job, config)
```

**Explicit Dependencies** (via `.after()`):
```python
job1 = task_a(arg1)
job2 = task_b(arg2)
# merge_job explicitly depends on both, without using their results
merge_job = merge.after(job1, job2)("output.txt")
```

**Hybrid Dependencies** (both):
```python
prep1 = preprocess1("data1.csv")
prep2 = preprocess2("data2.csv")
data_job = load_data("data3.csv")
# Depends on prep1, prep2 (explicit) and data_job (automatic)
result = process.after(prep1, prep2)(data_job, config)
```

### Context Management
- ✅ Thread-safe and async-safe (contextvars)
- ✅ Automatic inheritance to child threads
- ✅ Nested contexts supported
- ✅ Token-based reset prevents corruption

### Hierarchical Directories
- ✅ Human-friendly organization by task name
- ✅ Sortable timestamps (YYYYMMDD_HHMMSS)
- ✅ Workflow nesting under tasks/ subdirectory
- ✅ Metadata files for programmatic querying

### Workflow Orchestration
- ✅ Workflows submit other tasks
- ✅ Context injection via function parameters
- ✅ Shared directories for data exchange
- ✅ Context symmetry for consuming outputs

### Array Jobs
- ✅ Fluent `.map()` API
- ✅ Grouped directory structure
- ✅ Dependencies via `.after()` chaining
- ✅ Batch result collection

### Type Safety
- ✅ Proper type hints throughout
- ✅ Generic `Job[T]` and `ArrayJob[T]` types for type-safe results
- ✅ `TYPE_CHECKING` guards for circular imports
- ✅ `@overload` declarations for decorator type signatures
- ✅ `ParamSpec` and `TypeVar` for preserving function signatures
- ✅ Decorated tasks return `Callable[P, Job[R]]` to type checkers
- ✅ Support for `Union[T, Job[T]]` pattern in task signatures

## Limitations and Future Work

### Current Limitations
- **Backend Support**: Only SSH backend implemented (no local backend yet)
  - LocalBackend exists for testing but not for production use
- **Array Jobs**: Submit individual tasks rather than native Slurm array jobs
  - Grouped directory structure is in place
  - Native `--array` submission would be more efficient
- **JobResultPlaceholder**: Requires result files to be preserved
  - If job directory is cleaned up, result resolution fails
  - Consider result caching or alternative dependency passing

### Design Considerations
- **Stateless Task Methods**: `.after()` and `.with_options()` return new instances
  - Enables composition and method chaining
  - Prevents mutation of shared task objects
  - May create more objects than mutable design
- **Context Requirement**: Tasks must be called within context
  - Explicit but may surprise new users
  - `.unwrapped` provides escape hatch for local execution
- **Type Safety Trade-offs**: Uses `TYPE_CHECKING` blocks
  - Runtime behavior differs from type checker view
  - Necessary to avoid circular imports and preserve runtime performance

### Future Enhancements
- Native Slurm array job submission with `--array` directive
- Production-ready local backend for single-machine testing
- REST backend for remote cluster management
- Workflow visualization and debugging tools
- Enhanced result caching for better JobResultPlaceholder reliability

## Extension Points

### Custom Callbacks
Implement `BaseCallback` for lifecycle hooks:
- `on_begin_package_ctx`, `on_end_package_ctx`
- `on_begin_submit_job_ctx`, `on_end_submit_job_ctx`
- `on_begin_run_job_ctx`, `on_end_run_job_ctx`
- `on_job_status_update_ctx`, `on_completed_ctx`

### Custom Packaging Strategies
Implement `PackagingStrategy`:
- `prepare(task, cluster)`: Setup logic
- `generate_setup_commands(...)`: Bash commands
- `generate_cleanup_commands(...)`: Teardown

### Custom Backends
Implement `BackendBase`:
- `submit_job(...)`: Job submission
- `get_job_status(...)`: Status queries
- `cancel_job(...)`: Cancellation
- `get_queue()`, `get_cluster_info()`

## External Dependencies
- **paramiko**: SSH/SFTP communication
- **rich**: Optional UI enhancements
- **Standard library**: contextvars, pickle, base64, logging, subprocess
