# Runner Simplification Proposal

## Overview

The `src/slurm/runner.py` script is 1158 lines and handles multiple responsibilities within a single `main()` function (~950 lines). This document proposes a refactoring strategy to improve maintainability, testability, and clarity.

## Current Structure Analysis

### Main Responsibilities

1. **Argument Parsing** (lines 207-249)
   - Parse CLI arguments for module, function, files, etc.

2. **Job ID & Environment Setup** (lines 279-321)
   - Determine job ID (regular vs array)
   - Setup job context and environment snapshot
   - Restore sys.path if provided

3. **Argument Loading** (lines 326-370)
   - Different logic for array jobs vs regular jobs
   - Load pickled args/kwargs or array items

4. **Callback Loading** (lines 371-390)
   - Deserialize callbacks from file

5. **Placeholder Resolution** (lines 396-456)
   - Resolve `JobResultPlaceholder` objects by loading results from disk

6. **Context Injection** (lines 496-513)
   - Inject `JobContext` if function expects it

7. **Workflow Context Setup** (lines 514-841) - **~330 lines!**
   - Create cluster instance from Slurmfile
   - Handle packaging configuration inheritance
   - Create `WorkflowContext`
   - Inject workflow context
   - Write environment metadata
   - Activate cluster context
   - Emit workflow begin callbacks

8. **Task Execution** (lines 842-940)
   - Execute the function
   - Handle workflow end callbacks
   - Deactivate context
   - Close SSH connections

9. **Result Saving** (lines 942-1018)
   - Save result to pickle file
   - Update metadata.json with file locking

10. **Success Callbacks** (lines 1019-1077)
    - Call `on_end_run_job_ctx` and `on_completed_ctx`

11. **Error Handling** (lines 1079-1154)
    - Log errors
    - Call failure callbacks

## Proposed Refactoring

### Phase 1: Extract Helper Classes/Functions

#### 1.1 Create `ArgumentLoader` class
```python
class ArgumentLoader:
    """Handles loading task arguments from files."""

    def __init__(self, args, job_dir: str):
        self.args = args
        self.job_dir = job_dir

    def is_array_job(self) -> bool:
        return self.args.array_index is not None

    def load_args_kwargs(self) -> tuple[tuple, dict]:
        """Load task arguments based on job type."""
        if self.is_array_job():
            return self._load_array_item()
        return self._load_regular_args()

    def _load_array_item(self) -> tuple[tuple, dict]:
        ...

    def _load_regular_args(self) -> tuple[tuple, dict]:
        ...
```

#### 1.2 Create `PlaceholderResolver` class
```python
class PlaceholderResolver:
    """Resolves JobResultPlaceholder objects."""

    def resolve(self, value: Any) -> Any:
        """Recursively resolve placeholders."""
        ...
```

#### 1.3 Extract `WorkflowContextBuilder` class (~300 lines)
```python
class WorkflowContextBuilder:
    """Builds WorkflowContext for workflow execution.

    Handles:
    - Cluster creation from Slurmfile
    - Packaging configuration inheritance
    - Container image resolution
    - Environment metadata writing
    """

    def __init__(self, args, job_dir: str, job_id: str):
        self.args = args
        self.job_dir = job_dir
        self.job_id = job_id

    def build(self, func) -> tuple[WorkflowContext, Cluster | None]:
        """Build WorkflowContext if function expects it."""
        ...

    def _load_cluster(self) -> Cluster | None:
        ...

    def _configure_packaging_defaults(self, cluster: Cluster):
        ...

    def _write_environment_metadata(self, packaging_type: str):
        ...
```

#### 1.4 Create `CallbackRunner` class
```python
class CallbackRunner:
    """Manages callback execution."""

    def __init__(self, callbacks: list[BaseCallback]):
        self.callbacks = callbacks

    def run(self, method_name: str, *args, **kwargs):
        """Run callback method, catching errors."""
        ...

    def workflow_begin(self, ctx: WorkflowCallbackContext):
        ...

    def workflow_end(self, ctx: WorkflowCallbackContext):
        ...

    def run_begin(self, ctx: RunBeginContext):
        ...

    def run_end(self, ctx: RunEndContext):
        ...

    def completed(self, ctx: CompletedContext):
        ...
```

#### 1.5 Create `ResultSaver` class
```python
class ResultSaver:
    """Handles saving results and metadata."""

    def __init__(self, output_file: str, job_id: str):
        self.output_file = output_file
        self.job_id = job_id

    def save_result(self, result: Any):
        """Save result to pickle file."""
        ...

    def update_metadata(self, end_time: float):
        """Update metadata.json with file locking."""
        ...
```

### Phase 2: Restructure main()

After extracting the helper classes, `main()` becomes:

```python
def main():
    args = parse_arguments()
    setup_logging(args.loglevel)

    job_context = current_job_context()
    job_id = get_job_id()
    job_dir = args.job_dir or os.environ.get("JOB_DIR")

    # Load inputs
    arg_loader = ArgumentLoader(args, job_dir)
    task_args, task_kwargs = arg_loader.load_args_kwargs()
    callbacks = load_callbacks(args.callbacks_file)

    # Resolve placeholders
    resolver = PlaceholderResolver()
    task_args = resolver.resolve(task_args)
    task_kwargs = resolver.resolve(task_kwargs)

    # Setup callback runner
    callback_runner = CallbackRunner(callbacks)
    callback_runner.run_begin(...)

    # Import and unwrap function
    func = import_and_unwrap_function(args.module, args.function)

    # Handle context injection
    context_manager = ContextManager(func, task_args, task_kwargs)
    task_args, task_kwargs = context_manager.inject_job_context(job_context)

    workflow_ctx = None
    if context_manager.wants_workflow_context():
        builder = WorkflowContextBuilder(args, job_dir, job_id)
        workflow_ctx, cluster = builder.build(func)
        task_args, task_kwargs = context_manager.inject_workflow_context(workflow_ctx)

    # Execute
    try:
        with context_manager.activate(workflow_ctx):
            result = func(*task_args, **task_kwargs)

        # Save and report success
        saver = ResultSaver(args.output_file, job_id)
        saver.save_result(result)
        callback_runner.completed(success=True, ...)

    except Exception as e:
        callback_runner.completed(success=False, error=e, ...)
        raise
```

### Phase 3: Create Dedicated Module Structure (Optional)

If further modularization is desired:

```
src/slurm/runner/
    __init__.py          # Exports main()
    main.py              # Simplified main() function
    argument_loader.py   # ArgumentLoader class
    placeholder.py       # PlaceholderResolver class
    workflow_builder.py  # WorkflowContextBuilder class
    callbacks.py         # CallbackRunner class
    result_saver.py      # ResultSaver class
    context_manager.py   # Context injection logic
```

## Complexity Hotspots

### 1. Workflow Context Setup (lines 514-841)
**Issue**: 327 lines of nested logic for:
- Loading cluster from Slurmfile
- Handling container vs wheel vs inherit packaging
- Constructing image references
- Fallback logic

**Recommendation**: Extract into `WorkflowContextBuilder` with clear separation:
- `_load_cluster_from_slurmfile()`
- `_load_cluster_fallback()`
- `_configure_container_packaging()`
- `_configure_wheel_packaging()`
- `_write_environment_metadata()`

### 2. Duplicate Container Image Logic
Lines 638-676 and 706-744 have nearly identical code for configuring container packaging defaults.

**Recommendation**: Extract into a single `_configure_container_defaults()` method.

### 3. SSH Cleanup Code (lines 886-939)
Complex nested threading logic for closing connections.

**Recommendation**: Extract into `ClusterCleanup.close_connections(cluster)` utility.

## Testing Improvements

Current runner.py has 0% code coverage because it's difficult to test the monolithic `main()` function.

With extracted classes:
- `ArgumentLoader` can be unit tested with different file scenarios
- `PlaceholderResolver` can be tested with mock placeholders
- `WorkflowContextBuilder` can be tested with mock clusters
- `CallbackRunner` can be tested with mock callbacks
- `ResultSaver` can be tested with tmp directories

## Implementation Priority

1. **High Priority**: Extract `WorkflowContextBuilder` (biggest complexity reduction)
2. **Medium Priority**: Extract `CallbackRunner` and `ResultSaver`
3. **Lower Priority**: Extract `ArgumentLoader` and `PlaceholderResolver`
4. **Optional**: Create module structure

## Estimated Effort

- Phase 1: ~4-6 hours (extract helper classes)
- Phase 2: ~2-3 hours (restructure main)
- Phase 3: ~1-2 hours (optional module split)
- Testing: ~4-6 hours (write tests for new classes)

Total: ~11-17 hours of focused work

## Risks

1. **Behavioral Changes**: Careful testing needed to ensure identical behavior
2. **Import Order**: Some imports are intentionally inside functions for lazy loading
3. **Error Handling**: Current error paths must be preserved exactly
4. **Callback Timing**: Callbacks must fire at exactly the same points

## Recommendation

Start with extracting `WorkflowContextBuilder` since it:
- Has the most isolated logic
- Accounts for ~30% of the file
- Contains duplicated code that can be consolidated
- Is the hardest section to understand currently

This can be done incrementally without breaking changes.
