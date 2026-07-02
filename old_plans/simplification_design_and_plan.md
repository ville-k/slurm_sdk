# SLURM SDK Simplification: Design Review and Plan

## Goal

Reduce accidental complexity accrued over multiple iterations. Keep the core library elegant and coherent. Minimize cognitive overhead from the end user's perspective to help with adoption.

---

## Part 1: Current State Assessment

### What works well

- **Decorator-based API** (`@task`, `@workflow`) is intuitive and Pythonic
- **Lean dependency footprint** (7 core packages, no ML/data dependencies)
- **Modular backend system** (SSH, local) with clean `BackendBase` interface
- **Context management** via `contextvars` -- right abstraction, 71 lines, async/thread-safe
- **Serialization module** (`_serialization.py`) -- small, focused, backward-compatible
- **Test infrastructure** -- lean helpers (2 files, 202 lines), consistent patterns across 51 test files

### Where complexity has accumulated

The recent extraction of `_submission.py`, `_polling.py`, `_workflow.py` from `cluster.py` reduced file size but introduced a wrapper indirection layer. Combined with organic growth in the task system and packaging resolution, the codebase now has several areas of unnecessary complexity.

---

## Part 2: Findings

### Finding 1: Dead Wrapper Indirection in cluster.py

**Location**: `cluster.py` lines 663-792

After extracting submission logic into `_submission.py` and `_workflow.py`, cluster.py retained **9 private wrapper methods** that forward to module functions with zero added logic:

```python
def _setup_job_directory(self, task_func, task_defaults):
    return setup_job_directory(self, task_func, task_defaults)
```

Each is called from exactly one site in the `submitter()` closure. The wrappers add an indirection layer that makes the code harder to follow without providing any abstraction benefit.

**Exception**: The 4 polling/callback wrappers (`_dispatch_callbacks`, `_maybe_start_job_poller`, `_emit_completed_context`, `_on_poller_finished`) at lines 1034-1067 **do** earn their keep -- they bind `self.callbacks` and `self._job_pollers` state and are called from multiple external modules.

**Impact**: ~130 lines of unnecessary code, extra indirection when reading submission flow.

---

### Finding 2: Duplicated Task Wrapper Classes

**Location**: `task.py` lines 97-351 (`SlurmTaskWithDependencies`) and lines 353-721 (`SlurmTask`)

Two classes expose nearly identical interfaces:
- `SlurmTask`: `__call__`, `map`, `after`, `with_options`, `with_dependencies`, `unwrapped`
- `SlurmTaskWithDependencies`: `__call__`, `map`, `after`, `with_options`, `unwrapped`

~120 lines of duplicated logic including context resolution, Job dependency extraction, ArrayJob expansion, and submission delegation. The only behavioral difference: `SlurmTaskWithDependencies.__call__` merges explicit `self.dependencies` with automatic dependencies.

`SlurmTask` already has a `_pending_dependencies` attribute that tracks the same thing. The two classes can be merged.

**Impact**: ~200 lines of duplication. Two concepts where one suffices.

---

### Finding 3: Repeated Context Resolution Pattern

**Location**: 4 identical blocks in `task.py`

The pattern of getting active context, type-checking for Cluster vs WorkflowContext, extracting the cluster, and raising clear errors repeats verbatim in:
- `SlurmTask.__call__()`
- `SlurmTaskWithDependencies.__call__()`
- `SlurmTask.map()`
- `SlurmTaskWithDependencies.map()`

~15 lines each, ~60 lines total.

**Impact**: DRY violation. Each change to context resolution must be updated in 4 places.

---

### Finding 4: Scattered Packaging Config Resolution

**Location**: `_submission.py` lines 78-138 and `_workflow.py` lines 120-135

Packaging config is resolved from 6 sources with subtly different logic in two places. The precedence order is implicit and undocumented:

1. Pre-built dependency images (from `.with_dependencies()`)
2. Explicit `packaging_config` parameter
3. Task-level packaging (unless "auto" or "inherit")
4. Cluster `default_packaging` + merged kwargs
5. Cluster `packaging_defaults` (from Slurmfile)
6. Task packaging as final fallback

The two code paths can diverge, leading to confusing behavior when packaging config comes from different sources.

**Impact**: Bug-prone, hard to test, difficult for users to understand which setting wins.

---

### Finding 5: Overly-Parameterized render_job_script()

**Location**: `rendering.py` line 95

```python
def render_job_script(
    task_func, task_args, task_kwargs, task_definition,
    sbatch_overrides, packaging_strategy, target_job_dir,
    pre_submission_id, callbacks, cluster=None,
    is_array_job=False, array_items_file=None,
) -> str:
```

12 parameters. The submission pipeline passes 8+ related values through function chains as individual arguments rather than structured data.

**Impact**: Hard to read, easy to swap arguments, difficult to refactor.

---

### Finding 6: Small Issues

**6a. Unused validators**: `validate_account()`, `validate_partition()`, `validate_job_id()` in `validation.py` are defined but never called. Security-useful validation that should be wired in.

**6b. Dead backward compat shim**: `_runner_impl.py` is a 3-line shim not imported anywhere.

**6c. Redundant SBATCH normalization**: `render_job_script()` re-normalizes already-normalized dicts.

**6d. Conditional `import base64`**: `base64` is imported inside a conditional block but used unconditionally later -- latent NameError if code path changes.

---

## Part 3: Implementation Plan

### PR 1: Remove Dead Wrapper Indirection

Remove 9 wrapper methods from `cluster.py`. Call module functions directly from `submitter()`. Update `array_job.py` and `_workflow.py` to call `prepare_packaging_strategy()` directly instead of via `cluster._prepare_packaging_strategy()`.

**Files**: `cluster.py`, `array_job.py`, `_workflow.py`
**Lines**: ~-130 net
**Risk**: Low

### PR 2: Extract Context Helper + Merge Task Classes

Add `_resolve_cluster()` to `context.py`. Replace 4 duplicate blocks in `task.py`. Merge `SlurmTaskWithDependencies` into `SlurmTask` by making `.after()` return a new `SlurmTask` with dependencies set. Keep backward-compat alias.

**Files**: `context.py`, `task.py`, `tests/test_after_dependencies.py`
**Lines**: ~-230 net
**Risk**: Medium (highest line change, but all internal)

### PR 3: Consolidate Packaging Config Resolution

Extract `resolve_packaging_config()` with documented precedence. Use from both `_submission.py` and `_workflow.py`.

**Files**: `_submission.py`, `_workflow.py`
**Lines**: ~-10 net
**Risk**: Medium (packaging is the most complex subsystem)

### PR 4: Introduce SubmissionContext Dataclass

Replace 12 positional args on `render_job_script()` with a structured `SubmissionContext` dataclass.

**Files**: `_submission.py`, `rendering.py`, `array_job.py`
**Lines**: ~-10 net (cleaner, not shorter)
**Risk**: Medium

### PR 5: Small Cleanups

Wire unused validators, delete `_runner_impl.py`, remove redundant normalization, fix conditional import.

**Files**: `validation.py`, `_submission.py`, `rendering.py`, `_runner_impl.py`
**Lines**: ~-5 net
**Risk**: Low

### Implementation Order

```
PR 5 (Cleanups)                -- trivial, do first
PR 1 (Wrapper removal)         -- no dependencies
PR 2 (Context + merge)         -- independent of PR 1
PR 3 (Packaging)               -- after PR 1 (same files)
PR 4 (SubmissionContext)        -- after PR 1 and PR 3
```

### Estimated Impact

| Metric | Before | After |
|---|---|---|
| cluster.py LOC | ~1380 | ~1240 |
| task.py LOC | ~850 | ~650 |
| Duplicate context blocks | 4 | 0 |
| Submission wrapper methods | 9 | 0 |
| Task wrapper classes | 2 | 1 |
| render_job_script params | 12 | 1 (dataclass) |
| Net lines removed | -- | ~350-400 |

---

## Part 4: What NOT to Simplify

These areas look complex but serve clear purposes:

1. **Polling/callback wrappers on Cluster** -- bind instance state for extracted modules
2. **SubmittableWorkflow** -- separates container pre-building from submission
3. **Two-phase submit pattern** (`submit()` returns callable) -- core API, enables batch submission
4. **18 public exports** -- appropriate for library scope
5. **`_serialization.py`** -- small, focused, backward-compatible
6. **`contextvars`-based context** -- right abstraction for the problem
