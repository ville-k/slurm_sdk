# 0.4.6 Cleanup and Simplification Scoping

Updated after the follow-up code review. This version replaces the earlier
`0_4_6_cleanup_and_simplification_scoping.md` draft with the expanded set of
recommendations and priority adjustments.

Because the SDK is still pre-`1.0`, this release can take larger API and
architecture cleanups when they clearly reduce complexity.

## Release Intent

- Prefer removing compatibility shims over adding more indirection.
- Favor one obvious code path for submission, workflows, and packaging.
- Treat workflow and packaging correctness as higher priority than line-count
  reduction.
- Any refactor touching workflows, nested submission, or packaging inheritance
  must add targeted regression tests.
- Do not spend time on mechanical churn that does not retire real complexity.

## Highest Priority

### 1. Add real job-status fallback to accounting

`Job.get_status()` currently relies only on `backend.get_job_status()` even
though both backends implement `get_job_accounting()`.

This leaves completed jobs awkward to inspect once they disappear from
`scontrol`.

**Recommendation:** when queue status is unavailable or the scheduler reports an
invalid job ID, fall back to accounting data and normalize the returned shape so
the rest of `Job` still consumes one status model.

**If this is not implemented, remove the dead accounting surface instead.**
Keeping a fully implemented API that is never used is worse than not having it.

### 2. Fix array jobs preparing packaging twice

`ArrayJob` currently gets a strategy from `prepare_packaging_strategy()` and
then calls `packaging_strategy.prepare()` again.

For wheel and container packaging this can mean duplicate builds/uploads and
misleading callback timing.

**Recommendation:** treat `prepare_packaging_strategy()` as the only place that
prepares the strategy. Array jobs should reuse the prepared result directly.

### 3. Replace manual workflow Slurmfile rendering with typed TOML generation

`render_workflow_slurmfile()` manually serializes values into TOML-like strings.
That is fragile and does not safely round-trip list-valued packaging defaults
such as container mounts or `srun_args`.

**Recommendation:** build the generated Slurmfile through `tomlkit` just like
the upload/rewrite path already does. Preserve booleans, lists, and scalar
types without ad hoc string formatting.

### 4. Reframe packaging resolution as shared submission/workflow state

The earlier draft correctly identified packaging sprawl, but the right fix is
not necessarily to move everything onto `Cluster`.

`_submission.resolve_packaging_config()` already centralizes runtime precedence
for ordinary submission. The remaining inconsistency is that workflow Slurmfile
generation and runner-side workflow reconstruction do not share the same model
cleanly.

**Recommendation:** keep a shared internal packaging resolver/serializer in an
internal helper module, then have these paths all use it:

- task submission
- dependency container prebuilds
- workflow Slurmfile generation
- runner-side nested workflow cluster reconstruction

**Do not** create a `Cluster._resolve_packaging()` method if it just becomes a
grab-bag for unrelated client and runner concerns.

### 5. Split `render_job_script()` into focused helpers

This function is still too large and mixes:

- SBATCH directive emission
- output/error path defaults
- environment exports
- callback serialization
- runner command construction
- array-specific branching
- packaging setup and cleanup

**Recommendation:** extract helper functions around stable responsibilities,
keeping `render_job_script()` as orchestration only.

Suggested splits:

- `_resolve_output_paths(ctx, sbatch_params)`
- `_emit_sbatch_directives(...)`
- `_emit_environment_exports(...)`
- `_serialize_runner_inputs(...)`
- `_build_runner_command(...)`
- `_emit_packaging_setup(...)`
- `_emit_packaging_cleanup(...)`

### 6. Replace `_is_workflow` flags with a real workflow task type

Workflow detection is still spread across attribute checks and fallback paths.

**Recommendation:** introduce `WorkflowTask(SlurmTask)` and make
`@workflow` return that concrete type. Replace flag checks with
`isinstance(task, WorkflowTask)` everywhere practical.

This should clean up:

- `Cluster.submit()`
- workflow metadata writing
- workflow Slurmfile upload logic
- examples/callbacks that currently inspect `_is_workflow`

### 7. Make `Cluster.submit()` return one coherent submission type

The current split between `Callable[..., Job]` and `SubmittableWorkflow` is
awkward and keeps workflow detection mixed into the submission path.

**Recommendation:** introduce a common `Submittable` abstraction and return a
single object type for both regular tasks and workflows.

Because the project is still pre-`1.0`, it is acceptable to make this API
cleaner now rather than carrying the bifurcated return model forward.

### 8. Stop relying on private backend methods from array submission

`ArrayJob` still reaches into `backend._upload_file`.

**Recommendation:** add an explicit backend file-transfer method to
`BackendBase`, or rework array submission to use public `write_file()`/copy
operations consistently. Avoid private-method dispatch in user-facing paths.

### 9. Make prebuilt dependency images per-submission, not cluster-global

`_prebuilt_dependency_images` currently behaves like mutable cluster-global
state and can survive across workflow submissions on the same `Cluster`
instance.

**Recommendation:** move this into a per-submission context or ensure it is
cleared deterministically around each workflow submission.

## Medium Priority

### 10. Consolidate SSH retry/reconnect logic for idempotent reads

SSH currently has multiple retry layers with different semantics.

**Recommendation:** centralize reconnect-and-retry behavior for idempotent read
operations only, such as:

- `get_job_status`
- `get_job_accounting`
- `get_account_jobs`
- `get_queue`
- `get_cluster_info`
- `execute_command`
- `download_file`

**Do not** apply generic retries to side-effecting operations like `sbatch`,
`scancel`, uploads, or file writes.

### 11. Split runner execution paths and delete dead helper code

`runner/main.py` still carries too much orchestration, and
`run_task_with_callbacks()` is dead duplicate code.

**Recommendation:**

- delete `run_task_with_callbacks()`
- extract regular-task execution into one path
- extract workflow-task execution into one path
- keep `main()` as parse → initialize → dispatch → finalize

### 12. Rename or split `argument_loader.py`

This module handles argument parsing, startup logging, callback loading,
sys.path restoration, and task argument deserialization.

**Recommendation:** either rename it to reflect its true scope, or split it
into smaller runner modules by responsibility.

### 13. Split `callbacks/callbacks.py` and deduplicate workflow metrics I/O

This module is now a major concentration of unrelated concerns.

**Recommendation:**

- split base callback/context types from concrete callback implementations
- split logger/benchmark callbacks if that makes ownership clearer
- extract shared workflow metrics file reading/writing into one helper

This is a better use of cleanup time than several smaller cosmetic edits.

### 14. Make `from_backend()` initialize the same core attrs as `__init__`

Tests currently patch missing attributes after constructing clusters from
backends.

**Recommendation:** ensure `from_backend()` initializes the same core internal
state that the constructor does for non-connection concerns.

### 15. Decide the long-term fate of accounting APIs

Once accounting fallback exists, either:

- keep `get_job_accounting()` as a first-class backend capability and test it
  thoroughly, or
- remove it if the design changes away from explicit accounting lookups

Do not leave it half-integrated.

## Pre-1.0 API Cleanup Now Allowed

### 16. Remove vestigial `slurm_options` plumbing

The parameter is stored and forwarded but not used meaningfully.

**Recommendation:** remove it from `SlurmTask` construction and clone helpers.

### 17. Remove backward-compat task aliases and rename internals cleanly

The following compatibility layer should be removed in `0.4.6`:

- `SlurmTask.task`
- `SlurmTask.dependencies`
- `SlurmTaskWithDependencies = SlurmTask`

**Recommendation:** expose a proper `pending_dependencies` attribute instead of
wrapping `_pending_dependencies`.

### 18. Collapse `Cluster.from_file()` into a clearer config-construction path

`from_file()` overlaps with `from_args()` but uses a different configuration
model and keeps extra entry points alive.

**Recommendation:** either fold it into a generalized constructor or remove it
in favor of `from_env()` plus CLI/config parsing paths.

### 19. Fix decorator overloads to describe the real runtime type

The current overloads pretend a decorated task directly returns `Job[R]`, which
hides the actual `SlurmTask` API from type checkers.

**Recommendation:** make the overloads describe `SlurmTask`/`WorkflowTask`
directly, even if that gives up some idealized return-type narrowing.

### 20. Replace `hasattr`/`getattr` dispatch with explicit types or abstract methods

Remaining dynamic dispatch should be made explicit:

- task dependency expansion
- backend upload capabilities
- other internal type checks that are really modeling protocol boundaries

### 21. Decide whether `SubmissionError` should be data-only

The earlier draft correctly noted the coupling to Rich.

**Recommendation:** if straightforward, move rendering concerns to the CLI layer
and keep the exception object transport-agnostic. This is now acceptable
pre-`1.0`, but it is still lower priority than correctness fixes.

### 22. Standardize on `Slurmfile.toml`

The current discovery rules allow four variants.

**Recommendation:** standardize on `Slurmfile.toml`, keep temporary fallback
discovery for older names, and emit deprecation warnings when legacy variants
are used.

### 23. Standardize callback imports on the public package path

`slurm.callbacks` already re-exports the public callback classes and context
types. Many internal modules, tests, and examples still import from
`slurm.callbacks.callbacks`.

**Recommendation:** switch internal and example imports to `slurm.callbacks`,
then decide separately whether any additional top-level `slurm` re-exports are
worth adding.

## Lower Priority

### 24. Simplify `_get_importable_module_name()` after adding regression tests

The current implementation is more complex than it should be, but it touches
submissions launched from `__main__`.

**Recommendation:** add focused tests around script-style execution first, then
simplify aggressively.

### 25. Remove real SSH dead code, but keep helpful helpers

**Do:**

- remove `_upload_file()` if no longer used

**Do not do just for line count:**

- inline `_build_sbatch_command()` if it remains a clear single-purpose helper

### 26. Extract environment-loading metadata if it keeps spreading

If `env_name`, `slurmfile_path`, `environment_config`, `packaging_defaults`, and
`submit_defaults` keep spreading across the codebase, group them into a small
config object.

This is worthwhile only if it reduces branching and field leakage.

## Updates to the Earlier 0.4.6 Draft

### Keep

- backend parser extraction
- `render_job_script()` decomposition
- SSH retry cleanup
- runner refactor
- workflow type cleanup
- decorator type cleanup
- removal of vestigial task compatibility shims

### Revise

- old packaging-resolution item:
  keep the resolver centralized, but do not blindly move it onto `Cluster`
- old SSH dead-code item:
  remove `_upload_file()`, but keep `_build_sbatch_command()` if still useful
- old callback export item:
  exports mostly exist already; the remaining work is import cleanup and public
  surface cleanup

### Add

- job-status fallback to accounting
- array packaging double-prepare fix
- workflow Slurmfile typed serialization
- callback module split
- dead `run_task_with_callbacks()` removal
- `from_backend()` initialization symmetry

## Suggested Execution Order

1. Correctness and state model
   accounting fallback, array double-prepare, workflow Slurmfile serialization,
   prebuilt image lifecycle
1. Shared internal paths
   packaging resolver/serializer, backend upload abstraction, SSH retry model,
   `render_job_script()` decomposition
1. Public API simplification
   workflow task type, coherent `submit()` return type, task alias removals,
   `from_file()` consolidation, decorator overload cleanup
1. Structural cleanup
   runner split, callback module split, import cleanup, dead code removal

## Test Requirements

At minimum, add focused regression coverage for:

- completed jobs that require accounting fallback after leaving `scontrol`
- array submission ensuring packaging is prepared exactly once
- generated workflow Slurmfiles preserving list, bool, and scalar values
- per-submission isolation of prebuilt dependency images
- workflow detection through the new concrete task type
- any public API removal or constructor consolidation introduced in this release
