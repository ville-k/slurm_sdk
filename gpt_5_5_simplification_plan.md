# GPT-5.5 Simplification Plan

This plan sketches a major-version cleanup that removes several high-complexity features from the SDK:

- Wheel packaging for submitted jobs
- Slurmfile support and `Cluster.from_env()`
- MCP server and Textual TUI/docs browser
- Benchmarking callbacks
- Hard dependency on Rich for rich-only variants

The intended product shape after this work is a smaller core SDK focused on:

- Direct `Cluster(...)` construction
- Local and SSH backends
- `@task`, `@workflow`, jobs, arrays, dependencies, logs, and results
- No packaging by default
- Optional container execution for prebuilt images, if retained
- Plain logging by default, with Rich output only when Rich is installed

This is a breaking change and should be treated as a major release.

## Guiding Principles

- Prefer one configuration path. Direct Python construction should be the source of truth.
- Avoid implicit deployment behavior. Code should run from a shared filesystem or a declared container image.
- Keep optional integrations optional at import time. A core import of `slurm` should not require Rich, Textual, FastMCP, or TOML parsing packages.
- Remove compatibility shims only after tests and docs stop depending on them.
- Keep the core execution path boring: render script, submit job, track status, retrieve logs/results.

## Phase 1: Remove Wheel Packaging

### Current Complexity

Wheel packaging currently turns the SDK into a build, upload, install, cleanup, and environment management system. It touches:

- `src/slurm/packaging/wheel.py`
- `src/slurm/packaging/__init__.py`
- `src/slurm/packaging/inherit.py`
- `src/slurm/_packaging_resolver.py`
- `src/slurm/_submission.py`
- `src/slurm/decorators.py`
- `src/slurm/task.py`
- `src/slurm/runner/workflow_builder.py`
- docs, examples, callback messages, and integration fixtures

It also drives several hard-to-test edge cases: project root detection, temporary wheel directories, remote upload paths, virtualenv creation, pip/uv install commands, cleanup, and parent workflow environment detection.

### Removal Work

- Delete `src/slurm/packaging/wheel.py`.
- Remove `"wheel"` from `get_packaging_strategy()` and `PackagingConfig`.
- Remove wheel handling from `parse_packaging_config()`.
- Change the default packaging behavior:
  - Preferred: default to `{"type": "none"}`.
  - Also acceptable: keep `"auto"` temporarily but resolve it to `"none"` with a deprecation warning.
- Remove wheel branches from `InheritPackagingStrategy`.
- Remove virtualenv and `VIRTUAL_ENV` parent-packaging detection from `runner/workflow_builder.py`.
- Remove wheel-specific docs, examples, tests, and integration fixtures.
- Update error messages that currently suggest `packaging="wheel"`.

### API After Removal

Supported packaging values should be limited to:

- `packaging="none"`
- `packaging="container"` or `packaging="container:<image>"`, if container packaging is retained
- Possibly `packaging="inherit"` for containerized workflows only

Avoid keeping `packaging="auto"` unless there is a clear migration reason.

### Second-Order Simplifications Unlocked

- Remove `pyproject.toml` discovery from job submission.
- Remove local temporary wheel artifact lifecycle.
- Remove remote virtualenv setup and cleanup commands.
- Remove wheel-specific parent workflow metadata.
- Simplify `PackagingBeginContext` and `PackagingEndContext`, or rename them later to container-specific lifecycle hooks if container build/resolve remains.
- Narrow `PackagingError` so it no longer needs to explain Python build backend failures.
- Reduce integration test setup that exists only to make test functions importable from built wheels.

## Phase 2: Remove Slurmfile and `Cluster.from_env()`

### Current Complexity

Slurmfile support creates a second configuration plane in addition to direct `Cluster(...)` construction. It affects:

- `src/slurm/config.py`
- `Cluster.from_env()`
- deprecated `Cluster.from_file()`
- Slurmfile error classes in `src/slurm/errors.py`
- CLI `--env` and `--slurmfile` options
- workflow Slurmfile rendering/upload in `src/slurm/_workflow.py`
- workflow runner reconstruction in `src/slurm/runner/workflow_builder.py`
- MCP server configuration
- docs, examples, security guidance, and tests

It also forces the SDK to reconcile task defaults, cluster defaults, packaging defaults, callbacks, backend config, environment names, and generated Slurmfiles.

### Removal Work

- Delete `src/slurm/config.py`.
- Remove `Cluster.from_env()` and `Cluster.from_file()`.
- Remove `slurmfile_path`, `env_name`, `environment_config`, and `packaging_defaults` attributes from `Cluster`.
- Remove Slurmfile-specific errors:
  - `SlurmfileError`
  - `SlurmfileNotFoundError`
  - `SlurmfileInvalidError`
  - `SlurmfileEnvironmentNotFoundError`
- Remove Slurmfile handling from CLI helpers and command options.
- Remove `handle_workflow_slurmfile()` and `render_workflow_slurmfile()` from `src/slurm/_workflow.py`, or replace the module with workflow-only metadata helpers.
- Rewrite workflow runner reconstruction so it no longer calls `Cluster.from_env()`.
- Remove `tomlkit` and `tomli` dependencies if no remaining code needs TOML parsing.
- Update docs and examples to use explicit `Cluster(...)` construction.

### Replacement for Workflow Rehydration

Workflows that submit child tasks still need a way to create a child-submission cluster inside the workflow job. Replace Slurmfile upload with explicit runtime metadata:

- At parent workflow submission time, serialize a minimal cluster configuration as JSON into an environment variable or metadata file in the workflow job directory.
- Include only backend type and backend kwargs needed for child submission.
- Include job base directory and default SBATCH options.
- Do not serialize callbacks by default.
- Do not serialize secrets unless the current implementation already had access to them and the user explicitly opts in.

This keeps workflow reconstruction backend-aware and avoids mutating TOML in the job directory.

### CLI After Removal

Remove commands that only make sense with Slurmfile environments:

- `slurm cluster list`
- `slurm cluster connect`, unless it is redesigned to accept explicit SSH flags
- `--env`
- `--slurmfile`

For job commands, choose one of:

- Require explicit backend flags such as `--backend ssh --hostname ...`
- Read simple environment variables such as `SLURM_SDK_BACKEND=ssh`, `SLURM_SDK_HOSTNAME=...`
- Keep CLI job management intentionally minimal and recommend Python API usage for configured clients

### Second-Order Simplifications Unlocked

- Remove dynamic callback import from trusted Slurmfile config.
- Remove Slurmfile security documentation and error guidance.
- Remove TOML round-trip parsing and generated Slurmfile upload bugs.
- Simplify `Cluster` initialization docs and examples.
- Simplify workflow runner startup and remove timeout wrappers around `Cluster.from_env()`.
- Collapse packaging resolution order by removing "Slurmfile defaults" as an input.
- Remove a large class of tests that create temporary Slurmfiles just to instantiate clusters.

## Phase 3: Remove MCP Server and Textual TUI/Docs Browser

### Current Complexity

The MCP and TUI features are integration surfaces around the core SDK. They pull in additional dependencies and configuration assumptions:

- `src/slurm/mcp_server.py`
- `src/slurm/cli/mcp.py`
- `fastmcp` dependency
- `src/slurm/tui/*`
- `src/slurm/cli/docs.py`
- `src/slurm/cli/dash.py`, if removing the Textual dashboard as well
- `textual` and `pyyaml` optional dependency group
- bundled docs in the package wheel for the docs browser

The MCP server also depends heavily on Slurmfile configuration, so it becomes less useful after `Cluster.from_env()` is removed.

### Removal Work

- Delete `src/slurm/mcp_server.py`.
- Delete `src/slurm/cli/mcp.py` and remove `mcp_app` registration from `src/slurm/cli/app.py`.
- Remove `fastmcp` from project dependencies.
- Delete `tests/test_mcp_server.py` and MCP documentation/reference sections.
- Delete Textual docs browser modules:
  - `src/slurm/tui/docs/*`
  - `src/slurm/cli/docs.py`
- If the goal is to remove all Textual UI, also delete:
  - `src/slurm/tui/dashboard/*`
  - `src/slurm/tui/common/*`
  - `src/slurm/cli/dash.py`
- Remove the `tui` optional dependency group if no Textual code remains.
- Remove bundled docs from the package build if they only exist for the docs browser.

### Second-Order Simplifications Unlocked

- Core installs no longer include FastMCP.
- The package wheel can stop embedding `docs/` and `mkdocs.yml`, reducing installed package size.
- CLI reference generation gets smaller.
- The public surface becomes easier to explain: Python SDK plus lightweight CLI, not SDK plus MCP plus Textual apps.
- Fewer optional import paths need special handling in type checking.

## Phase 4: Remove Benchmarking Callbacks

### Current Complexity

Benchmarking currently lives in the callback layer and overlaps with normal logging/observability:

- `src/slurm/callbacks/benchmark.py`
- benchmark exports in `src/slurm/callbacks/__init__.py` and `src/slurm/__init__.py`
- workflow metric helpers in `src/slurm/callbacks/_metrics.py`
- benchmark-specific tests and integration tests
- docs/reference entries and callback explanation pages

It adds stateful timing and workflow aggregation logic that is not necessary for job execution.

### Removal Work

- Delete `src/slurm/callbacks/benchmark.py`.
- Remove `BenchmarkCallback` from exports.
- Remove benchmark tests:
  - `tests/test_workflow_example_callbacks.py`, or the benchmark-specific parts
  - benchmark-specific integration assertions
- Remove benchmark docs and examples.
- Keep `LoggerCallback` as the simple standard-library callback.
- Review `src/slurm/callbacks/_metrics.py`:
  - Delete it if only `BenchmarkCallback` uses it.
  - Fold minimal shared summary logic into `LoggerCallback` if still useful.
- Remove benchmark metric filenames and constants if they are no longer used.

### Second-Order Simplifications Unlocked

- Callback APIs can focus on lifecycle notification instead of metrics aggregation.
- Fewer callback objects need to be pickle-aware for workflow execution.
- Workflow callback tests can focus on propagation and ordering, not timing.
- Documentation can present one default callback path: `LoggerCallback`.

## Phase 5: Make Rich Variants Optional and Import-Guarded

### Current Complexity

Rich is currently a required dependency, and many modules import it directly:

- `src/slurm/callbacks/rich_logger.py`
- `src/slurm/callbacks/__init__.py`
- `src/slurm/cli/*.py`
- `src/slurm/cli/live/*`
- `src/slurm/cli/formatters.py`
- `src/slurm/ui.py`
- `src/slurm/errors.py` rich rendering hooks
- examples and docs that import `RichLoggerCallback`

The desired shape is that rich functionality exists only when Rich is installed.

### Removal Work

- Move `rich` from required dependencies to an optional extra, for example:
  - `rich = ["rich>=13.9.4"]`
  - or `cli = ["rich>=13.9.4"]` if the CLI remains Rich-first.
- Make `src/slurm/callbacks/__init__.py` avoid importing `rich_logger` unconditionally.
- Add a lazy export or clear import guard for `RichLoggerCallback`.
- Ensure `from slurm import Cluster, task` works without Rich installed.
- Ensure `from slurm.callbacks import LoggerCallback` works without Rich installed.
- Decide CLI behavior without Rich:
  - Option A: CLI requires the `cli` extra and prints a clear install error.
  - Option B: CLI falls back to plain text output.
- Keep `SubmissionError.__rich_console__` optional and safe when Rich is absent.
- Update tests to run a no-Rich import smoke test in an isolated environment or with import blocking.

### Second-Order Simplifications Unlocked

- Core dependency list becomes smaller.
- Callback imports become safer in minimal runtime environments.
- Standard logging and Rich logging become clearly separated.
- Examples can default to `LoggerCallback`, with Rich documented as optional polish.

## Cross-Cutting Migration Steps

### Public API

Remove or change these exports:

- `Cluster.from_env`
- `Cluster.from_file`
- `parse_packaging_config("wheel")`
- `WheelPackagingStrategy`
- `BenchmarkCallback`
- Slurmfile error classes
- MCP server helpers
- Textual TUI app helpers

Keep or clarify these APIs:

- `Cluster(...)`
- `Cluster.from_args(...)`, if it can be made Slurmfile-free
- `@task`
- `@workflow`
- `Job`
- `ArrayJob`
- `LoggerCallback`
- `RichLoggerCallback`, optional
- `NonePackagingStrategy`
- `ContainerPackagingStrategy`, if container support remains

### Dependency Cleanup

Likely removable from runtime dependencies:

- `fastmcp`
- `tomlkit`
- `tomli`
- `rich`, moved to an optional extra

Likely removable from optional dependencies if all Textual UI is removed:

- `textual`
- `pyyaml`, if only used by the docs/TUI browser

Potentially removable later:

- `requests`, if container registry digest/build/push helpers are also removed

### Test Cleanup

Remove or rewrite tests covering:

- Slurmfile discovery and parsing
- `Cluster.from_env()` and `Cluster.from_file()`
- Workflow Slurmfile upload/rendering
- Wheel packaging build/install/cleanup
- MCP tools/resources
- Textual docs/dashboard apps
- Benchmark callback metrics
- Rich logger behavior in the default test path

Add focused replacement tests for:

- Direct `Cluster(...)` construction from explicit kwargs
- Workflow child cluster reconstruction from JSON metadata
- No-Rich core import
- Optional Rich callback import when Rich is installed
- Packaging defaults resolving to `none`
- Container prebuilt-image execution, if container support remains

### Documentation Cleanup

Rewrite examples to prefer explicit construction:

```python
from slurm import Cluster

cluster = Cluster(
    backend_type="ssh",
    hostname="login.example.edu",
    username="alice",
    job_base_dir="/shared/alice/slurm-jobs",
    default_partition="gpu",
    default_account="research",
)
```

Remove Slurmfile-oriented docs:

- CLI environment selection
- Slurmfile security model
- Slurmfile callback configuration
- workflow Slurmfile upload explanations

Remove wheel-oriented docs:

- wheel packaging examples
- wheel debugging instructions
- wheel cleanup or virtualenv guidance

Remove integration docs:

- MCP command reference
- Textual docs browser
- Textual dashboard, if removed

Update callback docs:

- `LoggerCallback` as default
- `RichLoggerCallback` as optional extra
- no `BenchmarkCallback`

## Suggested Implementation Order

1. Gate Rich imports first. This reduces incidental failures while the public surface is changing.
1. Remove MCP server and Textual docs/TUI. These are leaf integrations and can be cut with limited impact on core execution.
1. Remove BenchmarkCallback. This is mostly isolated to callbacks, docs, and tests.
1. Remove wheel packaging. This is broader because it touches submission, runner metadata, inheritance, and docs.
1. Remove Slurmfile and `Cluster.from_env()`. This should come after MCP removal and alongside the workflow rehydration replacement.
1. Run a final API simplification pass over packaging resolution, callback contexts, CLI options, docs, and dependency metadata.

## Expected Net Effect

The largest complexity wins come from removing:

- runtime wheel build/install behavior
- Slurmfile as a second configuration source
- optional integration surfaces that are not part of job execution
- benchmark metrics aggregation
- hard Rich imports

After the full simplification, the SDK should be easier to reason about because task submission has fewer branches:

1. Resolve direct cluster configuration.
1. Resolve SBATCH options.
1. Resolve execution environment: none or prebuilt container.
1. Render job script.
1. Submit through local or SSH backend.
1. Track job, logs, and result.

That is the shape worth protecting as the simplified core.
