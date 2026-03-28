# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- How-to guide for creating custom task and workflow decorators using existing
  `@task`, `@workflow`, and `with_options()` APIs
- `write_file()` and `close()` methods on `BackendBase` interface for unified
  file operations and explicit resource cleanup

### Changed

- Callback exceptions are now logged at WARNING level with full tracebacks
  (previously logged at DEBUG)
- SSH backend `host_key_policy` default changed from `"warn"` to `"reject"` for
  improved security; pass `host_key_policy="warn"` to restore previous behavior
- Extracted `_dispatch_callbacks()` helper on `Cluster` to deduplicate callback
  dispatch logic
- Consolidated `_runner_impl.py` into the `runner/` package; a thin
  backwards-compatible shim remains for external references
- Slurmfile TOML modification now uses `tomlkit` for proper round-trip parsing
  instead of fragile line-by-line string manipulation

### Fixed

- Temp file leak in `Job.get_result()` when downloading results via SSH; files
  are now cleaned up in a `finally` block
- Thread-safety issue with `Job` status cache; reads and writes to
  `_status_cache`, `_status_cache_time`, and `_completed` are now protected
  by an `RLock`
- Race condition in `_job_pollers` dict access between main and poller threads
- Job name validation and quoting in rendered sbatch scripts
- Replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)`
- Environment metadata files are now written with `0o600` permissions
- `LocalBackend.execute_command()` no longer uses `shell=True`

### Dependencies

- Added `tomlkit>=0.12` as a required dependency

## [0.4.5] - 2026-02-05

### Added

- Interactive TUI commands (requires `pip install slurm-sdk[tui]`):
  - `slurm dash` - Interactive dashboard for monitoring jobs and cluster status
    - Two-pane layout with navigation tree and detail panel
    - Displays user's jobs, account jobs, and partition status
    - Hybrid refresh: auto-refresh when focused, toggleable with 'a' key
    - Job cancellation support with keyboard shortcut
  - `slurm docs` - Documentation viewer with full-text search
    - Browsable navigation tree following mkdocs.yml structure
    - Markdown rendering with syntax highlighting
    - SQLite FTS5-powered full-text search with prefix matching and context snippets
    - Keyboard navigation: `/` to search, arrow keys and Enter for results
    - Documentation bundled with package for offline access
- Container-aware job connection: `slurm jobs connect` now automatically attaches to the container running inside a job when using `container` packaging:
  - Containers are named `slurm-sdk-{pre_submission_id}` at submission time (with `_{task_id}` suffix for array jobs)
  - Connect command detects container jobs and uses `--container-name` flag to attach
  - New `--no-container` flag to bypass container attachment and connect to bare node
  - Multi-node job support with interactive node selection prompt
  - Array job support: each array task gets a unique container name to prevent collisions
- `slurm jobs cancel` command to cancel running or pending jobs:
  - Shows job name and state before cancelling
  - Prompts for confirmation (use `--force` to skip)
  - Handles terminal states gracefully
- `slurm` CLI command for job and cluster management:
  - `slurm jobs list` - view jobs in the SLURM queue with color-coded states
  - `slurm jobs show <job-id>` - display detailed job information
  - `slurm jobs watch` - live dashboard for monitoring jobs in real-time
  - `slurm jobs connect <job-id>` - attach interactive shell to running jobs
  - `slurm jobs debug <job-id>` - attach debugger to running jobs with SSH port forwarding
  - `slurm cluster list` - list configured environments from Slurmfile (offline)
  - `slurm cluster show` - view cluster partition information
  - `slurm cluster watch` - live dashboard for monitoring cluster partition utilization
  - `slurm cluster connect` - open SSH session to cluster login node
  - Rich output formatting with tables and panels
  - User-friendly error handling with actionable hints
- MCP (Model Context Protocol) server for AI assistant integration:
  - `slurm mcp run` - start MCP server exposing SLURM SDK APIs
  - `slurm mcp status` - display MCP server configuration
  - Tools: `list_jobs`, `get_job`, `cancel_job`, `list_partitions`, `list_environments`
  - Resources: `slurm://queue`, `slurm://jobs/{id}`, `slurm://partitions`, `slurm://config`
  - Supports stdio (Claude Desktop) and HTTP transports
- `DebugCallback` for enabling debugpy debugging in SLURM jobs:
  - Configurable via Slurmfile or environment variables (`DEBUGPY_PORT`, `DEBUGPY_WAIT`)
  - Automatic debugpy setup when jobs start on compute nodes
  - Integration with `slurm jobs debug` CLI command
- SSH host key verification with configurable policies (`auto`, `warn`, `reject`)
- Modular runner architecture with dedicated modules for argument loading, callbacks,
  context injection, placeholder resolution, result saving, and workflow building
- Input validation module (`slurm.validation`) for job names, accounts, and partitions
- Security documentation explaining the SDK's trust model and best practices
- How-to guide for hardening SSH connections in production
- Bandit security scanning as dev dependency and CI workflow
- GitHub Actions CI workflow for running unit tests on PRs and main branch pushes
- Integration tests in CI with Docker-based Slurm cluster, gated by unit tests, lint, and security checks
- Basic monitoring APIs for job status tracking
- Mermaid diagrams throughout documentation for improved understanding:
  - Parallelization pattern diagrams (fan-out/fan-in, pipeline, sweep, dynamic dependencies)
  - Workflow execution sequence diagram and directory structure
  - System architecture and component relationship diagrams
  - Callback timeline and execution loci diagram
  - Job state machine diagram in docstrings
  - Task API flow diagram in docstrings
  - Two-phase submission pattern diagram in docstrings

### Changed

- Refactored `slurm.runner` from monolithic module into focused package with 7 modules

- Removed legacy underscore-prefixed function exports from `slurm.runner` module:

  - `_run_callbacks` → `run_callbacks`
  - `_function_wants_workflow_context` → `function_wants_workflow_context`
  - `_bind_workflow_context` → `bind_workflow_context`
  - `_write_environment_metadata` → `write_environment_metadata`

- Integration test registry port changed from 5000 to 20002 to avoid conflicts with
  common services (e.g., macOS AirPlay Receiver)

- Local backend now uses `shell=False` for SLURM commands (more secure)

- Default SSH host key policy changed from `auto` to `warn` (logs warning for unknown hosts)

- Job script permissions now default to `0o750` (configurable via `script_permissions`)

- SSH passwords are cleared from memory immediately after successful authentication

- Documentation restructured to follow Diataxis framework with four distinct types:

  - Renamed `docs/guides/` to `docs/how-to/` for consistency
  - Moved architecture content from `docs/reference/architecture/` to `docs/explanation/`
  - Updated navigation in `mkdocs.yml` to reflect new structure

### Security

- Fixed missing `shlex.quote()` calls in SSH backend path handling
- Added security-focused Bandit `# nosec` comments with justifications throughout codebase
- Improved temporary directory handling to use secure paths

### Fixed

- Fixed potential issue with `update_job_metadata` when job ID is None (now defaults to "unknown")
- Replaced Linux-only `flock` with cross-platform `mkdir`-based locking in wheel
  packaging for macOS compatibility
- `Cluster.get_job()` now correctly extracts `pre_submission_id` from stdout path for jobs with timestamp-based IDs (e.g., `20260118_090851_0f492fb`)
- Type annotations added to public APIs to resolve mkdocstrings warnings:
  - `Cluster.from_file()`, `Cluster.add_argparse_args()`, `Cluster.from_args()`, `Cluster.submit()`
  - `task()` and `workflow()` decorator return types
  - `SlurmTask.unwrapped`, `.map()`, `.after()`, `.with_options()`, `.with_dependencies()`

## [0.4.4] - 2026-01-10

### Added

- Workflow support with `@workflow` decorator for multi-step job orchestration
- Monitoring APIs for tracking job status and progress
- Container packaging integration with Pyxis/enroot
- Native SLURM array jobs support for efficient batch processing
- Signal handlers for RichLoggerCallback
- Environment inheritance for workflow child tasks
- Local backend for testing without SLURM access
- Python 3.9 support

### Changed

- Improved container packaging reproducibility
- API simplification and cleanup
- Enhanced error messaging with actionable resolution steps

### Fixed

- Container integration issues with workflow execution
