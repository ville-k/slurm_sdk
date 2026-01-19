# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- SSH host key verification with configurable policies (`auto`, `warn`, `reject`)
- Modular runner architecture with dedicated modules for argument loading, callbacks,
  context injection, placeholder resolution, result saving, and workflow building
- Input validation module (`slurm.validation`) for job names, accounts, and partitions
- Security documentation explaining the SDK's trust model and best practices
- How-to guide for hardening SSH connections in production
- Bandit security scanning as dev dependency and CI workflow
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
