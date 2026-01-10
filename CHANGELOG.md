# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
- Documentation restructured to follow Diataxis framework with four distinct types:
  - Renamed `docs/guides/` to `docs/how-to/` for consistency
  - Moved architecture content from `docs/reference/architecture/` to `docs/explanation/`
  - Updated navigation in `mkdocs.yml` to reflect new structure

### Fixed
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
