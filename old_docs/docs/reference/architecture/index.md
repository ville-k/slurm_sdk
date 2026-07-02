# Architecture

Background material that clarifies concepts, architecture decisions, and design rationale. Consult these when you want to understand the "why" behind the SDK.

## Core Architecture

- **[Overall Architecture](architecture.md)**: Complete system architecture overview
  - Module responsibilities and relationships
  - Public API surface
  - Implementation details
  - Extension points

## Key Features

- **[Context-Based Execution](context_execution.md)**: Automatic job submission via context managers
  - Design philosophy and motivation
  - Context tracking with contextvars
  - Automatic dependency extraction from Job arguments
  - Explicit dependencies with `.after()` method
  - Local testing with `.unwrapped`
  - Runtime option overrides with `.with_options()`
  - Stateless task composition patterns
  - Type safety with Generic[T] and TYPE_CHECKING

- **[Hierarchical Directory Structure](directory_structure.md)**: Human-friendly job organization
  - Task-based directory layout
  - Sortable timestamp format
  - Workflow nesting patterns
  - Array job grouped structure
  - Metadata file format
  - Querying and cleanup strategies

## Extension Points

- **[Callbacks](callbacks.md)**: Lifecycle hooks and event handling
- **[Configuration](configuration.md)**: Slurmfile and environment setup
- **[Runner](runner.md)**: Job execution environment

## Design Principles

The Slurm SDK follows these core principles throughout:

1. **Pythonic**: Feels natural to Python developers
2. **Minimal**: Small API surface, extend existing concepts
3. **Composable**: Features work well together
4. **Testable**: Can test locally before cluster deployment
5. **Explicit**: No hidden magic, clear what runs where
6. **Type-safe**: Leverage type hints for better IDE support
