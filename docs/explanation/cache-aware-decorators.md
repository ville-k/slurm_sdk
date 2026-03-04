# Cache-Aware Decorators Explained

This document explains the design behind cache-aware custom task and workflow
decorators in the example `slurm.examples.cached_pipeline`.

## Overview

The example demonstrates three composable pieces:

1. **Decorator composition** for default policy:
   - `@cached_task` sets task-level SBATCH defaults.
   - `@checkpoint_workflow` sets workflow-level SBATCH defaults.
1. **Runtime substitution** for call semantics:
   - `cache_runtime(...)` binds a custom invocation runtime in context.
1. **Logical invocation references**:
   - `CacheResultRef` represents either a submitted job or a cache hit.

The task implementation stays focused on application logic. Cache and replay
concerns are moved to decorator/runtime infrastructure.

## Context

In long-running pipelines, identical logical work can be triggered repeatedly:

- Workflow retries after preemption.
- Partial restarts after orchestration failure.
- Re-runs with unchanged inputs for validation.

Without a cache-aware layer, each repeated call produces duplicate submissions.
The example shows how to avoid this by keying work identity on `(task, args, kwargs)` and replaying persisted results.

## Key Concepts

### Decorators define policy, not behavior branching

The decorators encapsulate stable defaults (time/memory/CPU policy), but they
do not embed caching conditionals inside task code.

This reduces cognitive load for users writing task functions: they only express
business behavior, while orchestration policy is composed externally.

### Runtime controls invocation semantics

`SlurmTask.__call__` delegates to the active runtime. By binding a cache-aware
runtime, identical calls can return cache-backed references instead of always
submitting a new job.

This is the same extension pattern used for other cross-cutting concerns such as
fault tolerance, tracing, or policy enforcement.

### Cache references preserve dependency surface

`CacheResultRef` keeps a dependency-compatible shape:

- For submitted work: returns scheduler dependency IDs.
- For cache hits: returns no scheduler dependency IDs.

This allows downstream APIs to treat both paths uniformly at call sites.

## Trade-offs and Alternatives

- **Stable key by `repr(...)`**:
  - Pro: simple and generic for examples.
  - Con: fragile for mutable/unordered structures.
  - Production extension: key serializers per input schema.
- **Pickle artifact format**:
  - Pro: supports arbitrary Python return types.
  - Con: portability and trust constraints.
  - Production extension: typed artifact formats (JSON/Parquet/model stores).
- **Local filesystem cache directory**:
  - Pro: minimal setup for shared-run directories.
  - Con: limited multi-cluster/multi-region sharing.
  - Production extension: remote artifact backends with consistency controls.

## Common Misconceptions

- **“Decorator defaults are enough for caching.”**
  They are not. Defaults shape policy, but replay behavior requires runtime
  invocation control.

- **“Cache hits and submitted jobs should be different API types.”**
  Distinct internals are fine, but exposing a unified reference interface
  minimizes workflow complexity.

- **“Caching belongs in every task body.”**
  Embedding cache checks in task code couples business logic to orchestration
  concerns and increases duplication.

## Conclusion

The cache-aware example shows a practical generalization pattern:

- Keep tasks/workflows imperative and business-focused.
- Move cross-cutting behavior into composable decorators and runtimes.
- Represent both submitted and replayed work through a common reference model.

This pattern scales naturally toward production capabilities such as artifact
versioning, cache invalidation policies, lineage tracking, and resumable
multi-stage pipelines.
