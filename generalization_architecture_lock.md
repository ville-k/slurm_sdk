# Generalization Architecture Lock

## Status

Locked for implementation of `generalization_implementation_plan.md` milestones.

## Canonical Naming

Core protocol names are locked:

- `TaskLike`
- `WorkflowLike`
- `InvocationRef`
- `DependencyRef`
- `InvocationRuntime`

Handle names are locked:

- `TaskHandle`
- `WorkflowHandle`
- `JobRef`

## Module Layout (Locked)

New extensibility modules live under `src/slurm/core/`:

- `src/slurm/core/protocols.py`
  - Public protocols (`TaskLike`, `WorkflowLike`, `InvocationRef`,
    `DependencyRef`, `InvocationRuntime`)
- `src/slurm/core/spec.py`
  - Immutable task/workflow spec types (`TaskSpec`)
- `src/slurm/core/refs.py`
  - Reference implementations (`JobRef`, placeholder types)
- `src/slurm/core/runtime.py`
  - Runtime interfaces and defaults (`ClusterRuntime`, `WorkflowRuntime`,
    extension runtime hooks)
- `src/slurm/core/middleware.py`
  - Middleware protocols and no-op/default middleware
- `src/slurm/core/handles.py`
  - `TaskHandle` and `WorkflowHandle`
- `src/slurm/core/decorators.py`
  - Generalized decorator composition helpers

Bridging/adaptation remains in existing modules during migration:

- `src/slurm/task.py`
- `src/slurm/decorators.py`
- `src/slurm/cluster.py`
- `src/slurm/context.py`
- `src/slurm/runner/placeholder.py`

## Import Boundaries

- `core/*` must not import from `cluster.py` directly unless runtime-specific.
- `handles.py` may reference legacy task internals during transition.
- `cluster.py` and `decorators.py` can depend on `core/*`.
- `runner/*` must only depend on protocol-level placeholder APIs, not concrete
  `Job`-only placeholders.

## Temporary Migration Policy

Until milestone `M8` completion:

1. Legacy and generalized APIs can coexist where needed to keep merges incremental.
2. Avoid broad compatibility wrappers unless they materially reduce migration risk.
3. New behavior should be implemented behind protocol-based surfaces first.
4. Refactors should prefer replacing concrete type checks with capability checks.
5. Test updates are allowed to reflect intentional breaking API changes.

## Deletion Targets by End of Migration

Planned removals or de-emphasis by `M8`:

- Hard `isinstance(..., SlurmTask)` submission gating.
- `Job`-only placeholder assumptions in runner argument resolution.
- Fluent clone paths that reconstruct concrete `SlurmTask` and lose extension
  metadata.

