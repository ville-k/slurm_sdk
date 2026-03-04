# How to Migrate to the Generalized Task/Workflow API

## Problem

You have existing code written against concrete task/workflow internals and want
to migrate to the generalized protocol-first API.

## Prerequisites

- You are on a release that includes `slurm.core` generalized APIs.
- You can update decorators and imports in your codebase.

## Steps

1. Replace concrete wrapper assumptions with protocol-based APIs.

- Use `TaskLike` / `WorkflowLike` in extension code.
- Avoid `isinstance(x, SlurmTask)` checks in your own abstractions.

2. Migrate custom decorators to `task_decorator(...)` and `workflow_decorator(...)`.

```python
from slurm import task_decorator, workflow_decorator

gpu_task = task_decorator(default_options={"partition": "gpu"})
ops_workflow = workflow_decorator(default_options={"time": "00:20:00"})
```

3. Prefer cloning APIs for fluent customization.

- Use `.with_options(...)` for submission overrides.
- Use `.clone_with(...)` in advanced wrappers to preserve metadata.

4. Use generalized references in extension runtimes.

- Implement `scheduler_dependencies()` on custom invocation refs.
- Use `RefPlaceholder` + `register_placeholder_resolver(...)` for custom
  serialization and replay behavior.

5. Update HA-style extensions to wrapper decorators.

- Use example-scoped decorators like `ha_task` / `ha_workflow`.
- Bind extension runtimes (for example `ha_runtime()`) instead of embedding
  train/eval-specific supervisor hooks into SDK core.

## Verification

- `Cluster.submit(...)` accepts your custom task wrappers.
- Decorator stacks preserve options and middleware.
- Dependency resolution works for nested reference payloads.

## See also

- [Tasks and Workflows reference](../reference/api/tasks_workflows.md)
- [Runtime and Middleware reference](../reference/api/runtime_middleware.md)
