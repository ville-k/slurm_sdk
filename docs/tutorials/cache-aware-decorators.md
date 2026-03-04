# Tutorial: Build Cache-Aware Decorators

This tutorial shows how to compose a custom task decorator, a custom workflow
decorator, and a runtime context that replays cached results instead of
resubmitting identical work.

## What you'll build

- A `@cached_task` decorator for task policy defaults.
- A `@checkpoint_workflow` decorator for orchestration defaults.
- A cache runtime that returns cached references for repeated calls.

## Prerequisites

- A Slurm cluster reachable by the SDK (SSH or local backend).
- A Python environment with `slurm-sdk` installed.
- Familiarity with `@task`, `@workflow`, and `Cluster.submit(...)`.

## Step 1: Define custom decorators

Create reusable decorators with `task_decorator(...)` and
`workflow_decorator(...)`:

```python
from slurm.core.decorators import task_decorator, workflow_decorator

cached_task = task_decorator(
    default_options={"time": "00:03:00", "mem": "256M", "cpus_per_task": 1}
)
checkpoint_workflow = workflow_decorator(
    default_options={"time": "00:10:00", "mem": "512M", "cpus_per_task": 1}
)
```

Expected result: these decorators encapsulate scheduling defaults while task
and workflow functions stay focused on business logic.

## Step 2: Add a cache runtime

Bind a runtime that computes a stable key from task name and call arguments:

```python
with cache_runtime(cache_dir):
    first_ref = build_feature_summary(dataset="tiny-imagenet", epoch=3, scale=2)
    second_ref = build_feature_summary(dataset="tiny-imagenet", epoch=3, scale=2)
```

Expected result: `first_ref` and `second_ref` resolve to the same logical work
reference, so only one submission happens for repeated calls in the same runtime.

## Step 3: Demonstrate replay from persisted cache

Open a fresh runtime and call the same task again:

```python
with cache_runtime(cache_dir):
    replay_ref = build_feature_summary(dataset="tiny-imagenet", epoch=3, scale=2)
    replay_result = replay_ref.get_result()
```

Expected result: if a cached artifact exists, the runtime returns a cache-backed
reference and avoids a new task submission.

## Step 4: Run the full example

Run the included example module:

```bash
uv run python -m slurm.examples.cached_pipeline.workflow \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --dataset tiny-imagenet \
  --epoch 3 \
  --scale 2
```

Expected result: the workflow prints:

- `Workflow complete. Cache state file: ...`
- `Replay cache hit: True`

## Summary

You composed custom decorators and a runtime to make idempotent replay behavior
available without putting caching logic into task business code.

Next steps:

- [How to compose middleware and workflow decorators](../how-to/composing-workflow-decorators.md)
- [Understanding cache-aware decorator composition](../explanation/cache-aware-decorators.md)
- [Tasks and Workflows reference](../reference/api/tasks_workflows.md)
