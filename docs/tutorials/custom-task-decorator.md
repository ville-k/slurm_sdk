# Tutorial: Write a Custom Task Decorator

This tutorial shows how to build a reusable task decorator that applies shared
SBATCH defaults and invocation middleware. You will create one decorator and use
it across multiple tasks.

## What you'll build

- A middleware class that normalizes task call arguments.
- A reusable `@training_task` decorator built with `task_decorator(...)`.
- A task that is submitted through the custom decorator.

## Prerequisites

- A Python environment with `slurm-sdk` installed.
- Familiarity with `@task` and `Cluster.submit(...)`.

## Step 1: Define middleware behavior

Create middleware that runs before submit and normalizes the call payload:

```python
from pathlib import Path


class NormalizeInputPath:
    def transform_call(self, ctx) -> None:
        if "input_path" in ctx.kwargs:
            ctx.kwargs["input_path"] = str(Path(ctx.kwargs["input_path"]).expanduser())
```

Expected result: every task call using this middleware gets a normalized
`input_path` argument.

## Step 2: Compose a reusable decorator

Use `task_decorator(...)` to bundle defaults and middleware:

```python
from slurm import task_decorator

training_task = task_decorator(
    default_options={"partition": "gpu", "time": "00:30:00", "mem": "8G"},
    middleware=(NormalizeInputPath(),),
)
```

Expected result: any function decorated with `@training_task` gets those SBATCH
defaults and middleware hooks.

## Step 3: Decorate and submit a task

```python
from slurm import Cluster


@training_task
def train_epoch(input_path: str, epochs: int) -> dict[str, int]:
    return {"epochs": epochs}


cluster = Cluster.from_env()
job = cluster.submit(train_epoch)(input_path="~/data/train.json", epochs=3)
result = job.get_result()
```

Expected result: task submission uses the default partition/time/memory and the
normalized path value.

## Step 4: Override when needed

You can still create variants at call sites:

```python
fast_train = train_epoch.with_options(time="00:10:00", mem="4G")
job = cluster.submit(fast_train)(input_path="~/data/train.json", epochs=1)
```

Expected result: task-specific overrides apply without rebuilding the decorator.

## Summary

You created a composable decorator that keeps business logic focused on training
code while centralizing policy and invocation behavior in one place.

Next steps:

- [How to compose middleware and workflow decorators](../how-to/composing-workflow-decorators.md)
- [Tasks and Workflows reference](../reference/api/tasks_workflows.md)
