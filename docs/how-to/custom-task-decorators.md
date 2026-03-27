# How to create custom task and workflow decorators

## Problem

You have teams or projects that reuse the same SBATCH options across many tasks
(e.g., GPU partitions, time limits, packaging settings). You want to define
reusable decorators that encode these defaults, so individual task definitions
stay concise and consistent.

## Prerequisites

- Familiarity with the `@task` and `@workflow` decorators
- Understanding of SBATCH options and packaging configuration

## Define a custom task decorator

The `@task` decorator accepts SBATCH options as keyword arguments and returns a
`SlurmTask`. Since it is a regular Python function, you can wrap it to create
reusable decorator factories.

### Simple wrapper function

Define a function that calls `@task` with your defaults:

```python
from slurm import task


def gpu_task(**overrides):
    """Task decorator pre-configured for GPU jobs."""
    defaults = {
        "partition": "gpu",
        "gpus": 1,
        "mem": "32G",
        "time": "04:00:00",
    }
    return task(**{**defaults, **overrides})
```

Use it exactly like `@task`:

```python
@gpu_task()
def train_model(config: dict) -> dict:
    return train(config)


@gpu_task(gpus=4, mem="128G")
def train_large_model(config: dict) -> dict:
    return train(config)
```

### Using `functools.partial`

For even less boilerplate, use `functools.partial`:

```python
from functools import partial
from slurm import task

gpu_task = partial(task, partition="gpu", gpus=1, mem="32G", time="04:00:00")
```

**Note:** With `partial`, the decorator must always be called with parentheses:
`@gpu_task()`. If you want to support bare `@gpu_task` (no parentheses),
use the wrapper function approach instead.

## Define a custom workflow decorator

The same patterns work for `@workflow`:

```python
from slurm import workflow


def long_workflow(**overrides):
    """Workflow decorator for long-running orchestration jobs."""
    defaults = {
        "time": "12:00:00",
        "mem": "16G",
        "partition": "orchestration",
    }
    return workflow(**{**defaults, **overrides})


@long_workflow()
def training_pipeline(config: dict, ctx):
    prep_job = preprocess(config["data_path"])
    train_job = train_model.after(prep_job)(config)
    return train_job.get_result()
```

## Create task variants with `with_options`

You don't always need a decorator. For ad-hoc variants of an existing task, use
`with_options()`:

```python
@task(time="02:00:00", cpus_per_task=4)
def process(data_path: str) -> dict:
    return run_processing(data_path)


@workflow
def my_workflow(ctx):
    # Standard run
    job1 = process("small_dataset.csv")

    # GPU variant of the same task
    gpu_job = process.with_options(partition="gpu", gpus=1)("large_dataset.csv")

    return [job1.get_result(), gpu_job.get_result()]
```

`with_options` returns a new `SlurmTask` with merged options. The original task
is unchanged.

## Stack and compose decorators

Decorator factories and `with_options` compose naturally:

```python
@gpu_task()
def train(config: dict) -> dict:
    return run_training(config)


# Create a priority variant at import time
priority_train = train.with_options(partition="gpu-priority", account="urgent")

# Use both in a workflow
@workflow
def experiment(ctx):
    normal_job = train({"lr": 0.001})
    fast_job = priority_train({"lr": 0.001})
    return [normal_job.get_result(), fast_job.get_result()]
```

## Encode packaging defaults

Custom decorators can also set packaging configuration:

```python
def containerized_task(image: str, **overrides):
    """Task decorator for pre-built container images."""
    return task(packaging=f"container:{image}", **overrides)


@containerized_task("pytorch/pytorch:2.1-cuda12.1", time="08:00:00", gpus=1)
def distributed_train(config: dict) -> dict:
    return train_distributed(config)
```

## Verification

Inspect a decorated task to confirm options are applied:

```python
@gpu_task(mem="64G")
def my_task(x: int) -> int:
    return x * 2

assert my_task.sbatch_options["partition"] == "gpu"
assert my_task.sbatch_options["gpus"] == 1
assert my_task.sbatch_options["mem"] == "64G"  # override applied

# Verify with_options produces a new task
variant = my_task.with_options(gpus=4)
assert variant.sbatch_options["gpus"] == 4
assert my_task.sbatch_options["gpus"] == 1  # original unchanged
```

## See also

- [API Reference: Tasks and Workflows](../reference/api/tasks_workflows.md) for
  the full `SlurmTask` API
- [Parallelization Patterns](parallelization_patterns.md) for using tasks with
  `.map()` and `.after()`
