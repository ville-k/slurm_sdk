"""
Workflow tasks for integration testing.
This module is included in the slurm package wheel.
"""

from slurm import task
from slurm.decorators import workflow
from slurm.workflow import WorkflowContext


@task(time="00:01:00", cpus_per_task=1, mem="100M")
def simple_task(x: int) -> int:
    """A simple task that doubles a number."""
    return x * 2


@task(time="00:01:00", cpus_per_task=1, mem="100M")
def add_task(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


@workflow(time="00:10:00", cpus_per_task=1, mem="100M")
def simple_workflow(value: int, ctx: WorkflowContext):
    """A simple workflow that orchestrates tasks."""
    job1 = simple_task(value)
    result1 = job1.get_result()

    job2 = simple_task(value + 1)
    result2 = job2.get_result()

    # Combine results
    final_job = add_task(result1, result2)
    return final_job.get_result()


@workflow(time="00:05:00", cpus_per_task=1, mem="100M")
def inner_workflow(x: int, ctx: WorkflowContext):
    """Inner workflow."""
    job = simple_task(x)
    return job.get_result()


@workflow(time="00:08:00", cpus_per_task=1, mem="100M")
def outer_workflow(x: int, ctx: WorkflowContext):
    """Outer workflow that calls inner workflow."""
    inner_job = inner_workflow(x)
    result = inner_job.get_result()

    # Use the result from inner workflow
    final_job = add_task(result, 10)
    return final_job.get_result()


@task(time="00:01:00", cpus_per_task=1, mem="100M")
def failing_task() -> int:
    """A task that always fails."""
    raise ValueError("Intentional failure for testing")


@workflow(time="00:05:00", cpus_per_task=1, mem="100M")
def failing_workflow(ctx: WorkflowContext):
    """A workflow with a failing task."""
    job = failing_task()
    return job.get_result()


@task(time="00:01:00", cpus_per_task=1, mem="100M")
def quick_task(x: int) -> int:
    """A very quick task."""
    return x + 1


@workflow(time="00:08:00", cpus_per_task=1, mem="100M")
def sequential_workflow(start: int, ctx: WorkflowContext):
    """Submit tasks sequentially to test submission throughput."""
    results = []
    for i in range(5):
        job = quick_task(start + i)
        result = job.get_result()
        results.append(result)

    return sum(results)
