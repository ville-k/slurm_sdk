"""Container packaging integration test tasks and workflows.

This module contains task and workflow definitions used by container
packaging integration tests. Container packaging configuration is applied
via cluster defaults in the integration tests.

It is included in the slurm package wheel so tasks and workflows can be
imported when running remotely on Slurm clusters.

Note: This is NOT a user-facing example. It exists here (rather than in
tests/) because it must be importable on remote clusters.
"""

from slurm.decorators import task, workflow
from slurm.workflow import WorkflowContext


# Task Definitions


@task(time="00:01:00", mem="512M")
def container_add(a: int, b: int) -> int:
    """Add two numbers (for container packaging)."""
    return a + b


@task(time="00:01:00", mem="512M")
def container_multiply(a: int, b: int) -> int:
    """Multiply two numbers (for container packaging)."""
    return a * b


@task(time="00:01:00", mem="512M")
def container_failing_task() -> int:
    """Task that intentionally fails (for container packaging)."""
    raise ValueError("This task is designed to fail")


# Workflow Definitions


@workflow(time="00:05:00", mem="1G")
def simple_container_workflow(x: int, ctx: WorkflowContext):
    """Simple workflow with containerized tasks.

    Container packaging config is applied via cluster defaults.
    """
    job_a = container_add(x, 10)
    a = job_a.get_result()

    job_b = container_multiply(a, 2)
    return job_b.get_result()


# Task with inherit packaging (for use inside containerized workflows)


@task(time="00:01:00", mem="512M", packaging="inherit")
def inheriting_add(a: int, b: int) -> int:
    """Add two numbers (inherits packaging from parent workflow)."""
    return a + b


# Simplest possible workflow for debugging container packaging


@workflow(time="00:02:00", mem="512M")
def minimal_workflow() -> int:
    """Absolute minimal workflow that just returns a constant.

    Used to debug workflow + container packaging completion issues.
    """
    return 42


@workflow(time="00:02:00", mem="512M")
def workflow_with_param_no_context(x: int) -> int:
    """Workflow that takes a parameter but NO WorkflowContext.

    Used to isolate whether the issue is with parameters in general
    or specifically with WorkflowContext.
    """
    return x * 3


# Simple workflow that doesn't submit tasks (for basic container packaging testing)


@workflow(time="00:02:00", mem="512M")
def simple_value_workflow(x: int, ctx: WorkflowContext) -> int:
    """Simple workflow that just returns a value without submitting tasks.

    Used to test basic workflow + container packaging.
    """
    return x * 2


# Simple workflow for testing container packaging inheritance


@workflow(time="00:05:00", mem="1G")
def simple_inheriting_workflow(x: int, ctx: WorkflowContext):
    """Workflow that calls tasks with inherit packaging.

    When this workflow is containerized, tasks will inherit the container
    packaging without trying to rebuild containers (no docker-in-docker).
    """
    job = inheriting_add(x, 10)
    return job.get_result()


@workflow(time="00:05:00", mem="1G")
def inner_container_workflow(x: int, ctx: WorkflowContext):
    """Inner workflow for nested workflow testing."""
    job = container_add(x, 5)
    return job.get_result()


@workflow(time="00:05:00", mem="1G")
def outer_container_workflow(x: int, ctx: WorkflowContext):
    """Outer workflow that calls inner workflow."""
    inner_job = inner_container_workflow(x)
    result = inner_job.get_result()

    final_job = container_add(result, 10)
    return final_job.get_result()


@workflow(time="00:05:00", mem="1G")
def failing_container_workflow(ctx: WorkflowContext):
    """Workflow that should fail."""
    job = container_failing_task()
    return job.get_result()


# Task with its own container packaging (for with_dependencies testing)


@task(time="00:01:00", mem="512M", packaging="container")
def task_with_own_container(x: int) -> int:
    """Task that has its own container packaging configuration.

    This task needs its container pre-built via with_dependencies().
    Without pre-building, this would fail inside a containerized workflow
    because there's no docker/podman to build the image.
    """
    return x * 10


@workflow(time="00:05:00", mem="1G")
def workflow_with_dependency_task(x: int, ctx: WorkflowContext):
    """Workflow that calls a task with its own container.

    This workflow should be submitted with:
        cluster.submit(workflow).with_dependencies([task_with_own_container])(x)

    The task_with_own_container needs to have its container pre-built
    before the workflow runs.
    """
    job = task_with_own_container(x)
    result = job.get_result()
    # Also call a regular task to verify both work
    job2 = container_add(result, 5)
    return job2.get_result()
