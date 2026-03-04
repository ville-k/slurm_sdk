"""Tests for generalized TaskHandle/WorkflowHandle cloning behavior."""

from slurm.core import TaskHandle, WorkflowHandle
from slurm.decorators import task, workflow


@task(time="00:01:00", mem="1G")
def add_one(value: int) -> int:
    return value + 1


@workflow(time="00:02:00", mem="2G")
def simple_workflow(value: int, ctx) -> int:
    return add_one(value)


def test_task_decorator_returns_task_handle():
    assert isinstance(add_one, TaskHandle)
    assert add_one.__name__ == "add_one"


def test_workflow_decorator_returns_workflow_handle():
    assert isinstance(simple_workflow, WorkflowHandle)
    assert simple_workflow.is_workflow is True


def test_clone_with_preserves_class_and_immutability():
    cloned = add_one.clone_with(sbatch_options={"time": "00:10:00", "mem": "4G"})

    assert isinstance(cloned, TaskHandle)
    assert cloned is not add_one
    assert cloned.sbatch_options["time"] == "00:10:00"
    assert cloned.sbatch_options["mem"] == "4G"
    assert add_one.sbatch_options["time"] == "00:01:00"
    assert add_one.sbatch_options["mem"] == "1G"


def test_with_options_uses_clone_semantics():
    tuned = add_one.with_options(mem="8G", partition="gpu")

    assert isinstance(tuned, TaskHandle)
    assert tuned.sbatch_options["mem"] == "8G"
    assert tuned.sbatch_options["partition"] == "gpu"
    assert add_one.sbatch_options.get("partition") is None
