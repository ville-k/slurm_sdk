"""Tests for custom workflow decorator composition helpers."""

from slurm.core import WorkflowHandle, workflow_decorator


def test_workflow_decorator_applies_defaults():
    ops_workflow = workflow_decorator(
        default_options={"time": "00:20:00", "mem": "1G"},
    )

    @ops_workflow
    def pipeline(value: int, ctx) -> int:
        return value

    assert isinstance(pipeline, WorkflowHandle)
    assert pipeline.is_workflow is True
    assert pipeline.sbatch_options["time"] == "00:20:00"
    assert pipeline.sbatch_options["mem"] == "1G"


def test_workflow_decorator_wraps_existing_workflow_handle():
    base = workflow_decorator(default_options={"time": "00:05:00"})

    @base
    def flow(value: int, ctx) -> int:
        return value

    retag = workflow_decorator(default_options={"partition": "orchestration"})
    wrapped = retag(flow.with_options(mem="2G"))

    assert isinstance(wrapped, WorkflowHandle)
    assert wrapped.sbatch_options["partition"] == "orchestration"
    assert wrapped.sbatch_options["mem"] == "2G"
    assert wrapped.is_workflow is True


def test_stacked_workflow_decorators_merge_options_and_middleware():
    class Marker:
        def __init__(self, name: str) -> None:
            self.name = name

    first_marker = Marker("first")
    second_marker = Marker("second")

    first = workflow_decorator(
        default_options={"time": "00:05:00"},
        middleware=(first_marker,),
    )
    second = workflow_decorator(
        default_options={"partition": "ops"},
        middleware=(second_marker,),
    )

    @second
    @first
    def chained(value: int, ctx) -> int:
        return value

    assert isinstance(chained, WorkflowHandle)
    assert chained.sbatch_options["time"] == "00:05:00"
    assert chained.sbatch_options["partition"] == "ops"
    assert tuple(chained.middleware) == (first_marker, second_marker)
