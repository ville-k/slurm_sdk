"""Tests for nested generalized placeholder resolution."""

from slurm.core import RefPlaceholder
from slurm.runner.placeholder import register_placeholder_resolver, resolve_placeholder


def test_nested_ref_resolution_across_structures():
    register_placeholder_resolver(
        "echo",
        lambda payload, _job_base_dir: payload["value"],
    )

    value = {
        "a": RefPlaceholder(ref_type="echo", payload={"value": 1}),
        "b": [RefPlaceholder(ref_type="echo", payload={"value": 2}), 3],
        "c": (RefPlaceholder(ref_type="echo", payload={"value": 4}), 5),
    }

    assert resolve_placeholder(value) == {
        "a": 1,
        "b": [2, 3],
        "c": (4, 5),
    }
