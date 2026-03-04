"""Tests for invocation runtime binding and switching."""

from dataclasses import dataclass
from typing import Any

from slurm.context import (
    _clear_active_context,
    _reset_active_runtime,
    _set_active_runtime,
)
from slurm.decorators import task


@task(time="00:01:00")
def compute(value: int) -> int:
    return value + 1


@dataclass
class StubRuntime:
    result: Any
    last_call: tuple[Any, tuple[Any, ...], dict[str, Any]] | None = None

    def invoke(self, task, args, kwargs):
        self.last_call = (task, args, kwargs)
        return self.result


def test_task_call_uses_bound_runtime():
    _clear_active_context()
    runtime = StubRuntime(result="runtime-result")

    token = _set_active_runtime(runtime)
    try:
        result = compute(5)
    finally:
        _reset_active_runtime(token)

    assert result == "runtime-result"
    assert runtime.last_call is not None
    _, args, kwargs = runtime.last_call
    assert args == (5,)
    assert kwargs == {}


def test_runtime_context_isolation_after_reset():
    _clear_active_context()
    runtime = StubRuntime(result="x")

    token = _set_active_runtime(runtime)
    _reset_active_runtime(token)

    # With no active runtime and no cluster/workflow context, task call must fail.
    try:
        compute(1)
    except RuntimeError as exc:
        assert "must be called within a Cluster context or @workflow" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected RuntimeError when no runtime/context is active")
