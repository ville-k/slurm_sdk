from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from slurm.context import _reset_active_runtime, _set_active_runtime
from slurm.core.runtime import DefaultInvocationRuntime
from slurm.decorators import task, workflow


def _stable_key(task_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    payload = {
        "task": task_name,
        "args": repr(args),
        "kwargs": {k: repr(v) for k, v in sorted(kwargs.items())},
    }
    serialized = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class ResultRef:
    """Logical result reference used by HA runtime replay semantics."""

    key: str
    job: Any
    _cached_result: Any = None
    _loaded: bool = False

    @property
    def id(self) -> Optional[str]:
        return getattr(self.job, "id", None)

    def wait(self, timeout: float | None = None, poll_interval: float = 5.0) -> bool:
        return self.job.wait(timeout=timeout, poll_interval=poll_interval)

    def get(self, timeout: float | None = None) -> Any:
        if not self._loaded:
            self._cached_result = self.job.get_result(timeout=timeout)
            self._loaded = True
        return self._cached_result

    def get_result(self, timeout: float | None = None) -> Any:
        return self.get(timeout=timeout)

    def scheduler_dependencies(self) -> list[str]:
        job_id = getattr(self.job, "id", None)
        return [job_id] if isinstance(job_id, str) and job_id else []


@dataclass
class HARuntime:
    """Minimal replay-aware runtime for fluent HA examples."""

    key_builder: Callable[[str, tuple[Any, ...], dict[str, Any]], str] = _stable_key
    _cache: Dict[str, ResultRef] = field(default_factory=dict)
    _default_runtime: DefaultInvocationRuntime = field(
        default_factory=DefaultInvocationRuntime
    )

    def invoke(
        self, task_obj: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        task_name = getattr(task_obj, "name", getattr(task_obj, "__name__", "task"))
        key = self.key_builder(str(task_name), args, kwargs)
        if key in self._cache:
            return self._cache[key]

        job = self._default_runtime.invoke(task_obj, args, kwargs)
        ref = ResultRef(key=key, job=job)
        self._cache[key] = ref
        return ref


class ha_runtime:
    """Context manager binding an ``HARuntime`` to task invocation."""

    def __init__(self, runtime: Optional[HARuntime] = None) -> None:
        self.runtime = runtime or HARuntime()
        self._token = None

    def __enter__(self) -> HARuntime:
        self._token = _set_active_runtime(self.runtime)
        return self.runtime

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _reset_active_runtime(self._token)
            self._token = None


def ha_task(*, middleware: tuple[Any, ...] = (), **task_kwargs: Any):
    """Create an HA-marked task decorator using generalized task handles."""

    def decorator(target: Any) -> Any:
        if hasattr(target, "func") and hasattr(target, "with_flags"):
            handle = target
        else:
            handle = task(**task_kwargs)(target)
        if middleware and hasattr(handle, "with_middleware"):
            handle = handle.with_middleware(*middleware)
        if hasattr(handle, "with_flags"):
            handle = handle.with_flags(ha_task=True)
        return handle

    return decorator


def ha_workflow(*, middleware: tuple[Any, ...] = (), **workflow_kwargs: Any):
    """Create an HA-marked workflow decorator using generalized workflow handles."""

    def decorator(target: Any) -> Any:
        if hasattr(target, "func") and getattr(target, "is_workflow", False):
            handle = target
        else:
            handle = workflow(**workflow_kwargs)(target)
        if middleware and hasattr(handle, "with_middleware"):
            handle = handle.with_middleware(*middleware)
        if hasattr(handle, "with_flags"):
            handle = handle.with_flags(ha_workflow=True)
        return handle

    return decorator


__all__ = [
    "HARuntime",
    "ResultRef",
    "ha_runtime",
    "ha_task",
    "ha_workflow",
]
