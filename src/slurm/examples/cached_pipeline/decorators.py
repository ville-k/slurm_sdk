from __future__ import annotations

import hashlib
import json
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from slurm.context import _reset_active_runtime, _set_active_runtime
from slurm.core.decorators import task_decorator, workflow_decorator
from slurm.core.runtime import DefaultInvocationRuntime


def _default_cache_key(
    task_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> str:
    payload = {
        "task": task_name,
        "args": [repr(value) for value in args],
        "kwargs": {key: repr(value) for key, value in sorted(kwargs.items())},
    }
    serialized = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class CacheResultRef:
    """Invocation reference that supports both submitted and cached results."""

    key: str
    cache_path: Path
    job: Any | None = None
    cache_hit: bool = False
    _loaded: bool = False
    _cached_result: Any = None

    @property
    def id(self) -> Optional[str]:
        if self.job is None:
            return None
        return getattr(self.job, "id", None)

    def wait(self, timeout: float | None = None, poll_interval: float = 5.0) -> bool:
        if self.job is None:
            return True
        return self.job.wait(timeout=timeout, poll_interval=poll_interval)

    def get(self, timeout: float | None = None) -> Any:
        if self._loaded:
            return self._cached_result

        if self.cache_path.exists():
            with open(self.cache_path, "rb") as cache_file:
                # nosec B301 - cache file is written by this trusted SDK example.
                self._cached_result = pickle.load(cache_file)  # nosec B301
            self._loaded = True
            return self._cached_result

        if self.job is None:
            raise FileNotFoundError(
                "CacheResultRef has no backing job and cache file is missing at "
                f"{self.cache_path}"
            )

        result = self.job.get_result(timeout=timeout)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "wb") as cache_file:
            pickle.dump(result, cache_file)
        self._cached_result = result
        self._loaded = True
        return result

    def get_result(self, timeout: float | None = None) -> Any:
        return self.get(timeout=timeout)

    def scheduler_dependencies(self) -> list[str]:
        if self.id is None:
            return []
        return [self.id]


@dataclass
class CacheRuntime:
    """Runtime that deduplicates task submissions by stable cache key."""

    cache_dir: Path
    key_builder: Callable[[str, tuple[Any, ...], dict[str, Any]], str] = (
        _default_cache_key
    )
    default_runtime: DefaultInvocationRuntime = field(
        default_factory=DefaultInvocationRuntime
    )
    _cache: Dict[str, CacheResultRef] = field(default_factory=dict)

    def invoke(
        self,
        task_obj: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> CacheResultRef:
        task_name = str(
            getattr(task_obj, "name", getattr(task_obj, "__name__", "task"))
        )
        cache_key = self.key_builder(task_name, args, kwargs)
        if cache_key in self._cache:
            return self._cache[cache_key]

        cache_path = self.cache_dir / f"{task_name}_{cache_key}.pkl"
        if cache_path.exists():
            cached_ref = CacheResultRef(
                key=cache_key,
                cache_path=cache_path,
                job=None,
                cache_hit=True,
            )
            self._cache[cache_key] = cached_ref
            return cached_ref

        job = self.default_runtime.invoke(task_obj, args, kwargs)
        submitted_ref = CacheResultRef(
            key=cache_key,
            cache_path=cache_path,
            job=job,
            cache_hit=False,
        )
        self._cache[cache_key] = submitted_ref
        return submitted_ref


class cache_runtime:
    """Context manager binding a :class:`CacheRuntime` as active invocation runtime."""

    def __init__(self, cache_dir: str | Path, runtime: CacheRuntime | None = None):
        self._cache_dir = Path(cache_dir).expanduser()
        self.runtime = runtime or CacheRuntime(cache_dir=self._cache_dir)
        self._token = None

    def __enter__(self) -> CacheRuntime:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._token = _set_active_runtime(self.runtime)
        return self.runtime

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _reset_active_runtime(self._token)
            self._token = None


def cached_task(
    *,
    middleware: tuple[Any, ...] = (),
    default_options: Mapping[str, Any] | None = None,
) -> Callable[[Any], Any]:
    """Create a task decorator with cache-friendly default resource policy."""
    merged_options: dict[str, Any] = {
        "time": "00:03:00",
        "mem": "256M",
        "cpus_per_task": 1,
    }
    if default_options:
        merged_options.update(dict(default_options))
    return task_decorator(default_options=merged_options, middleware=middleware)


def checkpoint_workflow(
    *,
    middleware: tuple[Any, ...] = (),
    default_options: Mapping[str, Any] | None = None,
) -> Callable[[Any], Any]:
    """Create a workflow decorator tuned for checkpoint/replay orchestration."""
    merged_options: dict[str, Any] = {
        "time": "00:10:00",
        "mem": "512M",
        "cpus_per_task": 1,
    }
    if default_options:
        merged_options.update(dict(default_options))
    return workflow_decorator(default_options=merged_options, middleware=middleware)


__all__ = [
    "CacheResultRef",
    "CacheRuntime",
    "cache_runtime",
    "cached_task",
    "checkpoint_workflow",
]
