"""Reference implementations for generalized invocation/dependency APIs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from slurm.job import Job


@dataclass(frozen=True)
class RefPlaceholder:
    """Serialized placeholder for deferred reference resolution."""

    ref_type: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class JobRef:
    """Adapter exposing ``Job`` through the generalized reference protocol."""

    job: Job

    @property
    def id(self) -> str:
        return self.job.id

    def wait(self, timeout: float | None = None, poll_interval: float = 5.0) -> bool:
        return self.job.wait(timeout=timeout, poll_interval=poll_interval)

    def get(self, timeout: float | None = None) -> Any:
        return self.job.get_result(timeout=timeout)

    def get_result(self, timeout: float | None = None) -> Any:
        return self.get(timeout=timeout)

    def scheduler_dependencies(self) -> list[str]:
        return [self.job.id]

    def __getattr__(self, name: str) -> Any:
        return getattr(self.job, name)
