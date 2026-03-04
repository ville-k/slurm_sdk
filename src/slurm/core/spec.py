"""Immutable task/workflow specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class TaskSpec:
    """Immutable task configuration used by generalized handles."""

    func: Callable[..., Any]
    sbatch_options: dict[str, Any]
    packaging: dict[str, Any] | None
    flags: dict[str, Any] = field(default_factory=dict)
    middleware: tuple[Any, ...] = field(default_factory=tuple)
