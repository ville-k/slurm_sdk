"""Core protocol interfaces for generalized task/workflow APIs."""

from __future__ import annotations

from typing import Any, Callable, Mapping, Protocol, runtime_checkable


@runtime_checkable
class DependencyRef(Protocol):
    """A dependency handle that can emit scheduler-level dependencies."""

    def scheduler_dependencies(self) -> list[str]:
        """Return scheduler dependency identifiers."""


@runtime_checkable
class InvocationRef(Protocol):
    """A generic invocation handle returned by task/workflow calls."""

    def wait(self, timeout: float | None = None, poll_interval: float = 5.0) -> bool:
        """Wait until completion and return success state."""

    def get(self, timeout: float | None = None) -> Any:
        """Return invocation result."""

    def scheduler_dependencies(self) -> list[str]:
        """Return scheduler dependency identifiers."""


@runtime_checkable
class InvocationRuntime(Protocol):
    """Runtime that evaluates task calls in the active execution context."""

    def invoke(
        self,
        task: "TaskLike",
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> InvocationRef:
        """Invoke task call under runtime semantics."""


@runtime_checkable
class TaskLike(Protocol):
    """Protocol representing a callable task definition."""

    name: str
    sbatch_options: Mapping[str, Any]
    packaging: Mapping[str, Any] | None
    is_workflow: bool

    def __call__(self, *args: Any, **kwargs: Any) -> InvocationRef:
        """Invoke task under active runtime."""

    def with_options(self, **options: Any) -> "TaskLike":
        """Return a cloned task with option overrides."""

    def after(self, *deps: DependencyRef) -> "TaskLike":
        """Return task with dependency bindings."""

    def clone_with(self, **changes: Any) -> "TaskLike":
        """Return a cloned task with updated attributes."""

    @property
    def unwrapped(self) -> Callable[..., Any]:
        """Return underlying Python function."""


@runtime_checkable
class WorkflowLike(TaskLike, Protocol):
    """Protocol representing a callable workflow definition."""

    is_workflow: bool
