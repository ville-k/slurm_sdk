"""Generalized task/workflow handle implementations."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from slurm.core.spec import TaskSpec
from slurm.task import SlurmTask


class TaskHandle(SlurmTask):
    """Generalized task handle with immutable spec metadata."""

    def __init__(
        self,
        func,
        sbatch_options: Dict[str, Any] | None = None,
        packaging: Dict[str, Any] | None = None,
        *,
        middleware: tuple[Any, ...] = (),
        flags: Optional[Mapping[str, Any]] = None,
        **slurm_options,
    ) -> None:
        self._middleware = tuple(middleware)
        self._flags = dict(flags or {})
        super().__init__(
            func=func,
            sbatch_options=sbatch_options,
            packaging=packaging,
            **slurm_options,
        )
        self._sync_spec()
        if self._flags.get("is_workflow"):
            self._is_workflow = True

    def _sync_spec(self) -> None:
        self.spec = TaskSpec(
            func=self.func,
            sbatch_options=dict(self.sbatch_options),
            packaging=self.packaging.copy() if self.packaging else None,
            flags=dict(self._flags),
            middleware=tuple(self._middleware),
        )

    @property
    def name(self) -> str:
        return self.sbatch_options.get("job_name", self.func.__name__)

    @property
    def is_workflow(self) -> bool:
        return bool(
            self._flags.get("is_workflow") or getattr(self, "_is_workflow", False)
        )

    @property
    def middleware(self) -> tuple[Any, ...]:
        return self._middleware

    @property
    def flags(self) -> dict[str, Any]:
        return dict(self._flags)

    def clone_with(
        self,
        *,
        func=None,
        sbatch_options: Dict[str, Any] | None = None,
        packaging: Dict[str, Any] | None = None,
        slurm_options: Dict[str, Any] | None = None,
        pending_dependencies: list[Any] | None = None,
        container_dependencies: list[Any] | None = None,
        is_workflow: bool | None = None,
        middleware: tuple[Any, ...] | None = None,
        flags: Mapping[str, Any] | None = None,
    ) -> "TaskHandle":
        next_flags = dict(self._flags) if flags is None else dict(flags)
        if is_workflow is not None:
            next_flags["is_workflow"] = bool(is_workflow)

        clone = self.__class__(
            func=func or self.func,
            sbatch_options=sbatch_options
            if sbatch_options is not None
            else dict(self.sbatch_options),
            packaging=packaging
            if packaging is not None
            else (self.packaging.copy() if self.packaging else None),
            middleware=self._middleware if middleware is None else tuple(middleware),
            flags=next_flags,
            **(
                slurm_options if slurm_options is not None else dict(self.slurm_options)
            ),
        )
        clone._pending_dependencies = (
            list(self._pending_dependencies)
            if pending_dependencies is None
            else list(pending_dependencies)
        )
        clone._container_dependencies = (
            list(self._container_dependencies)
            if container_dependencies is None
            else list(container_dependencies)
        )
        if clone.is_workflow:
            clone._is_workflow = True
        clone._sync_spec()
        return clone

    def with_middleware(self, *middleware: Any) -> "TaskHandle":
        return self.clone_with(middleware=tuple(self._middleware) + tuple(middleware))

    def with_flags(self, **flags: Any) -> "TaskHandle":
        merged = dict(self._flags)
        merged.update(flags)
        return self.clone_with(flags=merged)


class WorkflowHandle(TaskHandle):
    """Generalized workflow handle."""

    def __init__(
        self,
        func,
        sbatch_options: Dict[str, Any] | None = None,
        packaging: Dict[str, Any] | None = None,
        *,
        middleware: tuple[Any, ...] = (),
        flags: Optional[Mapping[str, Any]] = None,
        **slurm_options,
    ) -> None:
        merged_flags = dict(flags or {})
        merged_flags["is_workflow"] = True
        super().__init__(
            func=func,
            sbatch_options=sbatch_options,
            packaging=packaging,
            middleware=middleware,
            flags=merged_flags,
            **slurm_options,
        )
        self._is_workflow = True

    @property
    def is_workflow(self) -> bool:
        return True

    def clone_with(
        self,
        *,
        is_workflow: bool | None = None,
        **changes: Any,
    ) -> "WorkflowHandle":
        return super().clone_with(
            is_workflow=True if is_workflow is None else is_workflow, **changes
        )  # type: ignore[return-value]
