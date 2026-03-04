"""Composable decorator helpers for custom task/workflow abstractions."""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

from slurm.core.handles import TaskHandle, WorkflowHandle
from slurm.decorators import task as base_task
from slurm.decorators import workflow as base_workflow


def _apply_default_options(
    handle: TaskHandle | WorkflowHandle,
    default_options: Mapping[str, Any] | None,
) -> TaskHandle | WorkflowHandle:
    if not default_options:
        return handle
    return handle.with_options(**dict(default_options))


def task_decorator(
    *,
    middleware: tuple[Any, ...] = (),
    default_options: Mapping[str, Any] | None = None,
    mutate_spec: Optional[Callable[[TaskHandle], TaskHandle]] = None,
) -> Callable[[Any], TaskHandle]:
    """Create a reusable task decorator transformer.

    The returned decorator supports both:
    - Decorating raw Python callables
    - Decorating existing TaskHandle objects
    """

    def decorator(target: Any) -> TaskHandle:
        default_applied = False
        if isinstance(target, TaskHandle):
            handle = target
        elif callable(target):
            handle = base_task(**dict(default_options or {}))(target)
            default_applied = True
        else:
            raise TypeError(
                f"task_decorator expected callable or TaskHandle, got {type(target).__name__}"
            )

        if not callable(target) or not default_options:
            default_applied = False

        if default_options and not default_applied:
            handle = _apply_default_options(handle, default_options)  # type: ignore[assignment]

        if middleware:
            handle = handle.with_middleware(*middleware)

        if mutate_spec is not None:
            handle = mutate_spec(handle)
        return handle

    return decorator


def workflow_decorator(
    *,
    middleware: tuple[Any, ...] = (),
    default_options: Mapping[str, Any] | None = None,
    mutate_spec: Optional[Callable[[WorkflowHandle], WorkflowHandle]] = None,
) -> Callable[[Any], WorkflowHandle]:
    """Create a reusable workflow decorator transformer."""

    def decorator(target: Any) -> WorkflowHandle:
        default_applied = False
        if isinstance(target, WorkflowHandle):
            handle = target
        elif callable(target):
            handle = base_workflow(**dict(default_options or {}))(target)
            default_applied = True
        else:
            raise TypeError(
                f"workflow_decorator expected callable or WorkflowHandle, got {type(target).__name__}"
            )

        if not callable(target) or not default_options:
            default_applied = False

        if default_options and not default_applied:
            handle = handle.with_options(**dict(default_options))

        if middleware:
            handle = handle.with_middleware(*middleware)

        if mutate_spec is not None:
            handle = mutate_spec(handle)
        return handle

    return decorator
