"""Middleware protocols for generalized task invocation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class InvokeContext:
    """Context available during task invocation pre-processing."""

    task: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass
class SubmitContext:
    """Context available during task submission processing."""

    task: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass
class ResultContext:
    """Context available when a result handle has been produced."""

    task: Any
    result_ref: Any


class TaskMiddleware(Protocol):
    """Middleware extension points around invoke/submit/result flow."""

    def before_invoke(self, ctx: InvokeContext) -> None:
        """Run before runtime invocation."""

    def transform_call(self, ctx: InvokeContext) -> None:
        """Mutate or validate invocation args."""

    def before_submit(self, ctx: SubmitContext) -> None:
        """Run before submission."""

    def after_submit(self, ctx: SubmitContext, ref: Any) -> Any:
        """Transform submission result."""

    def on_result(self, ctx: ResultContext) -> None:
        """Observe finalized reference."""


class NoopMiddleware:
    """No-op middleware implementation used as default placeholder."""

    def before_invoke(self, ctx: InvokeContext) -> None:
        return None

    def transform_call(self, ctx: InvokeContext) -> None:
        return None

    def before_submit(self, ctx: SubmitContext) -> None:
        return None

    def after_submit(self, ctx: SubmitContext, ref: Any) -> Any:
        return ref

    def on_result(self, ctx: ResultContext) -> None:
        return None


class LoggingMiddleware:
    """Simple built-in middleware that logs invocation lifecycle hooks."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("slurm.middleware")

    @staticmethod
    def _task_name(task: Any) -> str:
        if hasattr(task, "name"):
            return str(task.name)
        if hasattr(task, "func"):
            return str(getattr(task.func, "__name__", "task"))
        return "task"

    def before_invoke(self, ctx: InvokeContext) -> None:
        self._logger.debug(
            "middleware.before_invoke task=%s", self._task_name(ctx.task)
        )

    def transform_call(self, ctx: InvokeContext) -> None:
        self._logger.debug(
            "middleware.transform_call task=%s", self._task_name(ctx.task)
        )

    def before_submit(self, ctx: SubmitContext) -> None:
        self._logger.debug(
            "middleware.before_submit task=%s", self._task_name(ctx.task)
        )

    def after_submit(self, ctx: SubmitContext, ref: Any) -> Any:
        self._logger.debug("middleware.after_submit task=%s", self._task_name(ctx.task))
        return ref

    def on_result(self, ctx: ResultContext) -> None:
        self._logger.debug("middleware.on_result task=%s", self._task_name(ctx.task))
