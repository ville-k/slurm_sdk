"""Invocation runtime abstractions and default runtime implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from slurm.core.middleware import InvokeContext, ResultContext, SubmitContext


@dataclass(frozen=True)
class DefaultInvocationRuntime:
    """Default runtime delegating task calls to cluster submission semantics."""

    def invoke(self, task: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        from slurm.task import _invoke_task_via_cluster_submission

        middleware = tuple(getattr(task, "middleware", tuple()) or tuple())

        invoke_ctx = InvokeContext(task=task, args=args, kwargs=dict(kwargs))
        for item in middleware:
            if hasattr(item, "before_invoke"):
                item.before_invoke(invoke_ctx)
        for item in middleware:
            if hasattr(item, "transform_call"):
                item.transform_call(invoke_ctx)

        submit_ctx = SubmitContext(
            task=task,
            args=invoke_ctx.args,
            kwargs=dict(invoke_ctx.kwargs),
        )
        for item in middleware:
            if hasattr(item, "before_submit"):
                item.before_submit(submit_ctx)

        result_ref = _invoke_task_via_cluster_submission(
            task,
            submit_ctx.args,
            submit_ctx.kwargs,
        )

        for item in middleware:
            if hasattr(item, "after_submit"):
                result_ref = item.after_submit(submit_ctx, result_ref)

        result_ctx = ResultContext(task=task, result_ref=result_ref)
        for item in middleware:
            if hasattr(item, "on_result"):
                item.on_result(result_ctx)

        return result_ref


@dataclass(frozen=True)
class ClusterRuntime(DefaultInvocationRuntime):
    """Runtime used when cluster context is active."""


@dataclass(frozen=True)
class WorkflowRuntime(DefaultInvocationRuntime):
    """Runtime used when workflow context is active."""
