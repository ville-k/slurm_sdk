"""Core generalized task/workflow abstractions."""

from slurm.core.decorators import task_decorator, workflow_decorator
from slurm.core.handles import TaskHandle, WorkflowHandle
from slurm.core.middleware import (
    InvokeContext,
    LoggingMiddleware,
    NoopMiddleware,
    ResultContext,
    SubmitContext,
    TaskMiddleware,
)
from slurm.core.protocols import (
    DependencyRef,
    InvocationRef,
    InvocationRuntime,
    TaskLike,
    WorkflowLike,
)
from slurm.core.refs import JobRef, RefPlaceholder
from slurm.core.runtime import ClusterRuntime, DefaultInvocationRuntime, WorkflowRuntime
from slurm.core.spec import TaskSpec

__all__ = [
    "ClusterRuntime",
    "DefaultInvocationRuntime",
    "DependencyRef",
    "InvocationRef",
    "InvocationRuntime",
    "InvokeContext",
    "JobRef",
    "LoggingMiddleware",
    "NoopMiddleware",
    "RefPlaceholder",
    "ResultContext",
    "SubmitContext",
    "TaskHandle",
    "task_decorator",
    "TaskLike",
    "TaskMiddleware",
    "TaskSpec",
    "WorkflowHandle",
    "WorkflowLike",
    "WorkflowRuntime",
    "workflow_decorator",
]
