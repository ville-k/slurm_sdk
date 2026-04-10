"""Base callback class and hook locus configuration."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

from .contexts import (
    CompletedContext,
    ExecutionLocus,
    JobStatusUpdatedContext,
    PackagingBeginContext,
    PackagingEndContext,
    RunBeginContext,
    RunEndContext,
    SubmitBeginContext,
    SubmitEndContext,
    WorkflowCallbackContext,
    WorkflowTaskSubmitContext,
)

WORKFLOW_METRICS_FILENAME = ".slurm_benchmark_metrics.json"


_DEFAULT_HOOK_LOCI: Dict[str, ExecutionLocus] = {
    "on_begin_package_ctx": ExecutionLocus.CLIENT,
    "on_end_package_ctx": ExecutionLocus.CLIENT,
    "on_begin_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_end_submit_job_ctx": ExecutionLocus.CLIENT,
    "on_job_status_update_ctx": ExecutionLocus.CLIENT,
    "on_begin_run_job_ctx": ExecutionLocus.RUNNER,
    "on_end_run_job_ctx": ExecutionLocus.RUNNER,
    "on_completed_ctx": ExecutionLocus.BOTH,
    "on_workflow_begin_ctx": ExecutionLocus.RUNNER,
    "on_workflow_end_ctx": ExecutionLocus.RUNNER,
    "on_workflow_task_submitted_ctx": ExecutionLocus.CLIENT,
}

_RUNNER_HOOKS: Tuple[str, ...] = (
    "on_begin_run_job_ctx",
    "on_end_run_job_ctx",
    "on_completed_ctx",
    "on_workflow_begin_ctx",
    "on_workflow_end_ctx",
)


class BaseCallback:
    """Base class for Slurm job lifecycle callbacks."""

    execution_loci: Dict[str, ExecutionLocus] = {}
    poll_interval_secs: Optional[float] = None
    requires_pickling: bool = True

    def on_begin_package_ctx(
        self, ctx: PackagingBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_package_ctx(
        self, ctx: PackagingEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_begin_submit_job_ctx(
        self, ctx: SubmitBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_submit_job_ctx(
        self, ctx: SubmitEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_begin_run_job_ctx(
        self, ctx: RunBeginContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_end_run_job_ctx(
        self, ctx: RunEndContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_job_status_update_ctx(
        self, ctx: JobStatusUpdatedContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_completed_ctx(
        self, ctx: CompletedContext
    ) -> None:  # pragma: no cover - default no-op
        pass

    def on_workflow_begin_ctx(
        self, ctx: WorkflowCallbackContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow orchestrator starts execution.

        This fires AFTER on_begin_run_job_ctx for the workflow job itself.
        Marks the point where WorkflowContext is created and orchestration begins.

        Args:
            ctx: Workflow-specific callback context
        """
        pass

    def on_workflow_end_ctx(
        self, ctx: WorkflowCallbackContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow orchestrator completes execution.

        This fires BEFORE on_end_run_job_ctx for the workflow job itself.
        Marks the point where orchestration is complete and result is ready.

        Args:
            ctx: Workflow-specific callback context with result info
        """
        pass

    def on_workflow_task_submitted_ctx(
        self, ctx: WorkflowTaskSubmitContext
    ) -> None:  # pragma: no cover - default no-op
        """Called when workflow submits a child task.

        Fires immediately after a task/workflow is submitted from within
        a workflow context. Enables tracking of parent-child relationships.

        Args:
            ctx: Context with parent workflow and child task info
        """
        pass

    def get_execution_locus(self, hook_name: str) -> ExecutionLocus:
        if hook_name in self.execution_loci:
            return ExecutionLocus(self.execution_loci[hook_name])
        return _DEFAULT_HOOK_LOCI.get(hook_name, ExecutionLocus.CLIENT)

    def should_run_on_client(self, hook_name: str) -> bool:
        locus = self.get_execution_locus(hook_name)
        return locus in (ExecutionLocus.CLIENT, ExecutionLocus.BOTH)

    def should_run_on_runner(self, hook_name: str) -> bool:
        locus = self.get_execution_locus(hook_name)
        return locus in (ExecutionLocus.RUNNER, ExecutionLocus.BOTH)

    def get_poll_interval(self) -> Optional[float]:
        if self.poll_interval_secs is None:
            return None
        try:
            interval = float(self.poll_interval_secs)
        except (TypeError, ValueError):
            return None
        if interval <= 0:
            return None
        return interval

    def requires_runner_transport(self) -> bool:
        if not self.requires_pickling:
            return False
        return any(self.should_run_on_runner(hook) for hook in _RUNNER_HOOKS)
