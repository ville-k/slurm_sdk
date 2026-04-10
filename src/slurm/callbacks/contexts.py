"""Execution locus enum and lifecycle context dataclasses."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

from ..runtime import JobContext

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from ..cluster import Cluster
    from ..job import Job
    from ..packaging import PackagingStrategy
    from ..workflow import WorkflowContext


class ExecutionLocus(str, Enum):
    """Indicates where a callback hook executes."""

    CLIENT = "client"
    RUNNER = "runner"
    BOTH = "both"


@dataclass
class PackagingBeginContext:
    """Context emitted when packaging begins."""

    task: Any
    packaging_config: Optional[Dict[str, Any]] = None
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class PackagingEndContext:
    """Context emitted when packaging completes."""

    task: Any
    packaging_result: Any
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)
    duration: Optional[float] = None


@dataclass
class SubmitBeginContext:
    """Context emitted immediately before job submission."""

    task: Any
    sbatch_options: Dict[str, Any]
    pre_submission_id: str
    target_job_dir: str
    cluster: Optional["Cluster"] = None
    packaging_strategy: Optional["PackagingStrategy"] = None
    backend_type: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class SubmitEndContext:
    """Context emitted right after job submission."""

    job: "Job"
    job_id: str
    pre_submission_id: str
    target_job_dir: str
    sbatch_options: Dict[str, Any]
    cluster: Optional["Cluster"] = None
    timestamp: float = field(default_factory=time.time)
    backend_type: Optional[str] = None


@dataclass
class RunBeginContext:
    """Context emitted on the runner before executing the user function."""

    module: str
    function: str
    args_file: str
    kwargs_file: str
    output_file: str
    job_id: Optional[str] = None
    job_dir: Optional[str] = None
    hostname: Optional[str] = None
    python_executable: Optional[str] = None
    python_version: Optional[str] = None
    working_directory: Optional[str] = None
    environment_snapshot: Optional[Dict[str, str]] = None
    start_time: float = field(default_factory=time.time)
    job_context: Optional[JobContext] = None


@dataclass
class RunEndContext:
    """Context emitted on the runner after executing the user function."""

    status: str
    output_file: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    job_id: Optional[str] = None
    job_dir: Optional[str] = None
    hostname: Optional[str] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: Optional[float] = None
    job_context: Optional[JobContext] = None


@dataclass
class JobStatusUpdatedContext:
    """Context emitted by the SDK-managed polling service."""

    job: "Job"
    job_id: str
    status: Dict[str, Any]
    timestamp: float
    previous_state: Optional[str] = None
    is_terminal: bool = False


@dataclass
class CompletedContext:
    """Context emitted when a job reaches a terminal state."""

    job: Optional["Job"]
    job_id: Optional[str]
    job_dir: Optional[str]
    job_state: Optional[str]
    exit_code: Optional[str]
    reason: Optional[str]
    stdout_path: Optional[str]
    stderr_path: Optional[str]
    start_time: Optional[float]
    end_time: Optional[float]
    duration: Optional[float]
    status: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    result_path: Optional[str] = None
    emitted_by: ExecutionLocus = ExecutionLocus.CLIENT
    job_context: Optional[JobContext] = None


@dataclass
class WorkflowCallbackContext:
    """Context for workflow lifecycle events (begin/end).

    Emitted when a workflow orchestrator starts or completes execution.
    This is distinct from the regular job lifecycle events that also fire
    for the workflow job itself.
    """

    # Workflow identification
    workflow_job_id: str
    workflow_job_dir: "Path"
    workflow_name: str  # Task function name

    # Workflow context (if available)
    workflow_context: Optional["WorkflowContext"]

    # Timing
    timestamp: float

    # Result (on_workflow_end only)
    result: Optional[Any] = None
    exception: Optional[Exception] = None

    # Cluster reference (may be None in runner context)
    cluster: Optional["Cluster"] = None


@dataclass
class WorkflowTaskSubmitContext:
    """Context for child task submission events.

    Emitted when a workflow submits a child task (which may itself be
    a workflow). Enables tracking of parent-child relationships and
    orchestration progress.
    """

    # Parent workflow info
    parent_workflow_id: str
    parent_workflow_dir: "Path"
    parent_workflow_name: str

    # Child task info
    child_job_id: str
    child_job_dir: "Path"
    child_task_name: str
    child_is_workflow: bool  # True if child is also a workflow

    # Timing and cluster
    timestamp: float
    cluster: "Cluster"
