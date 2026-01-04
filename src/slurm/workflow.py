"""Workflow orchestration support for Slurm SDK."""

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Any
import pickle

if TYPE_CHECKING:
    from .cluster import Cluster


@dataclass
class WorkflowContext:
    """Context provided to workflow orchestrators.

    WorkflowContext is injected as a parameter to @workflow functions,
    providing access to the cluster, directories, and metadata.

    Attributes:
        cluster: Cluster instance for submitting jobs.
        workflow_job_id: The job ID of the orchestrator task.
        workflow_job_dir: The workflow's own job directory.
        shared_dir: Shared directory (workflow_job_dir/shared/).
        local_mode: True if running locally (not on cluster).

    Examples:
        Basic workflow with context:

            >>> from slurm.runtime import WorkflowContext
            >>> @workflow(time="02:00:00")
            ... def my_workflow(data: str, ctx: WorkflowContext):
            ...     # Submit tasks using context cluster
            ...     job = process(data)
            ...
            ...     # Save to shared directory
            ...     output = ctx.shared_dir / "result.pkl"
            ...     save(output, job.get_result())
            ...
            ...     return job.get_result()
    """

    cluster: "Cluster"
    """Cluster instance for submitting jobs."""

    workflow_job_id: str
    """The job ID of the orchestrator task."""

    workflow_job_dir: Path
    """The workflow's own job directory."""

    shared_dir: Path
    """Shared directory (workflow_job_dir/shared/)."""

    local_mode: bool = False
    """True if running locally (not on cluster)."""

    def __post_init__(self):
        """Initialize workflow context - create directories."""
        # Ensure directories are Path objects
        if not isinstance(self.workflow_job_dir, Path):
            self.workflow_job_dir = Path(self.workflow_job_dir)
        if not isinstance(self.shared_dir, Path):
            self.shared_dir = Path(self.shared_dir)

        # Create shared directory if it doesn't exist
        self.shared_dir.mkdir(parents=True, exist_ok=True)

        # Create tasks directory
        tasks_dir = self.workflow_job_dir / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)

    @property
    def result_path(self) -> Path:
        """Path to this workflow's result file."""
        return self.workflow_job_dir / "result.pkl"

    @property
    def metadata_path(self) -> Path:
        """Path to this workflow's metadata file."""
        return self.workflow_job_dir / "metadata.json"

    @property
    def tasks_dir(self) -> Path:
        """Directory where submitted worker tasks are stored."""
        return self.workflow_job_dir / "tasks"

    def get_task_output_dir(self, task_name: str) -> Path:
        """Get directory containing all runs of a task.

        Args:
            task_name: Name of the task.

        Returns:
            Path to directory containing all runs of the task.
        """
        return self.tasks_dir / task_name

    def list_task_runs(self, task_name: str) -> List[Path]:
        """List all run directories for a task, newest first.

        Args:
            task_name: Name of the task.

        Returns:
            List of Path objects for task run directories, sorted newest first.
        """
        task_dir = self.get_task_output_dir(task_name)
        if not task_dir.exists():
            return []

        # List all subdirectories with timestamp_id format
        runs = [p for p in task_dir.iterdir() if p.is_dir()]
        # Sort by name (timestamp_id) in reverse (newest first)
        runs.sort(reverse=True)
        return runs

    def get_latest_task_result(self, task_name: str) -> Any:
        """Load result from most recent run of a task.

        Args:
            task_name: Name of the task.

        Returns:
            The result object from the latest run.

        Raises:
            FileNotFoundError: If no runs found or result file doesn't exist.
        """
        runs = self.list_task_runs(task_name)
        if not runs:
            raise FileNotFoundError(
                f"No runs found for task '{task_name}'.\n\n"
                f"Expected output directory: {self.get_task_output_dir(task_name)}\n\n"
                "This usually means:\n"
                "  1. The task hasn't been executed yet in this workflow\n"
                "  2. The task name is misspelled\n"
                "  3. The workflow output directory was cleaned up\n\n"
                "Ensure the task has completed successfully before trying to load its result."
            )

        latest_run = runs[0]
        result_file = latest_run / "result.pkl"

        if not result_file.exists():
            raise FileNotFoundError(
                f"Result file not found for task '{task_name}'.\n\n"
                f"Run directory: {latest_run}\n"
                f"Expected file: {result_file}\n\n"
                "The task may have failed before writing its result.\n"
                "Check the task's stdout/stderr logs in the run directory for errors."
            )

        with open(result_file, "rb") as f:
            return pickle.load(f)

    def load_workflow_result(self, workflow_path: str) -> Any:
        """Load result from another workflow.

        Args:
            workflow_path: Path to the workflow directory.

        Returns:
            The result object from the workflow.

        Raises:
            FileNotFoundError: If result file doesn't exist.
        """
        workflow_dir = Path(workflow_path)
        result_file = workflow_dir / "result.pkl"

        if not result_file.exists():
            raise FileNotFoundError(
                f"Result file not found for workflow.\n\n"
                f"Workflow directory: {workflow_dir}\n"
                f"Expected file: {result_file}\n\n"
                "This usually means:\n"
                "  1. The workflow hasn't completed yet\n"
                "  2. The workflow failed before writing its result\n"
                "  3. The workflow directory path is incorrect\n\n"
                "Verify the workflow completed successfully and the path is correct."
            )

        with open(result_file, "rb") as f:
            return pickle.load(f)
