"""Array job support for Slurm SDK."""

from typing import TYPE_CHECKING, TypeVar, Generic, List, Optional, Any, Union, Iterable
from pathlib import Path
import pickle

if TYPE_CHECKING:
    from .cluster import Cluster
    from .task import SlurmTask
    from .job import Job

T = TypeVar("T")


class ArrayJob(Generic[T]):
    """Represents a Slurm job array with grouped directory structure.

    ArrayJob provides a fluent API for submitting array jobs and managing
    their results. All tasks in the array are organized under a single
    directory with a grouped structure.

    Directory structure:
        {task_name}/{timestamp}_{id}/
        ├── array_metadata.json       # Array-level metadata
        ├── tasks/                    # Individual array tasks
        │   ├── 000/                  # Array index 0
        │   ├── 001/                  # Array index 1
        │   └── 002/                  # Array index 2
        └── results/                  # Aggregated results

    Attributes:
        task: The SlurmTask to execute for each array element.
        items: List of items to process (one per array task).
        cluster: Cluster instance for submission.
        array_dir: Path to the array submission directory.

    Examples:
        Basic array job:

            >>> @task(time="00:10:00")
            ... def process(file: str) -> Result:
            ...     return load_and_process(file)
            >>> files = ["a.csv", "b.csv", "c.csv"]
            >>> array_job = process.map(files)
            >>> results = array_job.get_results()

        Array with dependencies:

            >>> prep_job = preprocess("data.csv")
            >>> train_jobs = train.map(configs).after(prep_job)
            >>> results = train_jobs.get_results()
    """

    def __init__(
        self,
        task: "SlurmTask",
        items: List[Any],
        cluster: "Cluster",
        max_concurrent: Optional[int] = None,
        dependencies: Optional[List["Job"]] = None,
    ):
        """Initialize ArrayJob.

        Args:
            task: SlurmTask to execute for each item.
            items: List of items to process.
            cluster: Cluster for submission.
            max_concurrent: Maximum concurrent tasks (optional).
            dependencies: Job dependencies (optional).
        """
        self.task = task
        self.items = items
        self.cluster = cluster
        self.max_concurrent = max_concurrent
        self.dependencies = dependencies or []
        self._submitted = False
        self._array_job_id: Optional[str] = None
        self._jobs: List["Job"] = []

        # Directory structure will be set when submitted
        self.array_dir: Optional[Path] = None

    def __len__(self) -> int:
        """Get number of tasks in array."""
        return len(self.items)

    def __getitem__(self, index: int) -> "Job[T]":
        """Get job for specific array index.

        Args:
            index: Array index.

        Returns:
            Job for that array element.
        """
        if not self._submitted:
            self._submit()

        if index < 0 or index >= len(self.items):
            raise IndexError(f"Array index {index} out of range [0, {len(self.items)})")

        return self._jobs[index]

    def __iter__(self):
        """Iterate over jobs in array."""
        if not self._submitted:
            self._submit()

        return iter(self._jobs)

    def after(self, *jobs: Union["Job", "ArrayJob"]) -> "ArrayJob[T]":
        """Add dependencies (fluent API).

        Args:
            *jobs: Jobs or ArrayJobs to depend on.

        Returns:
            Self for method chaining.
        """
        for job in jobs:
            if isinstance(job, ArrayJob):
                # Depend on all jobs in the array
                self.dependencies.extend(job._jobs if job._submitted else [])
            else:
                self.dependencies.append(job)
        return self

    def _submit(self) -> None:
        """Submit the array job (internal)."""
        if self._submitted:
            return

        # Submit each item as a separate job
        # In a full implementation, this would use Slurm's native array job feature
        # For now, we submit individual jobs
        for i, item in enumerate(self.items):
            # Prepare arguments based on item type
            if isinstance(item, dict):
                # Dict: unpack as kwargs
                job = self.task(**item)
            elif isinstance(item, tuple):
                # Tuple: unpack as args
                job = self.task(*item)
            else:
                # Single value: pass as first arg
                job = self.task(item)

            self._jobs.append(job)

        self._submitted = True

        # Set array directory based on first job's directory
        if self._jobs:
            first_job_dir = Path(self._jobs[0].target_job_dir)
            # The array_dir should be the parent of the first job
            # In the grouped structure: {task_name}/{timestamp}_{id}/tasks/000/
            # So array_dir is {task_name}/{timestamp}_{id}/
            self.array_dir = first_job_dir.parent.parent

    def get_results(self, timeout: Optional[float] = None) -> List[T]:
        """Wait for all tasks and return results.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            List of results from all array tasks.
        """
        if not self._submitted:
            self._submit()

        results = []
        for job in self._jobs:
            result = job.get_result(timeout=timeout)
            results.append(result)

        return results

    def get_results_dir(self) -> Path:
        """Get directory for aggregated results.

        Returns:
            Path to results directory.
        """
        if self.array_dir is None:
            raise RuntimeError(
                "Cannot get results directory: array job has not been submitted yet.\n\n"
                "Array jobs are submitted lazily when you first access their results.\n"
                "To explicitly submit, call: array_job.submit()\n"
                "Or access results which will trigger submission: array_job.get_results()"
            )

        return self.array_dir / "results"

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for all array tasks to complete.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            True if all tasks completed, False if timeout.
        """
        if not self._submitted:
            self._submit()

        for job in self._jobs:
            if not job.wait(timeout=timeout):
                return False

        return True


__all__ = ["ArrayJob"]
