"""Array job support for Slurm SDK."""

from typing import TYPE_CHECKING, TypeVar, Generic, List, Optional, Any
from pathlib import Path

if TYPE_CHECKING:
    from .cluster import Cluster
    from .task import SlurmTask
    from .job import Job

T = TypeVar("T")


class ArrayJob(Generic[T]):
    """Represents a Slurm job array with eager submission.

    ArrayJob submits all tasks immediately upon creation as a native SLURM
    array job (--array flag), providing 10-1000x speedup over individual job
    submissions. All tasks in the array share a single directory.

    **Eager Execution**: Array jobs submit immediately when created. For
    dependencies, use the reversed fluent API: task.after(deps).map(items)

    Directory structure:
        {task_name}/{timestamp}_{id}/
        ├── array_items.pkl           # Serialized array items
        ├── slurm_{id}_0.out          # Array element 0 stdout
        ├── slurm_{id}_0.err          # Array element 0 stderr
        ├── slurm_{id}_1.out          # Array element 1 stdout
        └── ...

    Attributes:
        task: The SlurmTask to execute for each array element.
        items: List of items to process (one per array task).
        cluster: Cluster instance for submission.
        array_dir: Path to the array submission directory.

    Examples:
        Basic array job (submits immediately):

            >>> @task(time="00:10:00")
            ... def process(file: str) -> Result:
            ...     return load_and_process(file)
            >>> files = ["a.csv", "b.csv", "c.csv"]
            >>> array_job = process.map(files)  # Submits here!
            >>> results = array_job.get_results()

        Array with dependencies (reversed API):

            >>> prep_job = preprocess("data.csv")
            >>> train_jobs = train.after(prep_job).map(configs)  # Deps first!
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
        """Initialize ArrayJob and submit it immediately (eager execution).

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

        # Eager submission - submit immediately in constructor
        self._submit()

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
        if index < 0 or index >= len(self.items):
            raise IndexError(f"Array index {index} out of range [0, {len(self.items)})")

        return self._jobs[index]

    def __iter__(self):
        """Iterate over jobs in array."""
        return iter(self._jobs)

    def _submit(self) -> None:
        """Submit the array job as a native SLURM array using --array flag.

        This submits a single job with --array=0-N, which is 10-1000x faster
        than submitting N individual jobs.
        """
        if self._submitted:
            return

        if not self.items:
            self._submitted = True
            return
        from .array_items import serialize_array_items, generate_array_spec
        from .rendering import render_job_script
        import uuid
        from datetime import datetime
        import re
        import tempfile

        # Generate array specification
        array_spec = generate_array_spec(len(self.items), self.max_concurrent)

        # Create a base job directory for the array
        # This will be similar to individual job dirs but shared
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        pre_submission_id = f"{timestamp}_{unique_id}"

        task_name = getattr(self.task, "__name__", "array_job")
        job_base_dir = getattr(self.cluster.backend, "job_base_dir", "/tmp/slurm_jobs")
        array_dir = Path(job_base_dir) / task_name / pre_submission_id

        # Determine target directory based on backend type
        target_job_dir = str(array_dir)

        # Check if backend is remote (SSH) or local
        is_remote = (
            hasattr(self.cluster.backend, "is_remote")
            and self.cluster.backend.is_remote()
        )

        # Get packaging strategy (use cluster's default or auto)
        from .packaging import get_packaging_strategy

        packaging_config = self.task.packaging or getattr(
            self.cluster, "packaging_defaults", None
        )
        packaging_strategy = get_packaging_strategy(packaging_config)

        # Prepare packaging
        try:
            packaging_strategy.prepare(task=self.task, cluster=self.cluster)
        except Exception as exc:
            import logging

            logger = logging.getLogger(__name__)
            logger.error("Packaging preparation failed: %s", exc)
            raise

        # Get task defaults and merge with dependency options
        task_defaults = dict(getattr(self.task, "sbatch_options", {}) or {})
        sbatch_overrides = {}

        # Add dependency if specified
        if self.dependencies:
            job_ids = [job.id for job in self.dependencies]
            if job_ids:
                dependency_str = "afterok:" + ":".join(job_ids)
                sbatch_overrides["dependency"] = dependency_str

        # Serialize array items to target location
        # For local backends, write directly to target dir
        # For remote backends, write to temp dir then upload
        if is_remote:
            # Remote backend: use temp dir and upload
            with tempfile.TemporaryDirectory() as tmp_dir:
                array_items_filename = serialize_array_items(
                    self.items,
                    tmp_dir,
                    self.max_concurrent,
                )
                local_items_file = Path(tmp_dir) / array_items_filename

                # Render the job script with array job support
                script = render_job_script(
                    task_func=self.task.func,
                    task_args=(),  # Not used for array jobs
                    task_kwargs={},  # Not used for array jobs
                    task_definition=task_defaults,
                    sbatch_overrides=sbatch_overrides,
                    packaging_strategy=packaging_strategy,
                    target_job_dir=target_job_dir,
                    pre_submission_id=pre_submission_id,
                    callbacks=self.cluster.callbacks,
                    cluster=self.cluster,
                    is_array_job=True,
                    array_items_file=array_items_filename,
                )

                # Upload the array items file to the target directory
                if hasattr(self.cluster.backend, "_upload_file"):
                    remote_items_path = f"{target_job_dir}/{array_items_filename}"
                    self.cluster.backend._upload_file(
                        str(local_items_file), remote_items_path
                    )

                # Submit the array job to the backend
                submit_account = (
                    task_defaults.get("account") or self.cluster.default_account
                )
                submit_partition = (
                    task_defaults.get("partition") or self.cluster.default_partition
                )

                array_job_id = self.cluster.backend.submit_job(
                    script,
                    target_job_dir=target_job_dir,
                    pre_submission_id=pre_submission_id,
                    account=submit_account,
                    partition=submit_partition,
                    array_spec=array_spec,
                )
        else:
            # Local backend: write array items directly to target directory
            import os

            os.makedirs(target_job_dir, exist_ok=True)
            array_items_filename = serialize_array_items(
                self.items,
                target_job_dir,
                self.max_concurrent,
            )

            # Render the job script with array job support
            script = render_job_script(
                task_func=self.task.func,
                task_args=(),  # Not used for array jobs
                task_kwargs={},  # Not used for array jobs
                task_definition=task_defaults,
                sbatch_overrides=sbatch_overrides,
                packaging_strategy=packaging_strategy,
                target_job_dir=target_job_dir,
                pre_submission_id=pre_submission_id,
                callbacks=self.cluster.callbacks,
                cluster=self.cluster,
                is_array_job=True,
                array_items_file=array_items_filename,
            )

            # Submit the array job to the backend
            submit_account = (
                task_defaults.get("account") or self.cluster.default_account
            )
            submit_partition = (
                task_defaults.get("partition") or self.cluster.default_partition
            )

            array_job_id = self.cluster.backend.submit_job(
                script,
                target_job_dir=target_job_dir,
                pre_submission_id=pre_submission_id,
                account=submit_account,
                partition=submit_partition,
                array_spec=array_spec,
            )

        # Parse the array job ID (format: "12345_[0-99]" or just "12345")
        # and create Job objects for each array element
        match = re.match(r"(\d+)_?\[?(\d+)-(\d+)\]?", array_job_id)
        if match:
            base_job_id = match.group(1)
            start_idx = int(match.group(2))
            end_idx = int(match.group(3))
        else:
            # Fallback: assume it's just the base ID and infer range from items
            base_job_id = array_job_id
            start_idx = 0
            end_idx = len(self.items) - 1

        # Create Job objects for each array element
        from .job import Job

        for idx in range(start_idx, end_idx + 1):
            # Individual array element job ID: "12345_5"
            element_job_id = f"{base_job_id}_{idx}"

            # Get the item for this index
            item = self.items[idx]
            if isinstance(item, dict):
                args = ()
                kwargs = item
            elif isinstance(item, tuple):
                args = item
                kwargs = {}
            else:
                args = (item,)
                kwargs = {}

            # Create Job object
            # Note: For native arrays, all tasks share the same job directory
            # Use non-padded idx to match SLURM's %a substitution in result filenames
            job = Job(
                id=element_job_id,
                cluster=self.cluster,
                task_func=self.task,
                args=args,
                kwargs=kwargs,
                target_job_dir=target_job_dir,  # Shared directory for all array elements
                pre_submission_id=f"{pre_submission_id}_{idx}",
                sbatch_options=dict(task_defaults),
                stdout_path=f"{target_job_dir}/slurm_{pre_submission_id}_{idx}.out",
                stderr_path=f"{target_job_dir}/slurm_{pre_submission_id}_{idx}.err",
            )
            self._jobs.append(job)

        self._submitted = True
        self._array_job_id = array_job_id
        self.array_dir = array_dir

    def get_results(self, timeout: Optional[float] = None) -> List[T]:
        """Wait for all tasks and return results.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            List of results from all array tasks.
        """
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
                "Cannot get results directory: array job was not properly initialized."
            )

        return self.array_dir / "results"

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for all array tasks to complete.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            True if all tasks completed, False if timeout.
        """
        for job in self._jobs:
            if not job.wait(timeout=timeout):
                return False

        return True


__all__ = ["ArrayJob"]
