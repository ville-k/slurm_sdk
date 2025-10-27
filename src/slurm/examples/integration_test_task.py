"""
Simple task for integration testing.
This module is included in the slurm package wheel.
"""

from pathlib import Path
from slurm.decorators import task
from slurm.runtime import JobContext


@task()
def simple_integration_task() -> str:
    """A simple task that returns a success message for integration testing."""
    return "integration-ok"


@task(time="00:01:00", mem="100M")
def write_output_file_task(message: str, job: JobContext) -> Path:
    """Write a message to a file in the output directory and return its path.

    Args:
        message: The message to write to the file
        job: JobContext with output_dir set

    Returns:
        Path to the written file
    """
    if job.output_dir is None:
        raise RuntimeError("output_dir is not set in JobContext")

    output_file = job.output_dir / "test_output.txt"
    output_file.write_text(f"Message: {message}\nJob ID: {job.job_id}\n")

    import json

    json_file = job.output_dir / "metadata.json"
    metadata = {
        "message": message,
        "job_id": job.job_id,
        "output_dir": str(job.output_dir),
        "rank": job.rank,
        "world_size": job.world_size,
    }
    json_file.write_text(json.dumps(metadata, indent=2))

    return output_file


@task(time="00:01:00", mem="100M")
def get_output_dir_task(job: JobContext) -> dict:
    """Return information about the output directory.

    Args:
        job: JobContext with environment and output_dir

    Returns:
        Dictionary with output_dir information
    """
    return {
        "output_dir": str(job.output_dir) if job.output_dir else None,
        "output_dir_exists": job.output_dir.exists() if job.output_dir else False,
        "job_id": job.job_id,
        "job_dir_env": job.environment.get("SLURM_JOB_DIR"),
    }
