"""Result serialization and metadata writing utilities for the runner.

This module handles saving task results and environment metadata.
"""

import fcntl
import json
import logging
import os

# nosec B403 - pickle is required for serializing task results
# Security note: pickle files are created by the SDK and stored in trusted job directories
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("slurm.runner")


def write_environment_metadata(
    job_dir: str,
    packaging_type: str,
    job_id: Optional[str] = None,
    workflow_name: Optional[str] = None,
    pre_submission_id: Optional[str] = None,
) -> None:
    """Write environment metadata for child tasks to inherit.

    This metadata file allows child tasks using InheritPackagingStrategy
    to discover and activate the parent workflow's execution environment.

    Args:
        job_dir: The workflow job directory
        packaging_type: The type of packaging used (wheel, container, etc.)
        job_id: The SLURM job ID
        workflow_name: The name of the workflow function
        pre_submission_id: The pre-submission ID
    """
    try:
        metadata_path = Path(job_dir) / ".slurm_environment.json"

        # Detect current environment details
        venv_path = os.environ.get("VIRTUAL_ENV")
        python_executable = sys.executable
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        container_image = os.environ.get("SINGULARITY_NAME") or os.environ.get(
            "SLURM_CONTAINER_IMAGE"
        )

        # Build metadata structure
        metadata = {
            "version": "1.0",
            "packaging_type": packaging_type,
            "environment": {
                "venv_path": venv_path,
                "python_executable": python_executable,
                "python_version": python_version,
                "container_image": container_image,
                "activated": bool(venv_path or container_image),
            },
            "shared_paths": {
                "job_dir": job_dir,
                "shared_dir": str(Path(job_dir) / "shared"),
                "tasks_dir": str(Path(job_dir) / "tasks"),
            },
            "parent_job": {
                "slurm_job_id": job_id or "unknown",
                "pre_submission_id": pre_submission_id or "unknown",
                "workflow_name": workflow_name or "unknown",
            },
            "created_at": datetime.utcnow().isoformat() + "Z",
        }

        # Write metadata file
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Wrote environment metadata to {metadata_path}")
        logger.debug(f"Metadata: {json.dumps(metadata, indent=2)}")

    except Exception as e:
        logger.warning(f"Failed to write environment metadata: {e}")
        # Non-fatal - child tasks will fall back to other strategies


def save_result(output_file: str, result: Any) -> None:
    """Save task result to a pickle file.

    Args:
        output_file: Path to save the result
        result: The result to serialize
    """
    logger.debug("Saving result to %s...", output_file)

    output_dir = os.path.dirname(output_file)
    if output_dir:
        logger.debug("Ensuring output directory exists: %s", output_dir)
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "wb") as f:
        pickle.dump(result, f)

    logger.debug("Result saved successfully.")


def update_job_metadata(
    output_file: str,
    job_id: str,
    timestamp: float,
    max_retries: int = 10,
    initial_retry_delay: float = 0.1,
) -> None:
    """Update metadata.json with job result information.

    Uses file locking to handle concurrent writes from array jobs safely.

    Args:
        output_file: Path to the result file
        job_id: The job ID to record
        timestamp: Timestamp when result was saved
        max_retries: Maximum lock acquisition attempts
        initial_retry_delay: Initial delay between retries (doubles each attempt)
    """
    output_dir = os.path.dirname(output_file) or "."
    metadata_path = os.path.join(output_dir, "metadata.json")

    try:
        lock_file = metadata_path + ".lock"
        retry_delay = initial_retry_delay

        for attempt in range(max_retries):
            try:
                lock_fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY, 0o644)
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                try:
                    metadata_map = {}
                    if os.path.exists(metadata_path):
                        try:
                            with open(metadata_path, "r") as f:
                                metadata_map = json.load(f)
                        except Exception as read_err:
                            logger.warning(
                                "Could not read metadata, starting fresh: %s",
                                read_err,
                            )

                    metadata_map[job_id] = {
                        "result_file": os.path.basename(output_file),
                        "timestamp": timestamp,
                    }

                    # Atomic write: temp file + rename prevents partial reads
                    temp_path = metadata_path + ".tmp"
                    with open(temp_path, "w") as f:
                        json.dump(metadata_map, f, indent=2)
                    os.rename(temp_path, metadata_path)

                    logger.debug(
                        "Metadata saved to %s (job_id=%s)",
                        metadata_path,
                        job_id,
                    )
                finally:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                return  # Success

            except (IOError, OSError) as lock_err:
                if attempt < max_retries - 1:
                    logger.debug(
                        "Metadata lock busy, retrying in %s seconds...", retry_delay
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise Exception(
                        f"Could not acquire metadata lock after {max_retries} attempts"
                    ) from lock_err

    except Exception as e:
        logger.warning("Failed to save metadata: %s", e)


# Aliases for backwards compatibility
_write_environment_metadata = write_environment_metadata

__all__ = [
    "write_environment_metadata",
    "save_result",
    "update_job_metadata",
    "_write_environment_metadata",
]
