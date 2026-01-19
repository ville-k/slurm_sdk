"""JobResultPlaceholder resolution utilities for the runner.

This module handles resolving JobResultPlaceholder objects to their actual
values by loading results from completed jobs.
"""

import glob
import json
import logging
import os

# nosec B403 - pickle is required for deserializing task results
# Security note: pickle files are created by the SDK and transferred via trusted SSH/local storage
import pickle
from typing import Any

logger = logging.getLogger("slurm.runner")


def resolve_placeholder(value: Any, job_base_dir: str | None = None) -> Any:
    """Recursively resolve JobResultPlaceholder objects.

    This function traverses the input value and replaces any JobResultPlaceholder
    objects with their actual results by loading from the job's result file.

    Args:
        value: The value to resolve. Can be a JobResultPlaceholder, list, tuple,
               dict, or any other value.
        job_base_dir: Base directory for job results. If None, uses SLURM_JOBS_DIR
                     environment variable or ~/slurm_jobs.

    Returns:
        The resolved value with all placeholders replaced by actual results.

    Raises:
        FileNotFoundError: If the result file for a placeholder cannot be found.
    """
    # Import here to avoid circular imports
    from slurm.task import JobResultPlaceholder

    if isinstance(value, JobResultPlaceholder):
        return _load_placeholder_result(value, job_base_dir)
    elif isinstance(value, (list, tuple)):
        return type(value)(resolve_placeholder(item, job_base_dir) for item in value)
    elif isinstance(value, dict):
        return {k: resolve_placeholder(v, job_base_dir) for k, v in value.items()}
    else:
        return value


def _load_placeholder_result(placeholder: Any, job_base_dir: str | None = None) -> Any:
    """Load the result for a single JobResultPlaceholder.

    Args:
        placeholder: The JobResultPlaceholder to resolve.
        job_base_dir: Base directory for job results.

    Returns:
        The loaded result value.

    Raises:
        FileNotFoundError: If the result file cannot be found.
    """
    logger.debug("Resolving JobResultPlaceholder for job_id=%s", placeholder.job_id)

    if job_base_dir is None:
        job_base_dir = os.environ.get(
            "SLURM_JOBS_DIR", os.path.expanduser("~/slurm_jobs")
        )

    # Search for metadata.json files first (more efficient)
    search_pattern = f"{job_base_dir}/**/metadata.json"
    for metadata_path in glob.glob(search_pattern, recursive=True):
        try:
            with open(metadata_path, "r") as f:
                metadata_map = json.load(f)

            # Check if this metadata contains our job_id
            if placeholder.job_id in metadata_map:
                result_dir = os.path.dirname(metadata_path)
                result_filename = metadata_map[placeholder.job_id]["result_file"]
                result_path = os.path.join(result_dir, result_filename)

                logger.debug("Found result file: %s", result_path)
                # nosec B301 - result file created by SDK runner, stored in trusted job dir
                with open(result_path, "rb") as f:
                    return pickle.load(f)  # nosec B301
        except Exception as e:
            logger.warning("Error reading metadata from %s: %s", metadata_path, e)

    raise FileNotFoundError(
        f"Could not find result file for job_id={placeholder.job_id}"
    )


def resolve_task_arguments(
    args: tuple, kwargs: dict, job_base_dir: str | None = None
) -> tuple[tuple, dict]:
    """Resolve placeholders in task arguments and keyword arguments.

    Convenience function that resolves both args and kwargs in one call.

    Args:
        args: Positional arguments tuple
        kwargs: Keyword arguments dict
        job_base_dir: Base directory for job results

    Returns:
        Tuple of (resolved_args, resolved_kwargs)
    """
    resolved_args = resolve_placeholder(args, job_base_dir)
    resolved_kwargs = resolve_placeholder(kwargs, job_base_dir)
    return resolved_args, resolved_kwargs


__all__ = [
    "resolve_placeholder",
    "resolve_task_arguments",
]
