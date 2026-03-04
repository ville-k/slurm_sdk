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
from typing import Any, Callable, Dict

from slurm.core.refs import RefPlaceholder

logger = logging.getLogger("slurm.runner")

_PLACEHOLDER_RESOLVERS: Dict[str, Callable[[dict[str, Any], str | None], Any]] = {}


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
    elif isinstance(value, RefPlaceholder):
        return _resolve_ref_placeholder(value, job_base_dir)
    elif isinstance(value, (list, tuple)):
        return type(value)(resolve_placeholder(item, job_base_dir) for item in value)
    elif isinstance(value, dict):
        return {k: resolve_placeholder(v, job_base_dir) for k, v in value.items()}
    else:
        return value


def register_placeholder_resolver(
    ref_type: str,
    resolver: Callable[[dict[str, Any], str | None], Any],
) -> None:
    """Register resolver function for a placeholder reference type."""
    _PLACEHOLDER_RESOLVERS[ref_type] = resolver


def _resolve_ref_placeholder(
    placeholder: RefPlaceholder,
    job_base_dir: str | None = None,
) -> Any:
    resolver = _PLACEHOLDER_RESOLVERS.get(placeholder.ref_type)
    if resolver is None:
        raise ValueError(
            f"No resolver registered for placeholder type {placeholder.ref_type!r}"
        )
    return resolver(dict(placeholder.payload), job_base_dir)


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
    return _load_result_for_job_id(placeholder.job_id, job_base_dir)


def _load_result_for_job_id(job_id: str, job_base_dir: str | None = None) -> Any:
    """Load result for a known job identifier."""
    logger.debug("Resolving placeholder for job_id=%s", job_id)

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

            if not isinstance(metadata_map, dict):
                continue

            # Check if this metadata contains our job_id
            if job_id in metadata_map:
                result_dir = os.path.dirname(metadata_path)
                job_entry = metadata_map[job_id]
                if not isinstance(job_entry, dict):
                    continue
                result_filename = job_entry.get("result_file")
                if isinstance(result_filename, str) and result_filename:
                    result_path = os.path.join(result_dir, result_filename)
                    logger.debug("Found result file: %s", result_path)
                    # nosec B301 - result file created by SDK runner, stored in trusted job dir
                    with open(result_path, "rb") as f:
                        return pickle.load(f)  # nosec B301

            # Fallback for submission-shaped metadata where runner metadata was merged/overwritten.
            if metadata_map.get("job_id") == job_id:
                result_dir = os.path.dirname(metadata_path)
                result_filename = None

                unknown_entry = metadata_map.get("unknown")
                if isinstance(unknown_entry, dict):
                    candidate = unknown_entry.get("result_file")
                    if isinstance(candidate, str) and candidate:
                        result_filename = candidate

                if result_filename is None:
                    candidate = metadata_map.get("result_file")
                    if isinstance(candidate, str) and candidate:
                        result_filename = candidate

                if result_filename is None:
                    pre_submission_id = metadata_map.get("pre_submission_id")
                    if isinstance(pre_submission_id, str) and pre_submission_id:
                        result_filename = f"slurm_job_{pre_submission_id}_result.pkl"

                if result_filename:
                    result_path = os.path.join(result_dir, result_filename)
                    if os.path.exists(result_path):
                        logger.debug("Found result file via fallback: %s", result_path)
                        # nosec B301 - result file created by SDK runner, stored in trusted job dir
                        with open(result_path, "rb") as f:
                            return pickle.load(f)  # nosec B301
        except Exception as e:
            logger.warning("Error reading metadata from %s: %s", metadata_path, e)

    raise FileNotFoundError(f"Could not find result file for job_id={job_id}")


def _resolve_job_ref_payload(payload: dict[str, Any], job_base_dir: str | None) -> Any:
    job_id = payload.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job placeholder payload must contain non-empty 'job_id'")
    return _load_result_for_job_id(job_id, job_base_dir)


def _resolve_job_list_payload(payload: dict[str, Any], job_base_dir: str | None) -> Any:
    job_ids = payload.get("job_ids")
    if not isinstance(job_ids, list):
        raise ValueError("job_list placeholder payload must contain list 'job_ids'")
    return [_load_result_for_job_id(str(job_id), job_base_dir) for job_id in job_ids]


# Built-in resolver registrations
register_placeholder_resolver("job", _resolve_job_ref_payload)
register_placeholder_resolver("job_list", _resolve_job_list_payload)


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
    "register_placeholder_resolver",
]
