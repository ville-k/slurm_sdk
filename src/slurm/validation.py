"""Input validation utilities for SLURM SDK.

This module provides validation functions for user-provided input to prevent
shell injection and other security issues. All validation functions raise
ValueError for invalid input.
"""

import re
from typing import Optional

# SLURM allows alphanumeric characters, underscores, hyphens, and periods
# for most identifiers. Job names can also include some special characters.
_SLURM_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")

# Job names can include more characters but should still be restricted
# to prevent shell injection
_SLURM_JOB_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.:/]+$")

# Maximum length for SLURM identifiers
_MAX_IDENTIFIER_LENGTH = 256
_MAX_JOB_NAME_LENGTH = 1024


def validate_slurm_identifier(
    value: str,
    field_name: str,
    max_length: int = _MAX_IDENTIFIER_LENGTH,
) -> str:
    """Validate a SLURM identifier (account, partition, etc.).

    Args:
        value: The value to validate.
        field_name: Name of the field (for error messages).
        max_length: Maximum allowed length.

    Returns:
        The validated value (stripped of whitespace).

    Raises:
        ValueError: If the value is invalid.
    """
    if not value:
        raise ValueError(f"{field_name} cannot be empty")

    value = value.strip()

    if len(value) > max_length:
        raise ValueError(
            f"{field_name} exceeds maximum length of {max_length} characters"
        )

    if not _SLURM_IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{field_name} contains invalid characters. "
            f"Only alphanumeric characters, underscores, hyphens, and periods are allowed."
        )

    return value


def validate_account(account: Optional[str]) -> Optional[str]:
    """Validate a SLURM account name.

    Args:
        account: The account name to validate, or None.

    Returns:
        The validated account name, or None if input was None.

    Raises:
        ValueError: If the account name is invalid.
    """
    if account is None:
        return None
    return validate_slurm_identifier(account, "Account")


def validate_partition(partition: Optional[str]) -> Optional[str]:
    """Validate a SLURM partition name.

    Args:
        partition: The partition name to validate, or None.

    Returns:
        The validated partition name, or None if input was None.

    Raises:
        ValueError: If the partition name is invalid.
    """
    if partition is None:
        return None
    return validate_slurm_identifier(partition, "Partition")


def validate_job_name(job_name: Optional[str]) -> Optional[str]:
    """Validate a SLURM job name.

    Job names have slightly more relaxed rules than other identifiers,
    allowing colons and forward slashes.

    Args:
        job_name: The job name to validate, or None.

    Returns:
        The validated job name, or None if input was None.

    Raises:
        ValueError: If the job name is invalid.
    """
    if job_name is None:
        return None

    job_name = job_name.strip()

    if not job_name:
        raise ValueError("Job name cannot be empty")

    if len(job_name) > _MAX_JOB_NAME_LENGTH:
        raise ValueError(
            f"Job name exceeds maximum length of {_MAX_JOB_NAME_LENGTH} characters"
        )

    if not _SLURM_JOB_NAME_PATTERN.match(job_name):
        raise ValueError(
            "Job name contains invalid characters. "
            "Only alphanumeric characters, underscores, hyphens, periods, "
            "colons, and forward slashes are allowed."
        )

    return job_name


def validate_job_id(job_id: str) -> str:
    """Validate a SLURM job ID.

    Job IDs are numeric, but may include array notation like "12345_[0-10]"
    or "12345_5".

    Args:
        job_id: The job ID to validate.

    Returns:
        The validated job ID.

    Raises:
        ValueError: If the job ID is invalid.
    """
    if not job_id:
        raise ValueError("Job ID cannot be empty")

    job_id = job_id.strip()

    # Job ID pattern: numeric, optionally with array notation
    # Examples: "12345", "12345_5", "12345_[0-10]", "12345_[0-10%5]"
    job_id_pattern = re.compile(r"^\d+(_(\d+|\[\d+(-\d+)?(%\d+)?\]))?$")

    if not job_id_pattern.match(job_id):
        raise ValueError(
            f"Invalid job ID format: {job_id}. "
            "Expected numeric ID with optional array notation."
        )

    return job_id


def validate_environment_variable_name(name: str) -> str:
    """Validate an environment variable name.

    Environment variable names must start with a letter or underscore,
    followed by letters, digits, or underscores.

    Args:
        name: The environment variable name to validate.

    Returns:
        The validated name.

    Raises:
        ValueError: If the name is invalid.
    """
    if not name:
        raise ValueError("Environment variable name cannot be empty")

    name = name.strip()

    env_var_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    if not env_var_pattern.match(name):
        raise ValueError(
            f"Invalid environment variable name: {name}. "
            "Names must start with a letter or underscore, "
            "followed by letters, digits, or underscores."
        )

    return name
