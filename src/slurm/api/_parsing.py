"""Shared parsers for Slurm command output.

Both the SSH and local backends execute the same Slurm commands (scontrol,
sacct, squeue, sinfo) and parse identical output formats.  This module
provides the parsing logic once so backends only handle command execution
and error recovery.
"""

from typing import Any, Dict, List


def parse_scontrol_status(stdout: str) -> Dict[str, Any]:
    """Parse ``scontrol show job`` key=value output into a dict."""
    status: Dict[str, Any] = {}
    for line in stdout.strip().split("\n"):
        for item in line.strip().split():
            if "=" in item:
                key, value = item.split("=", 1)
                status[key] = value
    return status


def parse_sacct_accounting(stdout: str, job_id: str) -> Dict[str, Any]:
    """Parse ``sacct --parsable2`` output for a single job.

    Returns a dict with JobID, JobState, ExitCode, StartTime, EndTime, Elapsed.

    Raises:
        ValueError: If the output is empty or has fewer than 6 fields.
    """
    lines = stdout.strip().split("\n")
    if not lines or not lines[0]:
        raise ValueError(f"No accounting data found for job {job_id}")

    parts = lines[0].split("|")
    if len(parts) < 6:
        raise ValueError(f"Unexpected sacct output format for job {job_id}: {stdout}")

    return {
        "JobID": parts[0],
        "JobState": parts[1],
        "ExitCode": parts[2],
        "StartTime": parts[3],
        "EndTime": parts[4],
        "Elapsed": parts[5],
    }


def parse_sacct_account_jobs(stdout: str) -> List[Dict[str, Any]]:
    """Parse ``sacct --parsable2`` output for multiple jobs in an account.

    Skips job steps (entries with '.' in the JobID) and lines with
    fewer than 12 fields.
    """
    import logging

    logger = logging.getLogger(__name__)

    lines = stdout.strip().split("\n")
    if not lines or not lines[0]:
        return []

    jobs: List[Dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) < 12:
            logger.warning("Unexpected sacct output format: %s", line)
            continue

        job_id = parts[0]

        # Skip job steps (e.g., "12345.batch", "12345.0")
        if "." in job_id:
            continue

        jobs.append(
            {
                "JobID": parts[0],
                "JobName": parts[1],
                "User": parts[2],
                "Account": parts[3],
                "State": parts[4],
                "ExitCode": parts[5],
                "AllocTRES": parts[6] if parts[6] else "",
                "AllocGRES": parts[6] if parts[6] else "",
                "AllocNodes": parts[7],
                "Start": parts[8],
                "End": parts[9],
                "Elapsed": parts[10],
                "Partition": parts[11],
            }
        )

    return jobs


def parse_squeue_output(stdout: str) -> List[Dict[str, Any]]:
    """Parse pipe-delimited ``squeue`` output.

    Expected format from ``squeue -h -o '%A|%j|%T|%u|%S|%M|%l|%P|%a|%D|%r'``.
    """
    jobs: List[Dict[str, Any]] = []
    for line in stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) >= 11:
            (
                job_id,
                job_name,
                state,
                user,
                start_time,
                time_used,
                time_limit,
                partition,
                account,
                num_nodes,
                reason,
            ) = parts[:11]
            jobs.append(
                {
                    "JOBID": job_id,
                    "NAME": job_name,
                    "STATE": state,
                    "USER": user,
                    "START_TIME": start_time,
                    "TIME": time_used,
                    "TIME_LIMIT": time_limit,
                    "PARTITION": partition,
                    "ACCOUNT": account,
                    "NODES": num_nodes,
                    "REASON": reason,
                }
            )

    return jobs


def parse_sinfo_output(stdout: str) -> List[Dict[str, Any]]:
    """Parse pipe-delimited ``sinfo`` output.

    Expected format from ``sinfo -h -o '%R|%a|%l|%D|%T'``.
    """
    partitions: List[Dict[str, Any]] = []
    for line in stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) >= 5:
            partition, avail, time_limit, nodes, state = parts[:5]
            partitions.append(
                {
                    "PARTITION": partition,
                    "AVAIL": avail,
                    "TIMELIMIT": time_limit,
                    "NODES": nodes,
                    "STATE": state,
                }
            )

    return partitions
