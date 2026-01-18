"""Command-line interface for the slurm SDK.

This module provides the `slurm` CLI command for managing jobs and clusters.

Usage:
    slurm jobs list [--env ENV] [--slurmfile PATH]
    slurm jobs show <job-id> [--env ENV] [--slurmfile PATH]
    slurm cluster list [--slurmfile PATH]
    slurm cluster show [--env ENV] [--slurmfile PATH]
"""

from .app import app, main

__all__ = ["app", "main"]
