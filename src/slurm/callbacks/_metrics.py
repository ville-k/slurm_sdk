"""Shared workflow metrics helpers used by LoggerCallback and BenchmarkCallback."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from .base import WORKFLOW_METRICS_FILENAME


def log_workflow_metrics(
    logger: logging.Logger, workflow_name: str, metrics: Dict[str, Any]
) -> None:
    """Log a summary of workflow performance metrics."""
    child_count = metrics.get("child_count", 0)
    duration = metrics.get("duration_seconds")

    if duration is not None:
        logger.info(
            "[Workflow] '%s' completed in %.2fs (%d child tasks)",
            workflow_name,
            duration,
            child_count,
        )
    else:
        logger.info(
            "[Workflow] '%s' completed (%d child tasks)",
            workflow_name,
            child_count,
        )

    overhead = metrics.get("orchestration_overhead_ms")
    if overhead is not None:
        logger.info("  Avg submission interval: %.2fms", overhead)
        min_interval = metrics.get("min_submission_interval_ms")
        max_interval = metrics.get("max_submission_interval_ms")
        if min_interval is not None and max_interval is not None:
            logger.info(
                "  Min/Max interval: %.2fms / %.2fms",
                min_interval,
                max_interval,
            )

        throughput = metrics.get("submission_throughput")
        if throughput is not None:
            logger.info(
                "  Submission throughput: %.2f tasks/sec",
                throughput,
            )

    child_avg = metrics.get("child_avg_duration")
    if child_avg is not None:
        logger.info(
            "  Child task durations: avg=%.2fs min=%.2fs max=%.2fs",
            child_avg,
            metrics.get("child_min_duration"),
            metrics.get("child_max_duration"),
        )


def persist_metrics_to_disk(
    logger: logging.Logger,
    workflow_dir: Path,
    metrics: Dict[str, Any],
    metrics_filename: str = WORKFLOW_METRICS_FILENAME,
) -> None:
    """Write workflow metrics JSON to disk (best-effort)."""
    try:
        workflow_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = workflow_dir / metrics_filename
        with metrics_path.open("w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)
    except Exception as exc:  # pragma: no cover - best effort logging only
        logger.debug(
            "Failed to write workflow metrics to %s: %s",
            workflow_dir,
            exc,
        )
