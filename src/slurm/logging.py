"""
Logging helpers for the slurm SDK.

Provides a single entrypoint `configure_logging` to establish pleasant
defaults for everyday use, while keeping debug details available at
DEBUG level.
"""

from __future__ import annotations

import logging
from typing import Optional


def _quiet_third_party() -> None:
    """Reduce verbosity of common third-party libraries."""
    for name in (
        "paramiko",
        "paramiko.transport",
        "paramiko.transport.sftp",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def configure_logging(level: int = logging.INFO, use_rich: bool = True) -> None:
    """Configure application logging.

    Args:
        level: Root logging level (default: logging.INFO).
        use_rich: If True and rich is available, install a Rich handler with
            a concise format (no duplicated level text in messages).
    """
    _quiet_third_party()

    # Remove any pre-existing handlers to avoid duplicate output
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(level)

    handler: Optional[logging.Handler] = None
    if use_rich:
        try:
            from rich.logging import RichHandler  # type: ignore

            handler = RichHandler(
                show_time=True,
                show_path=False,
                markup=True,
                rich_tracebacks=True,
                enable_link_path=False,
            )
            # Use a concise format to avoid duplicating level text
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
        except Exception:
            handler = None

    if handler is None:
        # Fallback to a simple StreamHandler without duplicated level text
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))

    root.addHandler(handler)
