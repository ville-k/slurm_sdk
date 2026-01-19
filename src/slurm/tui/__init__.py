"""TUI components for the slurm CLI.

This module provides interactive terminal user interface components
using the Textual framework. It is an optional dependency that can be
installed with: pip install slurm-sdk[tui]
"""

from __future__ import annotations


def check_textual_available() -> None:
    """Check if Textual is installed and raise helpful error if not."""
    try:
        import textual  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "TUI features require the 'textual' package. "
            "Install it with: pip install slurm-sdk[tui]"
        ) from e
