"""Callback execution utilities for the runner."""

import logging
from typing import List

from slurm.callbacks.callbacks import BaseCallback

logger = logging.getLogger("slurm.runner")


def run_callbacks(callbacks: List[BaseCallback], method_name: str, *args, **kwargs):
    """Run a specific method on a list of callbacks, catching errors.

    This function iterates through the provided callbacks and calls the specified
    method on each one. It respects the `should_run_on_runner` filter if present,
    and catches any exceptions to prevent one callback's failure from affecting others.

    Args:
        callbacks: List of callback instances to execute
        method_name: Name of the method to call on each callback
        *args: Positional arguments to pass to the callback method
        **kwargs: Keyword arguments to pass to the callback method
    """
    for callback in callbacks:
        if hasattr(
            callback, "should_run_on_runner"
        ) and not callback.should_run_on_runner(method_name):
            continue
        try:
            method = getattr(callback, method_name)
            method(*args, **kwargs)
        except Exception as e:
            logger.warning(
                f"Runner: Error executing callback {type(callback).__name__}.{method_name}: {e}"
            )
