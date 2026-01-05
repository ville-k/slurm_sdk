"""Context injection utilities for the runner.

This module handles injecting WorkflowContext and JobContext into
task/workflow functions that expect them.
"""

import inspect
import logging
from typing import Tuple

from slurm.workflow import WorkflowContext

# Re-export JobContext binding functions from runtime
from slurm.runtime import (
    _bind_job_context,
    _function_wants_job_context,
)

logger = logging.getLogger("slurm.runner")


def function_wants_workflow_context(func) -> bool:
    """Check if function expects a WorkflowContext parameter.

    Detection is done by:
    1. Checking if any parameter has WorkflowContext type annotation
    2. Checking if any parameter has 'WorkflowContext' string annotation
    3. Checking if any parameter is named 'ctx', 'context', or 'workflow_context'

    Args:
        func: The function to check

    Returns:
        True if the function expects a WorkflowContext parameter
    """
    try:
        sig = inspect.signature(func)
        for param in sig.parameters.values():
            # Check by annotation
            annotation = param.annotation
            if annotation != inspect.Parameter.empty:
                if annotation is WorkflowContext:
                    return True
                # Check string annotations
                if isinstance(annotation, str) and "WorkflowContext" in annotation:
                    return True
                # Check name attribute
                if (
                    hasattr(annotation, "__name__")
                    and annotation.__name__ == "WorkflowContext"
                ):
                    return True
            # Check by parameter name
            if param.name in ("ctx", "context", "workflow_context"):
                return True
        return False
    except Exception:
        return False


def bind_workflow_context(
    func, args: tuple, kwargs: dict, workflow_context: WorkflowContext
) -> Tuple[tuple, dict, bool]:
    """Inject workflow_context into function if it expects it.

    Args:
        func: The function to bind context to
        args: Positional arguments tuple
        kwargs: Keyword arguments dict
        workflow_context: The WorkflowContext to inject

    Returns:
        Tuple of (args, kwargs, injected) where injected is True if
        context was injected, False otherwise.
    """
    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # Find the parameter that wants WorkflowContext
        target_param = None
        for param in params:
            annotation = param.annotation
            # Check by annotation
            if annotation != inspect.Parameter.empty:
                if annotation is WorkflowContext:
                    target_param = param
                    break
                if isinstance(annotation, str) and "WorkflowContext" in annotation:
                    target_param = param
                    break
                if (
                    hasattr(annotation, "__name__")
                    and annotation.__name__ == "WorkflowContext"
                ):
                    target_param = param
                    break
            # Check by parameter name
            if param.name in ("ctx", "context", "workflow_context"):
                target_param = param
                break

        if target_param is None:
            return args, kwargs, False

        param_name = target_param.name

        # If already provided, don't inject
        if param_name in kwargs:
            return args, kwargs, False

        # Inject as keyword argument
        new_kwargs = dict(kwargs)
        new_kwargs[param_name] = workflow_context
        return args, new_kwargs, True

    except Exception as e:
        logger.warning(f"Error binding workflow context: {e}")
        return args, kwargs, False


# Aliases for backwards compatibility (prefixed with underscore)
_function_wants_workflow_context = function_wants_workflow_context
_bind_workflow_context = bind_workflow_context

__all__ = [
    "function_wants_workflow_context",
    "bind_workflow_context",
    "_function_wants_workflow_context",
    "_bind_workflow_context",
    # Re-exported from runtime
    "_function_wants_job_context",
    "_bind_job_context",
]
