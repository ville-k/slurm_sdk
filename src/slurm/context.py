"""Context management for cluster and workflow execution.

This module provides context tracking using Python's contextvars for
async-safe, thread-safe execution context management. It enables tasks
and workflows to automatically detect the active cluster or workflow
context when called.

Note: This is an internal module. The APIs are prefixed with underscore
to indicate they are not part of the public API.
"""

import contextvars
from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .cluster import Cluster
    from .workflow import WorkflowContext


# Context variable for tracking active cluster or workflow context
# This is async-safe and automatically inherits to child threads
_cluster_context: contextvars.ContextVar[
    Optional[Union["Cluster", "WorkflowContext"]]
] = contextvars.ContextVar("cluster_context", default=None)


def _get_active_context() -> Optional[Union["Cluster", "WorkflowContext"]]:
    """Get the currently active cluster or workflow context.

    Works correctly with:
    - Regular execution
    - Threading (auto-inherits to child threads)
    - Async/await (preserves context across await points)
    - Nested contexts (automatic stack management)

    Returns:
        The active Cluster or WorkflowContext, or None if no context is active.
    """
    return _cluster_context.get()


def _set_active_context(
    context: Optional[Union["Cluster", "WorkflowContext"]],
) -> contextvars.Token:
    """Set the active context and return a token for resetting.

    Args:
        context: The Cluster or WorkflowContext to set as active.

    Returns:
        A token that can be used with _reset_active_context() to restore
        the previous context state.
    """
    return _cluster_context.set(context)


def _reset_active_context(token: contextvars.Token) -> None:
    """Reset the context to its previous state using a token.

    Args:
        token: The token returned from _set_active_context().
    """
    _cluster_context.reset(token)


def _clear_active_context() -> None:
    """Clear the active context completely (set to None).

    This is primarily useful for testing to ensure a clean state.
    """
    _cluster_context.set(None)
