"""Centralized packaging configuration resolution.

This module is the single source of truth for determining which packaging
configuration applies to a task submission.  All code paths that need to
resolve packaging — task submission, dependency container prebuilds, workflow
Slurmfile generation, and runner-side reconstruction — should use the helpers
defined here.
"""

import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .cluster import Cluster
    from .task import SlurmTask

logger = logging.getLogger(__name__)


def resolve_packaging_config(
    cluster: "Cluster",
    task_func: "SlurmTask",
    explicit_config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve the effective packaging configuration for a task.

    Precedence (highest to lowest):

    1. Pre-built dependency images (from ``.with_dependencies()``)
    2. Explicit ``explicit_config`` parameter
    3. Task-level packaging (unless type is ``auto``, ``inherit``, or ``None``)
    4. Cluster ``default_packaging`` merged with task packaging kwargs
    5. Cluster ``packaging_defaults`` (legacy Slurmfile ``[packaging]`` table)
    6. Task packaging as final fallback

    Returns:
        Resolved packaging config dict, or ``None`` if no packaging is configured.
    """
    from .decorators import _parse_packaging_config

    # 1. Pre-built dependency images take highest priority
    prebuilt_images = getattr(cluster, "_prebuilt_dependency_images", None)
    if prebuilt_images:
        func = task_func.func
        task_name = f"{func.__module__}.{func.__qualname__}"
        logger.debug(
            f"Checking pre-built images for {task_name}. "
            f"Available: {list(prebuilt_images.keys())}"
        )
        if task_name in prebuilt_images:
            prebuilt_image = prebuilt_images[task_name]
            logger.info(f"Using pre-built image for {task_name}: {prebuilt_image}")
            return {
                "type": "container",
                "image": prebuilt_image,
                "push": False,
            }
        logger.debug(f"No pre-built image found for {task_name}")

    # 2. Explicit config passed by caller
    if explicit_config is not None:
        return explicit_config

    # 3-6. Resolve from task / cluster / Slurmfile defaults
    task_packaging = task_func.packaging

    # 3. Task-level packaging with a concrete type
    if task_packaging and task_packaging.get("type") not in (
        "auto",
        "inherit",
        None,
    ):
        return task_packaging

    # 4. Cluster default_packaging merged with task kwargs
    if cluster.default_packaging:
        merged_kwargs = dict(cluster.default_packaging_kwargs)
        if task_packaging:
            task_packaging_kwargs = {
                k: v for k, v in task_packaging.items() if k != "type"
            }
            merged_kwargs.update(task_packaging_kwargs)
        return _parse_packaging_config(cluster.default_packaging, merged_kwargs)

    # 5. Legacy Slurmfile packaging_defaults
    packaging_defaults = getattr(cluster, "packaging_defaults", None)
    if packaging_defaults is not None:
        return packaging_defaults

    # 6. Task packaging as final fallback
    return task_packaging


def resolve_packaging_for_slurmfile(cluster: "Cluster") -> Dict[str, Any]:
    """Resolve cluster-level packaging config for workflow Slurmfile generation.

    This is a simpler variant that does not consider task-level packaging
    (not known at Slurmfile generation time).  It merges the cluster's
    ``default_packaging`` string with ``default_packaging_kwargs``.

    Returns:
        Packaging config dict (may be empty).
    """
    from .decorators import _parse_packaging_config

    config = _parse_packaging_config(cluster.default_packaging or "auto", {})
    config = config or {}
    config.update(cluster.default_packaging_kwargs)
    return config
