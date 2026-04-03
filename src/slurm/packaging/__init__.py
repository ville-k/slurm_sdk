"""
Packaging strategies for SLURM jobs.
"""

from typing import Any, Dict, List, Optional, TypedDict

from .base import PackagingStrategy
from .none import NonePackagingStrategy
from .container import ContainerPackagingStrategy
from .wheel import WheelPackagingStrategy
from .inherit import InheritPackagingStrategy


class PackagingConfig(TypedDict, total=False):
    """Configuration dictionary for packaging strategies.

    All fields are optional. The ``type`` field determines which strategy
    is used; other fields are strategy-specific.

    This TypedDict documents the valid keys accepted by
    :func:`get_packaging_strategy` and returned by
    :func:`~slurm.decorators.parse_packaging_config`.
    """

    type: str
    """Strategy type: ``"auto"``, ``"wheel"``, ``"none"``, ``"container"``, or ``"inherit"``."""

    # Container-specific options
    image: str
    """Container image reference (e.g. ``"nvcr.io/nvidia/pytorch:24.01"``)."""
    dockerfile: str
    """Path to Dockerfile for building the image."""
    context: str
    """Docker build context directory."""
    name: str
    """Image name override."""
    tag: str
    """Image tag override."""
    registry: str
    """Registry URL for pushing/pulling images."""
    platform: str
    """Target platform (e.g. ``"linux/amd64"``)."""
    build_args: Dict[str, Any]
    """Docker build arguments."""
    push: bool
    """Whether to push the built image to the registry."""
    use_digest: bool
    """Pin image by SHA256 digest for reproducibility."""
    no_cache: bool
    """Disable Docker build cache."""
    tls_verify: bool
    """Verify TLS certificates when communicating with the registry."""
    runtime: str
    """Container runtime: ``"docker"`` or ``"podman"``."""
    python_executable: str
    """Python executable inside the container (e.g. ``"uv run python"``)."""
    modules: List[str]
    """Environment modules to load before execution."""
    srun_args: List[str]
    """Additional arguments passed to ``srun``."""
    mount_job_dir: bool
    """Whether to mount the job directory into the container."""
    mounts: List[Any]
    """Additional container mount specifications."""
    build_secrets: List[Any]
    """Docker build secrets."""
    workdir: str
    """Working directory inside the container."""


_STRATEGIES = {
    "wheel": WheelPackagingStrategy,
    "none": NonePackagingStrategy,
    "container": ContainerPackagingStrategy,
    "inherit": InheritPackagingStrategy,
}


def get_packaging_strategy(config: Optional[Dict[str, Any]]) -> PackagingStrategy:
    """
    Get the packaging strategy based on the configuration.

    Args:
        config: Packaging configuration dictionary.
                Expected keys: 'type' (e.g., 'auto', 'wheel', 'none', 'container').

    Returns:
        An instance of the appropriate PackagingStrategy subclass.
        Defaults to NonePackagingStrategy if config is None or type is missing/invalid.
    """
    if config is None:
        return NonePackagingStrategy()

    strategy_type = config.get("type")

    # Handle "auto" type - detect based on presence of pyproject.toml
    if strategy_type == "auto":
        import pathlib

        # Check for pyproject.toml in current directory or parents
        cwd = pathlib.Path.cwd()
        for directory in [cwd] + list(cwd.parents):
            if (directory / "pyproject.toml").exists():
                strategy_type = "wheel"
                break
        else:
            # No pyproject.toml found, use none
            strategy_type = "none"

    if not strategy_type:
        return NonePackagingStrategy()

    strategy_class = _STRATEGIES.get(strategy_type)
    if strategy_class:
        return strategy_class(config)
    else:
        return NonePackagingStrategy()


__all__ = [
    "get_packaging_strategy",
    "PackagingConfig",
    "PackagingStrategy",
    "WheelPackagingStrategy",
    "NonePackagingStrategy",
    "ContainerPackagingStrategy",
    "InheritPackagingStrategy",
]
