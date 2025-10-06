"""
This package provides interfaces for interacting with Slurm clusters
through various backends.
"""

from typing import Any, Dict
from .base import BackendBase
from .ssh import SSHCommandBackend


def create_backend(backend_type: str, **kwargs: Any) -> BackendBase:
    """
    Create a SLURM API backend of the specified type.

    Args:
        backend_type: The type of backend to create ("ssh").
        **kwargs: Additional arguments to pass to the backend constructor.

    Returns:
        A SLURM API backend instance.

    Raises:
        ValueError: If the specified backend type is not supported.
    """
    if backend_type == "ssh":
        # Filter out None values to avoid passing None to the backend
        ssh_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return SSHCommandBackend(**ssh_kwargs)
    else:
        raise ValueError(f"Unsupported backend type: {backend_type}")


def get_backend_from_config(config: Dict[str, Any]) -> BackendBase:
    """
    Create a SLURM API backend based on a configuration dictionary.

    The configuration dictionary should have a "backend" key specifying the backend type,
    and backend-specific configuration parameters.

    Args:
        config: Configuration dictionary.

    Returns:
        BackendBase: An instance of the specified backend.

    Raises:
        ValueError: If the configuration is invalid.
        RuntimeError: If the backend initialization fails.
    """
    if "backend" not in config:
        raise ValueError("Configuration must specify a 'backend' key.")

    backend_type = config["backend"]
    backend_config = config.get("backend_config", {})

    return create_backend(backend_type, **backend_config)
