"""Factory for creating Cluster instances in tests.

Uses ``Cluster.from_backend()`` to create properly initialised instances
with a pre-constructed backend, avoiding SSH connections and other
side-effects of the default constructor.
"""

from typing import Any, Dict, List, Optional

from slurm.callbacks import BaseCallback
from slurm.cluster import Cluster


def make_test_cluster(
    *,
    backend: Any,
    backend_type: str = "LocalBackend",
    callbacks: Optional[List[BaseCallback]] = None,
    job_base_dir: Optional[str] = None,
    default_packaging: Optional[str] = None,
    default_account: Optional[str] = None,
    default_partition: Optional[str] = None,
    default_packaging_kwargs: Optional[Dict[str, Any]] = None,
    packaging_defaults: Optional[Dict[str, Any]] = None,
    console: Any = None,
    **extra_attrs: Any,
) -> Cluster:
    """Create a Cluster instance with a pre-built backend for testing."""
    cluster = Cluster.from_backend(
        backend,
        backend_type=backend_type,
        callbacks=callbacks,
        default_packaging=default_packaging,
        default_account=default_account,
        default_partition=default_partition,
        default_packaging_kwargs=default_packaging_kwargs,
    )

    if packaging_defaults is not None:
        cluster.packaging_defaults = packaging_defaults
    if job_base_dir is not None:
        cluster.job_base_dir = job_base_dir
    if console is not None:
        cluster.console = console

    for key, value in extra_attrs.items():
        setattr(cluster, key, value)

    return cluster
