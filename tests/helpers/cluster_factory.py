"""Factory for creating bare Cluster instances in tests.

Avoids the repetitive ``object.__new__(Cluster)`` + manual attribute
setting pattern used across the test suite.
"""

import threading
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
    """Create a Cluster instance without invoking ``__init__``.

    This bypasses backend creation (SSH connections, etc.) and sets only
    the attributes that production code reads.
    """
    cluster = object.__new__(Cluster)
    cluster.backend_type = backend_type
    cluster.backend = backend
    cluster.callbacks = callbacks or []
    cluster.default_packaging = default_packaging
    cluster.default_account = default_account
    cluster.default_partition = default_partition
    cluster.default_packaging_kwargs = default_packaging_kwargs or {}
    cluster._job_pollers = {}
    cluster._job_pollers_lock = threading.Lock()
    cluster.console = console

    if packaging_defaults is not None:
        cluster.packaging_defaults = packaging_defaults
    if job_base_dir is not None:
        cluster.job_base_dir = job_base_dir

    for key, value in extra_attrs.items():
        setattr(cluster, key, value)

    return cluster
