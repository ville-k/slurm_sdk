"""Shared utilities for the slurm CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import (
    _extract_environment_table,
    _extract_root_table,
    _read_toml,
    resolve_slurmfile_path,
)


def get_cluster(
    env: Optional[str] = None,
    slurmfile: Optional[str] = None,
) -> "Cluster":  # noqa: F821
    """Create a Cluster from CLI args.

    Args:
        env: Environment name to load from Slurmfile.
        slurmfile: Path to Slurmfile.

    Returns:
        Configured Cluster instance.
    """
    from ..cluster import Cluster

    return Cluster.from_env(slurmfile, env=env)


def list_slurmfile_environments(
    slurmfile: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Parse Slurmfile to list environments without connecting.

    Args:
        slurmfile: Optional path to Slurmfile.

    Returns:
        List of environment info dicts with keys: name, has_hostname, hostname.
    """
    resolved_path = resolve_slurmfile_path(slurmfile)
    raw_data = _read_toml(resolved_path)
    root_table = _extract_root_table(raw_data)
    env_table = _extract_environment_table(root_table)

    environments: List[Dict[str, Any]] = []

    for name, config in env_table.items():
        if not isinstance(config, dict):
            continue

        # Look for hostname in multiple possible locations based on Slurmfile format
        hostname = ""
        # Format 1: config.hostname (simple/flat format)
        if config.get("hostname"):
            hostname = config["hostname"]
        # Format 2: config.cluster.backend_config.hostname (standard format)
        elif isinstance(config.get("cluster"), dict):
            cluster_config = config["cluster"]
            backend_config = cluster_config.get("backend_config", {})
            if isinstance(backend_config, dict):
                hostname = backend_config.get("hostname", "")

        environments.append(
            {
                "name": name,
                "hostname": hostname,
                "has_hostname": bool(hostname),
                "slurmfile": str(resolved_path),
            }
        )

    if not environments:
        default_config = root_table.get("default", root_table)
        if isinstance(default_config, dict):
            # Same hostname lookup logic as above
            hostname = ""
            if default_config.get("hostname"):
                hostname = default_config["hostname"]
            elif isinstance(default_config.get("cluster"), dict):
                cluster_config = default_config["cluster"]
                backend_config = cluster_config.get("backend_config", {})
                if isinstance(backend_config, dict):
                    hostname = backend_config.get("hostname", "")

            # Only add default if there's meaningful config
            if hostname or default_config.get("cluster"):
                environments.append(
                    {
                        "name": "default",
                        "hostname": hostname,
                        "has_hostname": bool(hostname),
                        "slurmfile": str(resolved_path),
                    }
                )

    return environments


def get_slurmfile_path(slurmfile: Optional[str] = None) -> Path:
    """Resolve Slurmfile path.

    Args:
        slurmfile: Optional explicit path to Slurmfile.

    Returns:
        Resolved Path to Slurmfile.
    """
    return resolve_slurmfile_path(slurmfile)
