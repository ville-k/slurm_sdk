"""FastMCP server exposing the SLURM SDK APIs.

This module provides an MCP (Model Context Protocol) server that exposes
the SLURM SDK's functionality as tools and resources. It enables AI assistants
to interact with SLURM clusters for job monitoring and management.

Usage:
    # Start via CLI
    slurm mcp run

    # Or programmatically
    from slurm.mcp_server import create_mcp_server
    mcp = create_mcp_server()
    mcp.run()
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP

from .cluster import Cluster
from .config import resolve_slurmfile_path

logger = logging.getLogger(__name__)

_cluster_cache: Dict[str, Cluster] = {}


def _get_cluster(env: Optional[str] = None, slurmfile: Optional[str] = None) -> Cluster:
    """Get or create a cached Cluster instance.

    Args:
        env: Environment name from Slurmfile.
        slurmfile: Path to Slurmfile.

    Returns:
        Configured Cluster instance.
    """
    cache_key = f"{slurmfile or 'default'}:{env or 'default'}"

    if cache_key not in _cluster_cache:
        _cluster_cache[cache_key] = Cluster.from_env(slurmfile, env=env)

    return _cluster_cache[cache_key]


def create_mcp_server(
    name: str = "SLURM SDK",
    slurmfile: Optional[str] = None,
    env: Optional[str] = None,
) -> FastMCP:
    """Create a FastMCP server with SLURM SDK tools and resources.

    Args:
        name: Server name for MCP identification.
        slurmfile: Optional path to Slurmfile for configuration.
        env: Optional environment name from Slurmfile.

    Returns:
        Configured FastMCP server instance.
    """
    mcp = FastMCP(name=name)

    @mcp.tool
    def list_jobs(account: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs in the SLURM queue.

        Returns all jobs currently in the queue, optionally filtered by account.

        Args:
            account: Optional account/user to filter jobs by.

        Returns:
            List of job dictionaries with keys like JOBID, NAME, STATE, USER, etc.
        """
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        queue = cluster.get_queue()

        if account:
            queue = [
                j
                for j in queue
                if str(j.get("USER", "") or j.get("UserId", "")) == account
            ]

        return queue

    @mcp.tool
    def get_job(job_id: str) -> Dict[str, Any]:
        """Get detailed status for a specific job.

        Args:
            job_id: The SLURM job ID.

        Returns:
            Dictionary with job status including state, exit code, timing, etc.
        """
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        job = cluster.get_job(job_id)
        return job.get_status()

    @mcp.tool
    def cancel_job(job_id: str) -> Dict[str, Any]:
        """Cancel a running or pending job.

        Args:
            job_id: The SLURM job ID to cancel.

        Returns:
            Dictionary with cancellation status.
        """
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        job = cluster.get_job(job_id)

        try:
            job.cancel()
            return {"status": "cancelled", "job_id": job_id}
        except Exception as e:
            return {"status": "error", "job_id": job_id, "error": str(e)}

    @mcp.tool
    def list_partitions() -> List[Dict[str, Any]]:
        """List cluster partitions with availability and node counts.

        Returns:
            List of partition dictionaries with name, availability, nodes, etc.
        """
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        info = cluster.get_cluster_info()
        return info.get("partitions", [])

    @mcp.tool
    def list_environments() -> List[Dict[str, str]]:
        """List configured environments from the Slurmfile.

        Returns:
            List of environment dictionaries with name and hostname.
        """
        from .cli.utils import list_slurmfile_environments

        try:
            environments = list_slurmfile_environments(slurmfile=slurmfile)
            return [
                {
                    "name": e["name"],
                    "hostname": e.get("hostname", ""),
                    "slurmfile": e.get("slurmfile", ""),
                }
                for e in environments
            ]
        except Exception as e:
            return [{"error": str(e)}]

    @mcp.resource("slurm://queue")
    def queue_resource() -> str:
        """Current job queue as JSON."""
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        queue = cluster.get_queue()
        return json.dumps(queue, indent=2, default=str)

    @mcp.resource("slurm://jobs/{job_id}")
    def job_resource(job_id: str) -> str:
        """Job details as JSON.

        Args:
            job_id: The SLURM job ID.
        """
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        job = cluster.get_job(job_id)
        status = job.get_status()
        return json.dumps(status, indent=2, default=str)

    @mcp.resource("slurm://partitions")
    def partitions_resource() -> str:
        """Cluster partition info as JSON."""
        cluster = _get_cluster(env=env, slurmfile=slurmfile)
        info = cluster.get_cluster_info()
        return json.dumps(info.get("partitions", []), indent=2, default=str)

    @mcp.resource("slurm://config")
    def config_resource() -> str:
        """Current Slurmfile configuration as JSON."""
        from .cli.utils import list_slurmfile_environments

        try:
            resolved_path = resolve_slurmfile_path(slurmfile)
            environments = list_slurmfile_environments(slurmfile=slurmfile)

            config = {
                "slurmfile": str(resolved_path),
                "active_environment": env or "default",
                "environments": environments,
            }
            return json.dumps(config, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    return mcp


def run_server(
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8000,
    slurmfile: Optional[str] = None,
    env: Optional[str] = None,
) -> None:
    """Run the MCP server.

    Args:
        transport: Transport type - "stdio" or "http".
        host: Host to bind to (for HTTP transport).
        port: Port to bind to (for HTTP transport).
        slurmfile: Optional path to Slurmfile.
        env: Optional environment name from Slurmfile.
    """
    mcp = create_mcp_server(slurmfile=slurmfile, env=env)

    if transport == "http":
        mcp.run(transport="http", host=host, port=port)
    else:
        mcp.run()
