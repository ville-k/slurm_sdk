"""Minimal ``parallel(...)`` smoke example — runs without Slurm installed.

This example demonstrates Phase 12's local-mode bypass: a 2-peer
``parallel(...)`` submission executed by the Python supervisor directly,
with peers launched as plain subprocesses (no ``srun``). It mirrors
``hello_world.py`` in structure but exercises the parallel code path
without needing a real SLURM cluster.

Run it with::

    uv run python -m slurm.examples.parallel_simple

The leader returns a greeting; the sidecar returns its own pid so you can
verify that both peers actually ran in distinct processes.
"""

from __future__ import annotations

import logging
import os
import tempfile

from slurm import Peer, parallel, task
from slurm.cluster import Cluster


@task(time="00:02:00", mem="256M", cpus_per_task=1)
def leader() -> str:
    """Leader peer — returns a greeting string."""
    import socket

    return f"hello from leader on {socket.gethostname()} (pid {os.getpid()})"


@task(time="00:02:00", mem="256M", cpus_per_task=1)
def sidecar() -> int:
    """Sidecar peer — returns its own pid so we can prove it ran."""
    return os.getpid()


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    # Use a throwaway job base dir so repeated runs don't pile up.
    job_base_dir = tempfile.mkdtemp(prefix="slurm_sdk_parallel_simple_")
    cluster = Cluster(
        backend_type="local",
        job_base_dir=job_base_dir,
        default_packaging="none",
    )
    with cluster:
        job = parallel(
            Peer(leader, leader=True),
            Peer(sidecar, on_failure="continue"),
        )
        job.wait()
        results = job.get_results()

    print("Leader result:", results.get("leader"))
    print("Sidecar result:", results.get("sidecar"))
    assert "hello from leader" in (results.get("leader") or "")
    assert isinstance(results.get("sidecar"), int)
    print("parallel_simple smoke passed.")
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
