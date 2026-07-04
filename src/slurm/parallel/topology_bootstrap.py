"""Allocation-time bootstrap for ``parallel(...)`` submissions.

Runs once on the batch allocation's head node, before the supervisor:

1. Read ``$JOB_DIR/plan.json``.
2. Resolve the allocation's hostnames from ``SLURM_JOB_NODELIST`` /
   ``SLURM_NODELIST`` via ``scontrol show hostnames`` when available; an
   in-process expander handles the common cases in tests and
   scontrol-less environments.
3. Write a registry skeleton to ``$JOB_DIR/registry.json``. Every peer
   starts with ``hostname=""`` and ``state="pending"`` — Slurm chooses
   the node at ``srun`` time, so the peer's own runner publishes the
   real hostname via :func:`update_peer_hostinfo` at startup.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess  # nosec B404 - used to invoke scontrol
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from ..runtime import _expand_nodelist
from .plan import Plan, read_plan
from .registry import STATE_PENDING, write_registry

logger = logging.getLogger("slurm.parallel.bootstrap")


def _resolve_hostnames(nodelist: str) -> Tuple[str, ...]:
    """Expand a Slurm nodelist spec to concrete hostnames.

    Prefers ``scontrol show hostnames`` when the tool is installed — it
    handles every bracket form Slurm emits. Falls back to the in-process
    ``_expand_nodelist`` (same one the runtime uses) when ``scontrol`` is
    missing (tests, bare containers without the Slurm CLI).
    """
    if not nodelist:
        return tuple()

    try:
        result = subprocess.run(  # nosec B603,B607 - trusted scontrol call
            ["scontrol", "show", "hostnames", nodelist],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
    except FileNotFoundError:
        logger.debug("scontrol not found; falling back to in-process nodelist expander")
        return _expand_nodelist(nodelist)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        logger.warning(
            "scontrol show hostnames failed (%s); falling back to in-process expander",
            exc,
        )
        return _expand_nodelist(nodelist)

    return tuple(line for line in result.stdout.splitlines() if line.strip())


def build_registry_skeleton(
    plan: Plan,
    hostnames: Tuple[str, ...],
) -> dict:
    """Build the initial registry dict — all peers ``pending``.

    Every peer entry lists the allocation's hostnames. Replica peers get
    ``peer.replica_count`` entries in the registry list (one per replica
    index); singleton peers get exactly one.

    Every peer is seeded with ``hostname=""`` and stays in
    :data:`STATE_PENDING`. Slurm chooses the node at step-launch time; the
    peer's own runner publishes the real hostname via
    :func:`update_peer_hostinfo` as soon as it starts. Callers that need the
    value must ``wait_all(keys=["hostname"])``.

    Args:
        plan: Parsed supervisor plan.
        hostnames: The allocation's hostname list.
    """
    peers_out: dict = {}
    for peer in plan.peers:
        entries = []
        for replica_index in range(peer.replica_count):
            entries.append(
                {
                    "name": peer.name,
                    "pool": peer.pool,
                    "replica_index": replica_index,
                    "replica_count": peer.replica_count,
                    "hostname": "",
                    "hostnames": list(hostnames),
                    "step_id": None,
                    "metadata": {},
                    "state": STATE_PENDING,
                    "outcome": None,
                    "final_exit_code": None,
                    "message": None,
                }
            )
        peers_out[peer.name] = entries

    return {"peers": peers_out}


def _configure_logging() -> None:
    loglevel = os.environ.get("SLURM_SDK_LOGLEVEL", "INFO").upper()
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Parallel-allocation bootstrap")
    parser.add_argument(
        "--job-dir",
        required=True,
        help="Target job directory where plan.json lives and registry.json is written",
    )
    args = parser.parse_args(argv)

    _configure_logging()

    job_dir = Path(args.job_dir)
    plan_path = job_dir / "plan.json"
    if not plan_path.exists():
        logger.error("plan.json not found at %s — cannot bootstrap registry", plan_path)
        return 1

    # Create the allocation-shared directory before any peer launches so
    # cross-peer writes race against neither bootstrap nor sibling writers.
    shared_dir = job_dir / "shared"
    try:
        shared_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("Could not create shared dir %s: %s", shared_dir, exc)

    plan = read_plan(plan_path)

    hostnames = _resolve_allocation_hostnames()

    skeleton = build_registry_skeleton(plan, hostnames)
    write_registry(job_dir / "registry.json", skeleton)
    logger.info("Registry skeleton written to %s", job_dir / "registry.json")

    return 0


def _resolve_allocation_hostnames() -> Tuple[str, ...]:
    """Resolve the allocation's hostnames from the Slurm nodelist env vars.

    Reads ``SLURM_JOB_NODELIST`` (falling back to ``SLURM_NODELIST``) and
    expands it via ``scontrol show hostnames`` when available, with an
    in-process expander as the fallback.
    """
    nodelist = (
        os.environ.get("SLURM_JOB_NODELIST") or os.environ.get("SLURM_NODELIST") or ""
    )
    hostnames = _resolve_hostnames(nodelist) if nodelist else ()
    logger.info(
        "Bootstrap resolved %d hostname(s) from nodelist %r",
        len(hostnames),
        nodelist,
    )
    return hostnames


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
