"""Allocation-time bootstrap for ``parallel(...)`` submissions.

Runs once on the batch allocation's head node, before the supervisor. Its
responsibilities in Phase 3 are minimal:

1. Read ``$JOB_DIR/plan.json``.
2. Resolve the allocation's hostnames from ``SLURM_JOB_NODELIST`` (via
   ``scontrol show hostnames`` when available; an in-process expander
   handles the common cases in tests and scontrol-less environments).
3. Write a registry skeleton to ``$JOB_DIR/registry.json`` with every peer
   marked ``pending`` and pre-seeded with hostnames.

Placement resolution (``on_node`` / ``colocate_with``), replica
pinning, and hetjob multi-component parsing arrive in Phase 6 / 8.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess  # nosec B404 - used to invoke scontrol
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..runtime import _expand_nodelist
from .placement import resolve_placement_from_plan
from .plan import Plan, read_plan, write_plan
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
    *,
    per_component_hostnames: Optional[Dict[int, Tuple[str, ...]]] = None,
    pool_node_labels: Optional[Dict[str, Tuple[Optional[str], ...]]] = None,
    peer_pins: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> dict:
    """Build the initial registry dict — all peers ``pending``, nodes seeded.

    Every peer entry lists the hostnames of its pool. For hetjob submissions
    ``per_component_hostnames`` maps component index → hostname tuple so
    peers in different pools see different hostname sets. Replica peers get
    ``peer.replica_count`` entries in the registry list (one per replica
    index); singleton peers get exactly one.

    Per-replica node spread is round-robin by replica index across the pool's
    hostnames when no explicit pin applies — this keeps unpinned peers
    deterministic while pinned peers (via ``peer_pins``) land exactly where
    the Phase 8 placement resolver says.

    Args:
        plan: Parsed supervisor plan.
        hostnames: Flat hostname list — used for single-pool submissions and
            as the fallback when ``per_component_hostnames`` is missing a
            component.
        per_component_hostnames: Optional mapping of component index →
            hostname tuple. When provided, each peer is seeded with the
            hostnames of its component.
        pool_node_labels: Optional mapping of pool name → per-ordinal label
            tuple (same shape as :attr:`Pool.node_labels` but expanded to
            ``None`` for pools without labels). Drives each peer entry's
            ``node_label`` and the node registry entry's ``label`` field.
        peer_pins: Optional mapping of peer name → hostname pin tuple
            produced by :func:`slurm.parallel.placement.resolve_placement`.
            When a peer has an entry, the listed hostnames override the
            default round-robin assignment for that peer's replicas. Keys
            not in the mapping fall back to round-robin.
    """
    components = plan.effective_components()
    # Derive per-component hostname tuples. When the caller did not supply a
    # mapping (single-pool path or missing env vars), reuse the flat
    # hostnames list for every component so callers get sensible defaults.
    component_hosts: Dict[int, Tuple[str, ...]] = {}
    for comp in components:
        if (
            per_component_hostnames is not None
            and comp.index in per_component_hostnames
        ):
            component_hosts[comp.index] = tuple(per_component_hostnames[comp.index])
        else:
            component_hosts[comp.index] = tuple(hostnames)

    # Map pool name → component index for quick peer→component lookup.
    pool_to_component: Dict[str, int] = {c.pool: c.index for c in components}

    pool_node_labels = pool_node_labels or {}
    peer_pins = peer_pins or {}

    def _label_for(pool_name: str, hostname: str) -> Optional[str]:
        """Label for ``hostname`` within ``pool_name``, or ``None``."""
        hosts = component_hosts.get(pool_to_component.get(pool_name, -1), ())
        labels = pool_node_labels.get(pool_name)
        if not labels:
            return None
        try:
            idx = hosts.index(hostname)
        except ValueError:
            return None
        if idx >= len(labels):
            return None
        return labels[idx]

    peers_out: dict = {}
    for peer in plan.peers:
        comp_index = pool_to_component.get(peer.pool, 0)
        peer_hostnames = component_hosts.get(comp_index, ())
        pin = peer_pins.get(peer.name)
        entries = []
        for replica_index in range(peer.replica_count):
            if pin is not None and replica_index < len(pin):
                pinned = pin[replica_index]
            elif pin is not None and len(pin) == 1:
                # Singleton pin reused across all replicas — useful when a
                # replica set colocates with a singleton peer that owns one
                # host.
                pinned = pin[0]
            elif peer_hostnames:
                # Round-robin placement: replica i picks host i mod len(hosts).
                pinned = peer_hostnames[replica_index % len(peer_hostnames)]
            else:
                pinned = ""
            label = _label_for(peer.pool, pinned) if pinned else None
            entries.append(
                {
                    "name": peer.name,
                    "pool": peer.pool,
                    "replica_index": replica_index,
                    "replica_count": peer.replica_count,
                    "hostname": pinned,
                    "hostnames": list(peer_hostnames),
                    "node_label": label,
                    "step_id": None,
                    "ports": {},
                    "metadata": {},
                    "state": STATE_PENDING,
                    "restart_count": 0,
                    "outcome": None,
                    "final_exit_code": None,
                    "message": None,
                    "component_index": comp_index,
                }
            )
        peers_out[peer.name] = entries

    nodes_out: dict = {}
    # Track which peers each node carries so service discovery can enumerate
    # them once the registry matures.
    peers_by_component: Dict[int, List[str]] = {}
    for peer in plan.peers:
        comp_index = pool_to_component.get(peer.pool, 0)
        peers_by_component.setdefault(comp_index, []).append(peer.name)

    # Global ordinal counter across all components so every hostname key stays
    # unique even when two components share the same hostname (rare but
    # possible with overlapping partitions).
    global_ordinal = 0
    for comp in components:
        hosts = component_hosts.get(comp.index, ())
        comp_peers = peers_by_component.get(comp.index, [])
        labels = pool_node_labels.get(comp.pool, ())
        for pool_ordinal, hostname in enumerate(hosts):
            key = hostname or f"_unresolved_{global_ordinal}"
            label = labels[pool_ordinal] if pool_ordinal < len(labels) else None
            nodes_out[key] = {
                "hostname": hostname,
                "pool": comp.pool,
                # Pool-local ordinal — Phase 8 promotes this to a per-pool
                # index so ``NodeGroup[int]`` semantics work. The previous
                # "global ordinal across all components" scheme is
                # reconstructible by walking components in order.
                "ordinal": pool_ordinal,
                "label": label,
                "peers": list(comp_peers),
                "component_index": comp.index,
            }
            global_ordinal += 1

    return {"peers": peers_out, "nodes": nodes_out}


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

    hostnames, per_component_hostnames = _resolve_component_hostnames(plan)

    # Run placement resolution — walks ``on_node`` / ``on_nodes`` /
    # ``colocate_with`` chains and maps each pinned peer to concrete
    # hostnames. Bootstrap writes those pins back into plan.json so the
    # supervisor can emit ``--nodelist=<host>`` onto each srun step.
    pool_hostnames = _pool_hostnames_for_placement(
        plan, hostnames, per_component_hostnames
    )
    placement = resolve_placement_from_plan(plan, pool_hostnames)
    pool_node_labels: Dict[str, Tuple[Optional[str], ...]] = {}
    for comp in plan.effective_components():
        if comp.node_labels is not None:
            pool_node_labels[comp.pool] = tuple(comp.node_labels)

    # Write nodelist back onto plan peers and rewrite plan.json so the
    # supervisor sees the pinned hosts on every peer entry.
    updated_peers = []
    for peer in plan.peers:
        pins = placement.for_peer(peer.name)
        updated_peers.append(
            type(peer)(
                name=peer.name,
                pool=peer.pool,
                leader=peer.leader,
                on_failure=peer.on_failure,
                max_restarts=peer.max_restarts,
                srun_command_line=peer.srun_command_line,
                callback=peer.callback,
                replica_count=peer.replica_count,
                component_index=peer.component_index,
                nodelist=pins,
                on_node=peer.on_node,
                on_nodes=peer.on_nodes,
                colocate_with=peer.colocate_with,
                ports=dict(peer.ports),
            )
        )
    updated_plan = Plan(
        peers=updated_peers,
        grace_period_seconds=plan.grace_period_seconds,
        pool_names=plan.pool_names,
        pre_submission_id=plan.pre_submission_id,
        schema_version=plan.schema_version,
        components=plan.components,
    )
    write_plan(plan_path, updated_plan)
    logger.info(
        "Placement resolved for %d peer(s) (%d pinned)",
        len(plan.peers),
        len(placement.pinned_peers()),
    )

    skeleton = build_registry_skeleton(
        updated_plan,
        hostnames,
        per_component_hostnames=per_component_hostnames,
        pool_node_labels=pool_node_labels,
        peer_pins=placement.as_dict(),
    )
    write_registry(job_dir / "registry.json", skeleton)
    logger.info("Registry skeleton written to %s", job_dir / "registry.json")

    return 0


def _pool_hostnames_for_placement(
    plan: Plan,
    flat_hostnames: Tuple[str, ...],
    per_component_hostnames: Optional[Dict[int, Tuple[str, ...]]],
) -> Dict[str, Tuple[str, ...]]:
    """Build the ``{pool_name: hostnames}`` mapping the placement resolver wants.

    Single-pool submissions use the flat hostname list; hetjob submissions
    route each component's per-pool hostname slice.
    """
    mapping: Dict[str, Tuple[str, ...]] = {}
    for comp in plan.effective_components():
        if (
            per_component_hostnames is not None
            and comp.index in per_component_hostnames
        ):
            mapping[comp.pool] = tuple(per_component_hostnames[comp.index])
        else:
            mapping[comp.pool] = tuple(flat_hostnames)
    return mapping


def _resolve_component_hostnames(
    plan: Plan,
) -> Tuple[Tuple[str, ...], Optional[Dict[int, Tuple[str, ...]]]]:
    """Resolve hostnames per hetjob component.

    Slurm exposes per-component nodelists via
    ``SLURM_JOB_NODELIST_HET_GROUP_<N>`` — one env var per component. When any
    of these are set we treat the submission as a hetjob and slice hostnames
    per component. Otherwise we fall back to the flat ``SLURM_JOB_NODELIST``.

    Returns:
        ``(flat_hostnames, per_component_hostnames)``. ``per_component_hostnames``
        is ``None`` in the single-pool (non-hetjob) case so callers can branch
        on it without probing the dict.
    """
    components = plan.effective_components()

    per_component: Dict[int, Tuple[str, ...]] = {}
    has_any_component_env = False
    for comp in components:
        env_key = f"SLURM_JOB_NODELIST_HET_GROUP_{comp.index}"
        raw = os.environ.get(env_key, "")
        if raw:
            has_any_component_env = True
            per_component[comp.index] = _resolve_hostnames(raw)
            logger.info(
                "Bootstrap resolved %d hostname(s) for component %d (pool %r)",
                len(per_component[comp.index]),
                comp.index,
                comp.pool,
            )

    flat_nodelist = (
        os.environ.get("SLURM_JOB_NODELIST") or os.environ.get("SLURM_NODELIST") or ""
    )
    flat_hostnames = _resolve_hostnames(flat_nodelist) if flat_nodelist else ()
    if not flat_hostnames and per_component:
        # Single-pool consumers still want a non-empty flat list. Concatenate
        # the per-component hostnames preserving component order.
        combined: List[str] = []
        for comp in components:
            combined.extend(per_component.get(comp.index, ()))
        flat_hostnames = tuple(combined)

    logger.info(
        "Bootstrap resolved %d flat hostname(s) from nodelist %r",
        len(flat_hostnames),
        flat_nodelist,
    )

    if has_any_component_env:
        # Backfill components that didn't get their own env var so
        # build_registry_skeleton never sees a missing component index.
        for comp in components:
            per_component.setdefault(comp.index, ())
        return flat_hostnames, per_component

    # No hetjob env vars — Phase 3 single-pool semantics.
    return flat_hostnames, None


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
