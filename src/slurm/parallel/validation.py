"""Submission-time validation for ``_ParallelSpec``.

The validator aggregates every problem it can find before raising, so the
user sees a single ``TopologyError`` listing all issues. This is deliberate:
a 24-hour RL job must not fail 30 seconds into ``sbatch`` because of a
typo'd pool name — all mistakes should surface before submission.

Design choices driving the checks here:

- Pools are placement groups: same pool = co-located; different pools =
  potentially different node types.
- Names are the peers' runtime identity (``ctx.peers["<name>"]``,
  ``job["<name>"]``). They must be unique.
- A peer can declare capacity via its underlying ``@task`` decorator's
  sbatch options; the validator sums per-node demand and compares to
  ``Pool.*_per_node`` when both are known.
- ``on_failure="continue"`` on a ``leader=True`` peer would leave the group
  without a defined termination condition → explicitly rejected.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess  # nosec B404 - used to probe optional tooling (nvidia-smi)
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional

from ..errors import TopologyError
from .registry import _RESERVED_ANNOUNCE_KEYS
from .types import Peer, Pool, _ParallelSpec

__all__ = ["validate_spec", "validate_local_capacity", "_RESERVED_ANNOUNCE_KEYS"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ResourceVector:
    """Internal CPU/GPU/memory vector used by topology and local checks."""

    cpus: Optional[int] = None
    gpus: Optional[int] = None
    mem_mib: Optional[int] = None

    @classmethod
    def from_sbatch_options(cls, sbatch: dict[str, Any]) -> "_ResourceVector":
        """Return the per-task resource claims declared on a task."""
        cpus = sbatch.get("cpus_per_task")
        gpus = sbatch.get("gpus_per_task")
        return cls(
            cpus=int(cpus) if cpus is not None else None,
            gpus=int(gpus) if gpus is not None else None,
            mem_mib=_parse_mem_to_mib(sbatch.get("mem")),
        )

    @classmethod
    def from_pool(cls, pool: Pool) -> "_ResourceVector":
        """Return the per-node capacity declared on a pool."""
        return cls(
            cpus=int(pool.cpus_per_node) if pool.cpus_per_node is not None else None,
            gpus=int(pool.gpus_per_node) if pool.gpus_per_node is not None else None,
            mem_mib=_parse_mem_to_mib(pool.mem_per_node),
        )

    def scaled(self, factor: int) -> "_ResourceVector":
        """Scale every known axis by ``factor``."""
        return _ResourceVector(
            cpus=self.cpus * factor if self.cpus is not None else None,
            gpus=self.gpus * factor if self.gpus is not None else None,
            mem_mib=self.mem_mib * factor if self.mem_mib is not None else None,
        )

    def add(self, other: "_ResourceVector") -> "_ResourceVector":
        """Add two vectors, preserving ``None`` for wholly-unknown axes."""
        return _ResourceVector(
            cpus=_sum_known_axis(self.cpus, other.cpus),
            gpus=_sum_known_axis(self.gpus, other.gpus),
            mem_mib=_sum_known_axis(self.mem_mib, other.mem_mib),
        )

    @property
    def mem_per_node(self) -> Optional[str]:
        """Return the memory axis in Slurm's ``mem_per_node`` string format."""
        if self.mem_mib is None:
            return None
        return f"{self.mem_mib}M"


def _sum_known_axis(lhs: Optional[int], rhs: Optional[int]) -> Optional[int]:
    """Add two resource values while preserving ``None`` as 'unknown'."""
    if lhs is None and rhs is None:
        return None
    return (lhs or 0) + (rhs or 0)


def validate_spec(spec: _ParallelSpec) -> None:
    """Validate a parallel spec; raise :class:`TopologyError` on any issue.

    Problems are aggregated: every check runs even if earlier ones failed,
    so the user sees the full set in one error.

    Args:
        spec: The normalized spec to validate.

    Raises:
        TopologyError: If any pool/peer/placement/resource/name constraint is
            violated. The error's ``.problems`` attribute contains one string
            per problem.
    """
    problems: list[str] = []

    _check_unique_names(spec, problems)
    _check_pool_references(spec, problems)
    _check_announce_keys(spec, problems)
    _check_replica_args_length(spec, problems)
    _check_capacity(spec, problems)
    _check_outer_options(spec, problems)
    _check_packaging_inheritance(spec, problems)

    if problems:
        lines = [f"  • {p}" for p in problems]
        message = (
            f"{len(problems)} problem(s) with parallel() submission:\n"
            + "\n".join(lines)
        )
        raise TopologyError(message, problems=problems)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _check_unique_names(spec: _ParallelSpec, problems: list[str]) -> None:
    seen: dict[str, int] = {}
    for peer in spec.peers:
        name = peer.resolved_name
        seen[name] = seen.get(name, 0) + 1
    for name, count in seen.items():
        if count > 1:
            problems.append(
                f"Peer name {name!r} is used by {count} peers. Each peer "
                "needs a unique name — set name=... on the Peer(...) "
                "call or pass the peer as a keyword argument "
                f"(parallel({name}=...))."
            )


def _check_pool_references(spec: _ParallelSpec, problems: list[str]) -> None:
    pool_keys = set(spec.topology.pools.keys())
    for peer in spec.peers:
        # By this point spec building has resolved pool=None to the default.
        if peer.pool is None:
            problems.append(
                f"Peer {peer.resolved_name!r} has no pool and no default pool "
                f"is set on the topology. Available pools: {sorted(pool_keys)}."
            )
        elif peer.pool not in pool_keys:
            problems.append(
                f"Peer {peer.resolved_name!r} references unknown pool "
                f"{peer.pool!r}. Available pools: {sorted(pool_keys)}."
            )


def _check_announce_keys(spec: _ParallelSpec, problems: list[str]) -> None:
    for peer in spec.peers:
        if not peer.announce:
            continue
        conflicts = [k for k in peer.announce if k in _RESERVED_ANNOUNCE_KEYS]
        if conflicts:
            problems.append(
                f"Peer {peer.resolved_name!r} announces reserved key(s) "
                f"{conflicts!r}. Reserved keys are "
                f"{sorted(_RESERVED_ANNOUNCE_KEYS)}."
            )


def _check_replica_args_length(spec: _ParallelSpec, problems: list[str]) -> None:
    for peer in spec.peers:
        if not peer.is_replica_set:
            continue
        args = peer.args
        if args is None or callable(args):
            continue  # length unknown until runtime; skip static check
        if isinstance(args, range):
            if len(args) != peer.count:
                problems.append(
                    f"Peer {peer.resolved_name!r} has args=range(...) with "
                    f"len {len(args)} but count={peer.count}."
                )
        elif isinstance(args, Sequence):
            if len(args) != peer.count:
                problems.append(
                    f"Peer {peer.resolved_name!r} has args= with len "
                    f"{len(args)} but count={peer.count}."
                )


def _check_capacity(spec: _ParallelSpec, problems: list[str]) -> None:
    """Compare per-pool per-node demand against declared capacity.

    Demand is computed conservatively: every peer contributes
    ``per_task_claim × replicas_on_a_node`` to the node's budget. For
    replicas with ``tasks_per_node=K``, demand per node is
    ``per_task_claim × K``; otherwise the validator assumes the densest
    possible packing — ``per_task_claim × ceil(count / nodes)``.

    Unknown claims (missing ``cpus_per_task`` on the task, missing
    ``cpus_per_node`` on the pool) are silently skipped — the validator is
    advisory for partial declarations. Fully-declared topologies get
    fully-checked budgets.
    """
    pools = spec.topology.pools

    # Group peers by pool so we can sum across peers targeting the same pool.
    peers_by_pool: dict[str, list[Peer]] = {}
    for peer in spec.peers:
        if peer.pool is None or peer.pool not in pools:
            continue
        peers_by_pool.setdefault(peer.pool, []).append(peer)

    for pool_name, peers in peers_by_pool.items():
        pool = pools[pool_name]
        demand = _compute_per_node_demand(peers, pool.nodes)
        capacity = _ResourceVector.from_pool(pool)

        # Wrong-pool GPU peer check: a GPU-requesting peer pinned to a pool
        # with no GPUs is almost always a mistake (e.g. peer sent to a CPU
        # pool while another pool has the GPUs). Flag each peer individually
        # so the error points at the offender rather than the pool sum.
        pool_has_gpus = pool.gpus_per_node is not None and pool.gpus_per_node > 0
        if not pool_has_gpus:
            for peer in peers:
                opts = _task_sbatch_options(peer)
                gpt = opts.get("gpus_per_task")
                if gpt is not None and gpt > 0:
                    gpu_pools = [
                        name
                        for name, p in pools.items()
                        if p.gpus_per_node and p.gpus_per_node > 0
                    ]
                    hint = (
                        f" Pools with GPUs: {gpu_pools}."
                        if gpu_pools
                        else " No pool in this topology declares GPUs."
                    )
                    problems.append(
                        f"Peer {peer.resolved_name!r} requests "
                        f"gpus_per_task={gpt} but pool {pool_name!r} has "
                        f"gpus_per_node={pool.gpus_per_node!r}. The peer is "
                        f"pinned to the wrong pool.{hint}"
                    )

        if capacity.cpus is not None and demand.cpus is not None:
            if demand.cpus > capacity.cpus:
                problems.append(
                    f"Pool {pool_name!r} CPU overflow: demand "
                    f"{demand.cpus} per node > capacity "
                    f"{capacity.cpus} per node. Peers contributing: "
                    + _describe_peer_claims(peers, "cpus")
                )
        if capacity.gpus is not None and demand.gpus is not None:
            if demand.gpus > capacity.gpus:
                problems.append(
                    f"Pool {pool_name!r} GPU overflow: demand "
                    f"{demand.gpus} per node > capacity "
                    f"{capacity.gpus} per node. Peers contributing: "
                    + _describe_peer_claims(peers, "gpus")
                )
        if capacity.mem_mib is not None and demand.mem_mib is not None:
            if demand.mem_mib > capacity.mem_mib:
                problems.append(
                    f"Pool {pool_name!r} memory overflow: demand "
                    f"{demand.mem_mib} MiB per node > capacity "
                    f"{capacity.mem_mib} MiB per node "
                    f"({pool.mem_per_node!r}). Peers contributing: "
                    + _describe_peer_claims(peers, "mem")
                )


def _compute_per_node_demand(peers: list[Peer], pool_nodes: int) -> _ResourceVector:
    """Sum per-node task demand for a group of peers sharing a pool.

    Each axis (cpus / gpus / memory in MiB) sums ``per_task_claim ×
    replicas_on_a_node`` across the peer group. A ``None`` return for an
    axis means "no peer declared a claim on this axis" — the caller skips
    that axis so partially-declared topologies don't trip on spurious
    zeros.
    """
    demand = _ResourceVector()
    for peer in peers:
        per_node = _replicas_per_node(peer, pool_nodes)
        demand = demand.add(_peer_resource_claim(peer).scaled(per_node))

    return demand


def _peer_resource_claim(peer: Peer) -> _ResourceVector:
    """Return one peer replica's declared CPU/GPU/memory claims."""
    return _ResourceVector.from_sbatch_options(_task_sbatch_options(peer))


def _replicas_per_node(peer: Peer, pool_nodes: int) -> int:
    """Return the densest per-node packing the validator should assume."""
    if peer.tasks_per_node is not None:
        return peer.tasks_per_node
    if peer.count <= pool_nodes:
        return 1
    return -(-peer.count // pool_nodes)


def _task_sbatch_options(peer: Peer) -> dict:
    """Extract the sbatch options declared on a peer's task."""
    from ..task import BoundTask

    if isinstance(peer.task, BoundTask):
        return dict(peer.task.task.sbatch_options)
    return {}


def _describe_peer_claims(peers: list[Peer], resource: str) -> str:
    """Human-friendly summary of which peers contribute a resource claim."""
    key_map = {"cpus": "cpus_per_task", "gpus": "gpus_per_task", "mem": "mem"}
    key = key_map[resource]
    parts: list[str] = []
    for peer in peers:
        sbatch = _task_sbatch_options(peer)
        claim = sbatch.get(key)
        if claim is None:
            continue
        label = f"{peer.resolved_name!r} ({claim}×{peer.count})"
        parts.append(label)
    return ", ".join(parts) if parts else "<none declared>"


def _check_outer_options(spec: _ParallelSpec, problems: list[str]) -> None:
    if spec.grace_period_seconds < 0:
        problems.append(
            f"grace_period_seconds must be >= 0, got {spec.grace_period_seconds!r}."
        )


def _check_packaging_inheritance(spec: _ParallelSpec, problems: list[str]) -> None:
    """Reject ``packaging="inherit"`` when there is nothing to inherit from.

    The Phase 10 inheritance rule: a peer using ``packaging="inherit"``
    clones the leader's packaging; when no peer is ``leader=True`` the
    first-declared peer's packaging becomes the base. An ``"inherit"`` peer
    that *is* the first peer (and there is no leader) has no source, so the
    config is unresolvable at submission time.
    """
    from ..task import BoundTask

    has_leader = any(peer.leader for peer in spec.peers)
    first_peer_name = spec.peers[0].resolved_name if spec.peers else None

    for peer in spec.peers:
        if not isinstance(peer.task, BoundTask):
            continue
        pkg = peer.task.task.packaging or {}
        if pkg.get("type") != "inherit":
            continue
        if not has_leader and peer.resolved_name == first_peer_name:
            problems.append(
                f'Peer {peer.resolved_name!r} uses packaging="inherit" but '
                "is the first peer and no peer is marked leader=True — there "
                "is no source to inherit from. Mark a different peer as "
                "leader=True, move this peer later in the call, or set an "
                "explicit packaging= on it."
            )


# ---------------------------------------------------------------------------
# Local-backend capacity validation (Phase 12)
# ---------------------------------------------------------------------------


def _detect_local_memory_mib() -> Optional[int]:
    """Return the host's total RAM in MiB, or ``None`` when unknown.

    Prefers ``/proc/meminfo`` on Linux (no dependency), falls back to
    :mod:`psutil` when available, then to ``sysctl -n hw.memsize`` on macOS.
    """
    try:
        with open("/proc/meminfo", "r") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    return int(int(parts[1]) / 1024)
    except FileNotFoundError:
        pass
    except (OSError, ValueError):
        return None

    try:
        import importlib

        psutil = importlib.import_module("psutil")
        return int(psutil.virtual_memory().total / (1024 * 1024))
    except ImportError:
        pass
    except Exception:
        return None

    # macOS fallback via sysctl
    if shutil.which("sysctl"):
        try:
            result = subprocess.run(  # nosec B603,B607 - trusted sysctl
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return int(int(result.stdout.strip()) / (1024 * 1024))
        except (subprocess.TimeoutExpired, ValueError, OSError):
            return None

    return None


def _detect_local_gpus() -> Optional[int]:
    """Return the host's visible GPU count, or ``None`` when unknown.

    Checks ``CUDA_VISIBLE_DEVICES`` first (honours user masking), then
    ``nvidia-smi -L`` for the full count. Returns ``None`` — not ``0`` —
    when neither is informative so the capacity check can be skipped with
    a warning rather than falsely rejecting submissions on GPU-less boxes.
    """
    cuda_vis = os.environ.get("CUDA_VISIBLE_DEVICES")
    if cuda_vis is not None and cuda_vis.strip():
        items = [x for x in cuda_vis.split(",") if x.strip()]
        return len(items)

    if shutil.which("nvidia-smi"):
        try:
            result = subprocess.run(  # nosec B603,B607 - trusted nvidia-smi
                ["nvidia-smi", "-L"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
                return len(lines)
        except (subprocess.TimeoutExpired, OSError):
            pass
    return None


_MEM_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP]i?B?)?\s*$", re.IGNORECASE)


def _parse_mem_to_mib(value: Any) -> Optional[int]:
    """Parse a Slurm-style memory string (``"8G"``, ``"4096M"``) to MiB.

    Returns ``None`` when the value shape is unrecognised — callers should
    treat ``None`` as "unknown demand" and skip the check for that peer.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        return None
    match = _MEM_RE.match(value)
    if match is None:
        return None
    num = float(match.group(1))
    unit = (match.group(2) or "M").upper().rstrip("IB")
    multipliers = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024, "P": 1024**3}
    factor = multipliers.get(unit, 1)
    return int(num * factor)


def _aggregate_local_demand(spec: _ParallelSpec) -> _ResourceVector:
    """Sum per-task claims across every peer for local capacity checking.

    Local mode launches every peer on one host, so the total host-wide
    demand is ``sum(peer.cpus_per_task × peer.replicas)`` (and analogous
    for GPUs / memory). Unknown claims map to ``0`` — ``None`` return
    signals "nothing claimed; skip this axis entirely" for clean output.
    """
    demand = _ResourceVector()
    for peer in spec.peers:
        demand = demand.add(_peer_resource_claim(peer).scaled(max(1, peer.count)))
    return demand


def validate_local_capacity(spec: _ParallelSpec) -> None:
    """Raise :class:`TopologyError` when the local host cannot host the job.

    Only invoked when the submitting cluster's backend is ``local`` and
    Slurm is not available — a Slurm-backed parallel submission still
    targets real compute nodes and this check would be misleading.

    The check compares aggregate peer demand against::

        CPUs: os.cpu_count()
        RAM:  /proc/meminfo (Linux) / psutil / sysctl (macOS)
        GPUs: CUDA_VISIBLE_DEVICES or nvidia-smi -L

    Axes whose capacity can't be detected are skipped with a debug log —
    under-specified environments get a warning, not a hard failure.
    """
    demand = _aggregate_local_demand(spec)

    problems: list[str] = []

    cpus_available = os.cpu_count()
    if demand.cpus is not None and cpus_available:
        if demand.cpus > cpus_available:
            problems.append(
                f"local host has {cpus_available} CPU(s), parallel() "
                f"requires {demand.cpus} — scale down or use a real cluster."
            )

    mem_available = _detect_local_memory_mib()
    if demand.mem_mib is not None and mem_available is not None:
        if demand.mem_mib > mem_available:
            problems.append(
                f"local host has {mem_available} MiB of RAM, parallel() "
                f"requires {demand.mem_mib} MiB — scale down or use a real cluster."
            )
    elif demand.mem_mib is not None and mem_available is None:
        logger.debug(
            "Local-mode memory capacity unknown; skipping memory check "
            "(demand was %d MiB).",
            demand.mem_mib,
        )

    gpus_available = _detect_local_gpus()
    if demand.gpus is not None and gpus_available is not None:
        if demand.gpus > gpus_available:
            problems.append(
                f"local host has {gpus_available} GPU(s), parallel() "
                f"requires {demand.gpus} — scale down or use a real cluster."
            )
    elif demand.gpus is not None and gpus_available is None:
        logger.debug(
            "Local-mode GPU capacity unknown (no nvidia-smi / CUDA_VISIBLE_DEVICES); "
            "skipping GPU check (demand was %d).",
            demand.gpus,
        )

    if problems:
        lines = [f"  • {p}" for p in problems]
        raise TopologyError(
            f"{len(problems)} local-mode capacity problem(s):\n" + "\n".join(lines),
            problems=list(problems),
        )
