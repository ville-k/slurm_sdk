"""Runtime placement resolution for parallel allocations.

Phase 1 already *declares* placement intent on peers (:attr:`Peer.on_node`,
:attr:`Peer.on_nodes`, :attr:`Peer.colocate_with`) and validates it (labels
exist, ordinals are in range, cycles rejected). Phase 8 turns those
declarations into concrete hostnames: the bootstrap resolves each peer to
one or more hostnames from its pool's allocation, and the supervisor emits
``--nodelist=<host>`` onto the peer's ``srun`` so Slurm honours the pin.

The resolver is intentionally pure — it takes a validated spec plus a
mapping of per-pool hostnames and returns a :class:`PlacementMap`. Side
effects (registry writes, srun flag emission) live in the bootstrap and
supervisor respectively.

Placement algorithm
-------------------

For each peer, walk ``colocate_with`` chains in topological order (leaves
first) so a peer inherits the pin of whatever it colocates with:

- ``on_node="<label>"`` — resolve via the pool's ``node_labels`` mapping.
- ``on_node=<int>`` — pool's hostnames list, ordinal index.
- ``on_nodes=[...]`` — per-replica pinning (length must equal ``count``).
- ``colocate_with="<peer>"`` — inherit the target peer's resolved
  hostname(s). If the target itself has no pin, the resolver assigns it
  the pool's first unused hostname so both peers share a concrete pin.
- Otherwise — leave unpinned (Slurm places freely).

"First unused" means "not already assigned a pin by a previously-resolved
peer in topological order." This keeps the assignment deterministic
across invocations as long as ``spec.peers`` ordering is stable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from typing import TYPE_CHECKING

from ..errors import TopologyError
from .types import NodeRef, _ParallelSpec

if TYPE_CHECKING:
    from .plan import Plan

logger = logging.getLogger("slurm.parallel.placement")


__all__ = ["PlacementMap", "resolve_placement", "resolve_placement_from_plan"]


@dataclass(frozen=True)
class PlacementMap:
    """Resolved placement pins for every peer in a spec.

    Entries map each peer's resolved name to a tuple of hostnames. A
    singleton peer contributes a 1-element tuple; a replica peer contributes
    one entry per replica (length == count). Peers that end up unpinned are
    omitted from the mapping entirely — callers should treat absence as
    "Slurm places freely."
    """

    _pins: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)

    def for_peer(self, name: str) -> Optional[Tuple[str, ...]]:
        """Return resolved hostnames for ``name`` or ``None`` if unpinned."""
        return self._pins.get(name)

    def is_pinned(self, name: str) -> bool:
        """Whether ``name`` has an explicit nodelist pin."""
        return name in self._pins

    def pinned_peers(self) -> Tuple[str, ...]:
        """Tuple of peer names that have a pin."""
        return tuple(self._pins.keys())

    def as_dict(self) -> Dict[str, Tuple[str, ...]]:
        """Return a copy of the underlying mapping as a plain dict."""
        return dict(self._pins)


@dataclass(frozen=True)
class _PlacementRequest:
    """Normalized placement intent for one peer.

    Both the submission-time spec (:class:`Peer`) and the runtime plan
    (:class:`PlanPeer`) carry the same five placement fields under different
    names. Projecting each into a ``_PlacementRequest`` lets a single resolver
    drive off one type instead of duplicating the algorithm per source.
    """

    name: str
    pool: str
    on_node: Optional[NodeRef]
    on_nodes: Optional[Tuple[NodeRef, ...]]
    colocate_with: Optional[str]
    count: int


def _resolve_requests(
    requests: Sequence[_PlacementRequest],
    pool_hostnames: Mapping[str, Tuple[str, ...]],
    pool_labels: Mapping[str, Optional[Tuple[str, ...]]],
) -> PlacementMap:
    """Resolve normalized placement requests to concrete hostnames.

    Walks ``colocate_with`` chains in declaration order (``_resolve``
    recursively pulls targets in first), assigning the pool's "first unused"
    host to any unpinned colocation anchor so colocated peers share a node.

    Args:
        requests: One :class:`_PlacementRequest` per peer, in declaration
            order. The resolver trusts the validator's invariants (no cycles,
            no cross-pool colocation, in-range node references).
        pool_hostnames: Pool name → hostnames allocated to it, in Slurm
            allocation order. Absent pools contribute no hostnames, leaving
            their peers unpinned (safe default for tests without a real
            allocation).
        pool_labels: Pool name → its ``node_labels`` (or ``None``), used to
            resolve string node references.

    Returns:
        A :class:`PlacementMap` with one entry per explicitly-pinned peer.

    Raises:
        TopologyError: Defensive — on a runtime cycle, cross-pool colocation,
            or ``on_nodes``/``count`` mismatch the validator should have
            already rejected.
    """
    by_name: Dict[str, _PlacementRequest] = {r.name: r for r in requests}
    pins: Dict[str, Tuple[str, ...]] = {}
    # Pool free-list: hostnames not yet claimed by any pinned peer. Drained
    # as we assign "first unused" nodes to unpinned colocate targets.
    free_lists: Dict[str, List[str]] = {
        r.pool: list(pool_hostnames.get(r.pool, ())) for r in requests
    }
    # Guard against runaway recursion if a malformed spec slips past the
    # validator. The cycle detector in validation.py catches real cycles at
    # submission time; this is defence-in-depth.
    resolving: set[str] = set()

    def _resolve(name: str) -> Optional[Tuple[str, ...]]:
        if name in pins:
            return pins[name]
        if name in resolving:
            raise TopologyError(
                f"colocate_with cycle detected at runtime involving peer "
                f"{name!r}. This should have been rejected at spec "
                "validation — please file a bug."
            )
        req = by_name.get(name)
        if req is None:
            return None

        resolving.add(name)
        try:
            resolved = _resolve_one(req)
        finally:
            resolving.discard(name)

        if resolved is not None:
            pins[name] = resolved
            free = free_lists.get(req.pool, [])
            for host in resolved:
                if host in free:
                    free.remove(host)
        return resolved

    def _resolve_one(req: _PlacementRequest) -> Optional[Tuple[str, ...]]:
        hostnames = pool_hostnames.get(req.pool, ())
        labels = pool_labels.get(req.pool)

        # Explicit pins — on_node (singleton) / on_nodes (replica set).
        if req.on_node is not None:
            return (_node_ref_to_hostname(req.on_node, labels, hostnames, req.name),)

        if req.on_nodes is not None:
            if len(req.on_nodes) != req.count:
                raise TopologyError(
                    f"Peer {req.name!r}: on_nodes length "
                    f"{len(req.on_nodes)} != count {req.count}. This should "
                    "have been rejected at spec validation."
                )
            return tuple(
                _node_ref_to_hostname(ref, labels, hostnames, req.name)
                for ref in req.on_nodes
            )

        # Colocation — inherit target's pin; if the target has none, assign
        # it the pool's first unused node so both peers end up on the same
        # concrete host.
        if req.colocate_with is not None:
            target = by_name.get(req.colocate_with)
            if target is None:
                # Validator would normally have rejected this; if it ever
                # slips through, treat the peer as unpinned rather than
                # crashing the whole allocation.
                logger.warning(
                    "Peer %r colocate_with=%r — target missing; leaving unpinned",
                    req.name,
                    req.colocate_with,
                )
                return None
            if target.pool != req.pool:
                raise TopologyError(
                    f"Peer {req.name!r} colocate_with={req.colocate_with!r} "
                    f"but peers are in different pools ({req.pool!r} vs "
                    f"{target.pool!r}). This should have been rejected at "
                    "spec validation."
                )
            target_pin = _resolve(target.name)
            if target_pin is not None:
                # Replica sets colocating with a singleton inherit the single
                # host across all their replicas (Slurm handles per-task
                # placement on the shared node).
                if req.count > 1 and len(target_pin) == 1:
                    return tuple(target_pin[0] for _ in range(req.count))
                return target_pin

            # Target is itself unpinned — assign it the first free host in
            # its pool, then share.
            target_free = free_lists.get(target.pool, [])
            if not target_free:
                return None
            assigned_host = target_free[0]
            pins[target.name] = (assigned_host,)
            target_free.remove(assigned_host)
            if req.count > 1:
                return tuple(assigned_host for _ in range(req.count))
            return (assigned_host,)

        # Nothing declared — leave unpinned.
        return None

    for req in requests:
        _resolve(req.name)

    return PlacementMap(_pins=dict(pins))


def _node_ref_to_hostname(
    ref: object,
    labels: Optional[Tuple[str, ...]],
    hostnames: Tuple[str, ...],
    peer_name: str,
) -> str:
    """Resolve a single ``NodeRef`` (label or ordinal) to a hostname.

    Trusts validation for bounds/label-exists checks but still guards against
    missing hostnames (empty allocation in tests) by raising a clear error.
    """
    if isinstance(ref, int):
        if ref < 0 or ref >= len(hostnames):
            # In a well-formed allocation the validator's ordinal bounds
            # check plus Pool.nodes guarantees this never triggers. If it
            # does, the runtime allocation shrank below the declared pool
            # size — surface the mismatch to the caller.
            raise TopologyError(
                f"Peer {peer_name!r}: cannot resolve on_node={ref} — pool "
                f"has {len(hostnames)} hostname(s) available."
            )
        return hostnames[ref]
    if isinstance(ref, str):
        if labels is None:
            raise TopologyError(
                f"Peer {peer_name!r}: on_node={ref!r} references a label but "
                "the pool declares no node_labels. This should have been "
                "rejected at spec validation."
            )
        try:
            idx = labels.index(ref)
        except ValueError as err:
            raise TopologyError(
                f"Peer {peer_name!r}: unknown label {ref!r} — pool labels "
                f"are {list(labels)}."
            ) from err
        if idx >= len(hostnames):
            raise TopologyError(
                f"Peer {peer_name!r}: label {ref!r} maps to ordinal {idx} "
                f"but pool has {len(hostnames)} hostname(s) allocated."
            )
        return hostnames[idx]
    raise TopologyError(
        f"Peer {peer_name!r}: invalid node reference {ref!r} — must be a "
        "string label or integer ordinal."
    )


def resolve_placement(
    spec: _ParallelSpec,
    pool_hostnames: Mapping[str, Tuple[str, ...]],
) -> PlacementMap:
    """Resolve every peer's placement intent to concrete hostnames.

    Args:
        spec: The validated :class:`_ParallelSpec`. Validation must have
            already rejected cycles, cross-pool colocation, and out-of-range
            node references — the resolver trusts those invariants.
        pool_hostnames: Mapping of pool name → tuple of hostnames allocated to
            that pool, in Slurm's allocation order.

    Returns:
        A :class:`PlacementMap` with one entry per explicitly-pinned peer
        (directly via ``on_node`` / ``on_nodes``, or transitively via
        ``colocate_with``).
    """
    pool_labels = {name: pool.node_labels for name, pool in spec.topology.pools.items()}
    requests = [
        _PlacementRequest(
            name=p.resolved_name,
            pool=p.pool or "",
            on_node=p.on_node,
            on_nodes=p.on_nodes,
            colocate_with=p.colocate_with,
            count=p.count,
        )
        for p in spec.peers
    ]
    return _resolve_requests(requests, pool_hostnames, pool_labels)


def resolve_placement_from_plan(
    plan: "Plan",
    pool_hostnames: Mapping[str, Tuple[str, ...]],
) -> PlacementMap:
    """Resolve placement from a :class:`Plan` without needing ``_ParallelSpec``.

    The bootstrap only has ``plan.json`` at runtime — the full
    :class:`_ParallelSpec` is a submission-time artifact. The plan carries
    enough placement intent (``PlanPeer.on_node`` / ``on_nodes`` /
    ``colocate_with`` and ``PlanComponent.node_labels``) to feed the same
    :func:`_resolve_requests` core that :func:`resolve_placement` uses.
    """
    components = {c.pool: c for c in plan.effective_components()}
    pool_labels = {pool: comp.node_labels for pool, comp in components.items()}
    requests = [
        _PlacementRequest(
            name=p.name,
            pool=p.pool,
            on_node=p.on_node,
            on_nodes=tuple(p.on_nodes) if p.on_nodes is not None else None,
            colocate_with=p.colocate_with,
            count=int(p.replica_count),
        )
        for p in plan.peers
    ]
    return _resolve_requests(requests, pool_hostnames, pool_labels)
