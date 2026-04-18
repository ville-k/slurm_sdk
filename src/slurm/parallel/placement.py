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
from typing import Dict, List, Mapping, Optional, Tuple

from typing import TYPE_CHECKING

from ..errors import TopologyError
from .types import Peer, _ParallelSpec

if TYPE_CHECKING:
    from .plan import Plan, PlanPeer

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


def resolve_placement(
    spec: _ParallelSpec,
    pool_hostnames: Mapping[str, Tuple[str, ...]],
) -> PlacementMap:
    """Resolve every peer's placement intent to concrete hostnames.

    Args:
        spec: The validated :class:`_ParallelSpec`. Validation must have
            already rejected cycles, cross-pool colocation, and out-of-range
            node references — the resolver trusts those invariants.
        pool_hostnames: Mapping of pool name → tuple of hostnames allocated
            to that pool, in Slurm's allocation order. Pools not represented
            in the mapping contribute no hostnames — peers in those pools
            remain unpinned (safe default for tests without a real
            allocation).

    Returns:
        A :class:`PlacementMap` with one entry per explicitly-pinned peer
        (directly via ``on_node`` / ``on_nodes``, or transitively via
        ``colocate_with``).

    Raises:
        TopologyError: If a ``colocate_with`` target references a peer in a
            different pool (defensive — the validator should have caught
            this) or if replica-set ``on_nodes`` length disagrees with
            ``count`` (ditto).
    """
    peers_by_name: Dict[str, Peer] = {p.resolved_name: p for p in spec.peers}

    pins: Dict[str, Tuple[str, ...]] = {}
    # Pool free-list: hostnames not yet claimed by any pinned peer. Drained
    # as we assign "first unused" nodes to unpinned colocate targets.
    free_lists: Dict[str, List[str]] = {
        pool: list(pool_hostnames.get(pool, ())) for pool in spec.topology.pools
    }
    # Guard against runaway recursion if a malformed spec slips past the
    # validator. The cycle detector in validation.py catches real cycles at
    # submission time; this is defence-in-depth.
    resolving: set[str] = set()

    def _resolve(name: str) -> Optional[Tuple[str, ...]]:
        # Already resolved? Return cached result.
        if name in pins:
            return pins[name]
        if name in resolving:
            # Cycle — validator should have caught this. Break out rather
            # than loop forever.
            raise TopologyError(
                f"colocate_with cycle detected at runtime involving peer "
                f"{name!r}. This should have been rejected at spec "
                "validation — please file a bug."
            )
        peer = peers_by_name.get(name)
        if peer is None:
            return None

        resolving.add(name)
        try:
            resolved = _resolve_one(peer)
        finally:
            resolving.discard(name)

        if resolved is not None:
            pins[name] = resolved
            # Remove claimed hostnames from the pool's free list so future
            # "first unused" assignments don't collide.
            free = free_lists.get(peer.pool or "", [])
            for host in resolved:
                if host in free:
                    free.remove(host)
        return resolved

    def _resolve_one(peer: Peer) -> Optional[Tuple[str, ...]]:
        pool_name = peer.pool or ""
        pool = spec.topology.pools.get(pool_name) if pool_name else None
        hostnames = pool_hostnames.get(pool_name, ()) if pool_name else ()

        # Explicit pins — on_node (singleton) / on_nodes (replica set).
        if peer.on_node is not None:
            host = _node_ref_to_hostname(peer.on_node, pool, hostnames, peer)
            return (host,)

        if peer.on_nodes is not None:
            if len(peer.on_nodes) != peer.count:
                raise TopologyError(
                    f"Peer {peer.resolved_name!r}: on_nodes length "
                    f"{len(peer.on_nodes)} != count {peer.count}. This "
                    "should have been rejected at spec validation."
                )
            return tuple(
                _node_ref_to_hostname(ref, pool, hostnames, peer)
                for ref in peer.on_nodes
            )

        # Colocation — inherit target's pin; if the target has none, assign
        # it the pool's first unused node so both peers end up on the same
        # concrete host.
        if peer.colocate_with is not None:
            target = peers_by_name.get(peer.colocate_with)
            if target is None:
                # Validator would normally have rejected this; if it ever
                # slips through, treat the peer as unpinned rather than
                # crashing the whole allocation.
                logger.warning(
                    "Peer %r colocate_with=%r — target missing; leaving unpinned",
                    peer.resolved_name,
                    peer.colocate_with,
                )
                return None
            if target.pool != peer.pool:
                raise TopologyError(
                    f"Peer {peer.resolved_name!r} colocate_with="
                    f"{peer.colocate_with!r} but peers are in different "
                    f"pools ({peer.pool!r} vs {target.pool!r}). This "
                    "should have been rejected at spec validation."
                )
            target_pin = _resolve(target.resolved_name)
            if target_pin is not None:
                # Replica sets colocating with a singleton inherit the
                # single host across all their replicas (Slurm handles
                # per-task placement on the shared node). Replica sets
                # colocating with replica sets would need per-replica
                # broadcasting — not currently specified, and the validator
                # rejects this combo with a cross-replica colocate check.
                if peer.is_replica_set and len(target_pin) == 1:
                    return tuple(target_pin[0] for _ in range(peer.count))
                return target_pin

            # Target is itself unpinned — assign it the first free host in
            # its pool, then share.
            target_free = free_lists.get(target.pool or "", [])
            if not target_free:
                # No capacity left to assign — leave both unpinned. Slurm
                # will place them as it sees fit.
                return None
            assigned_host = target_free[0]
            pins[target.resolved_name] = (assigned_host,)
            target_free.remove(assigned_host)
            if peer.is_replica_set:
                return tuple(assigned_host for _ in range(peer.count))
            return (assigned_host,)

        # Nothing declared — leave unpinned.
        return None

    # Resolve in declaration order. ``_resolve`` recursively pulls
    # colocate targets in first, so this single pass is enough.
    for peer in spec.peers:
        _resolve(peer.resolved_name)

    return PlacementMap(_pins=dict(pins))


def _node_ref_to_hostname(
    ref: object,
    pool: "object | None",
    hostnames: Tuple[str, ...],
    peer: Peer,
) -> str:
    """Resolve a single ``NodeRef`` (label or ordinal) to a hostname.

    Trusts validation for bounds/label-exists checks but still guards
    against missing hostnames (empty allocation in tests) by raising a
    clear error.
    """
    if isinstance(ref, int):
        if ref < 0 or ref >= len(hostnames):
            # In a well-formed allocation the validator's ordinal bounds
            # check plus Pool.nodes guarantees this never triggers. If it
            # does, it's because the runtime allocation shrunk below the
            # declared pool size — surface the mismatch to the caller.
            raise TopologyError(
                f"Peer {peer.resolved_name!r}: cannot resolve on_node={ref} "
                f"— pool has {len(hostnames)} hostname(s) available."
            )
        return hostnames[ref]
    if isinstance(ref, str):
        from .types import Pool

        if not isinstance(pool, Pool) or pool.node_labels is None:
            raise TopologyError(
                f"Peer {peer.resolved_name!r}: on_node={ref!r} references a "
                "label but the pool declares no node_labels. This should "
                "have been rejected at spec validation."
            )
        try:
            idx = pool.node_labels.index(ref)
        except ValueError as err:
            raise TopologyError(
                f"Peer {peer.resolved_name!r}: unknown label {ref!r} — pool "
                f"labels are {list(pool.node_labels)}."
            ) from err
        if idx >= len(hostnames):
            raise TopologyError(
                f"Peer {peer.resolved_name!r}: label {ref!r} maps to "
                f"ordinal {idx} but pool has {len(hostnames)} hostname(s) "
                "allocated."
            )
        return hostnames[idx]
    raise TopologyError(
        f"Peer {peer.resolved_name!r}: invalid node reference {ref!r} "
        "— must be a string label or integer ordinal."
    )


def resolve_placement_from_plan(
    plan: "Plan",
    pool_hostnames: Mapping[str, Tuple[str, ...]],
) -> PlacementMap:
    """Resolve placement from a :class:`Plan` without needing ``_ParallelSpec``.

    The bootstrap only has ``plan.json`` at runtime — the full
    :class:`_ParallelSpec` is a submission-time artifact. Rather than
    re-serialize the spec, the plan carries enough placement intent
    (``PlanPeer.on_node`` / ``on_nodes`` / ``colocate_with`` and
    ``PlanComponent.node_labels``) for the bootstrap to run the same
    algorithm :func:`resolve_placement` uses.

    The implementation mirrors :func:`resolve_placement` but drives off the
    plan's dataclasses instead of ``Peer`` / ``Pool``.
    """
    # Build an index from plan data so the resolver algorithm below stays
    # close to resolve_placement() even though it operates on plan types.
    plan_peers_by_name = {p.name: p for p in plan.peers}
    components = {c.pool: c for c in plan.effective_components()}

    pins: Dict[str, Tuple[str, ...]] = {}
    free_lists: Dict[str, List[str]] = {
        pool: list(pool_hostnames.get(pool, ())) for pool in components
    }
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
        peer = plan_peers_by_name.get(name)
        if peer is None:
            return None
        resolving.add(name)
        try:
            resolved = _resolve_plan_peer(peer)
        finally:
            resolving.discard(name)
        if resolved is not None:
            pins[name] = resolved
            free = free_lists.get(peer.pool, [])
            for host in resolved:
                if host in free:
                    free.remove(host)
        return resolved

    def _resolve_plan_peer(peer: "PlanPeer") -> Optional[Tuple[str, ...]]:
        pool_name = peer.pool
        hostnames = pool_hostnames.get(pool_name, ())
        comp = components.get(pool_name)
        labels = comp.node_labels if comp is not None else None

        on_node_ref = peer.on_node
        on_nodes_refs = peer.on_nodes
        colocate_target = peer.colocate_with
        replica_count = int(peer.replica_count)
        peer_name = peer.name

        if on_node_ref is not None:
            host = _plan_node_ref_to_hostname(on_node_ref, labels, hostnames, peer_name)
            return (host,)
        if on_nodes_refs is not None:
            if len(on_nodes_refs) != replica_count:
                raise TopologyError(
                    f"Peer {peer_name!r}: on_nodes length "
                    f"{len(on_nodes_refs)} != replica_count {replica_count}."
                )
            return tuple(
                _plan_node_ref_to_hostname(r, labels, hostnames, peer_name)
                for r in on_nodes_refs
            )
        if colocate_target is not None:
            target = plan_peers_by_name.get(colocate_target)
            if target is None:
                return None
            if target.pool != pool_name:
                raise TopologyError(
                    f"Peer {peer_name!r} colocate_with={colocate_target!r} "
                    f"but peers are in different pools ({pool_name!r} vs "
                    f"{target.pool!r})."
                )
            target_pin = _resolve(target.name)
            if target_pin is not None:
                if replica_count > 1 and len(target_pin) == 1:
                    return tuple(target_pin[0] for _ in range(replica_count))
                return target_pin
            target_free = free_lists.get(target.pool, [])
            if not target_free:
                return None
            assigned_host = target_free[0]
            pins[target.name] = (assigned_host,)
            target_free.remove(assigned_host)
            if replica_count > 1:
                return tuple(assigned_host for _ in range(replica_count))
            return (assigned_host,)
        return None

    for peer in plan.peers:
        _resolve(peer.name)
    return PlacementMap(_pins=dict(pins))


def _plan_node_ref_to_hostname(
    ref: object,
    labels: Optional[Tuple[str, ...]],
    hostnames: Tuple[str, ...],
    peer_name: str,
) -> str:
    """Resolve a plan-level NodeRef (label or ordinal) to a hostname."""
    if isinstance(ref, int):
        if ref < 0 or ref >= len(hostnames):
            raise TopologyError(
                f"Peer {peer_name!r}: cannot resolve on_node={ref} — "
                f"pool has {len(hostnames)} hostname(s) available."
            )
        return hostnames[ref]
    if isinstance(ref, str):
        if labels is None:
            raise TopologyError(
                f"Peer {peer_name!r}: on_node={ref!r} references a label "
                "but the pool declares no node_labels."
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
