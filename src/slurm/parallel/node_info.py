"""Read-only views over the node registry — :class:`NodeInfo` / :class:`NodeGroup`.

Phase 8 extends ``ctx.peers`` with a sibling ``ctx.nodes`` surface so user
code can discover nodes by label or ordinal and enumerate which peers share
each node. The types mirror :class:`~slurm.parallel.peer_info.PeerInfo` /
:class:`~slurm.parallel.peer_info.PeerGroup`:

- Snapshots, not live views — :meth:`NodeGroup.refresh` re-reads the
  registry on demand.
- Frozen dataclasses so accidental mutation can't corrupt shared state.
- Indexed access is flexible — integer ordinals resolve within the
  current peer's pool, string keys resolve by label across every pool.
- Cross-links are derived on read — :attr:`NodeInfo.peers` comes from the
  peer section's runtime placement, not from the raw ``nodes[*].peers``
  cache stored in ``registry.json`` for debugging / compatibility.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping, Optional, Tuple, cast

logger = logging.getLogger("slurm.parallel.node_info")


__all__ = ["NodeInfo", "NodeGroup"]


@dataclass(frozen=True)
class NodeInfo:
    """One node in the allocation as visible to user code.

    Attributes:
        hostname: The node's concrete hostname.
        pool: Pool the node belongs to.
        label: Pool-local label from :attr:`Pool.node_labels`, or ``None``.
        ordinal: 0-based position within the pool (stable across
            invocations given a stable allocation).
        peers: Names of peers that landed on this node. Derived from the
            registry's peer-placement section at load time; the raw
            ``nodes[*].peers`` field is treated as informational only.
        component_index: Hetjob component index — matches the pool's
            component in multi-pool submissions.
    """

    hostname: str
    pool: str
    label: Optional[str]
    ordinal: int
    peers: Tuple[str, ...]
    component_index: int

    @classmethod
    def from_entry(cls, data: Mapping[str, object]) -> "NodeInfo":
        """Build a :class:`NodeInfo` from a raw registry entry dict."""
        label_raw = data.get("label")
        ordinal_raw = data.get("ordinal", 0)
        component_raw = data.get("component_index", 0)
        peers_raw = data.get("peers") or ()
        return cls(
            hostname=str(data.get("hostname", "")),
            pool=str(data.get("pool", "")),
            label=str(label_raw) if label_raw is not None else None,
            ordinal=int(ordinal_raw) if isinstance(ordinal_raw, (int, str)) else 0,
            peers=tuple(str(p) for p in peers_raw)
            if isinstance(peers_raw, (list, tuple))
            else (),
            component_index=int(component_raw)
            if isinstance(component_raw, (int, str))
            else 0,
        )


@dataclass
class NodeGroup:
    """All nodes in the allocation, keyed by hostname.

    Obtained via :attr:`JobContext.nodes`. Supports label / ordinal lookup,
    hostname lookup, per-pool iteration, and :meth:`refresh` — the standard
    node-discovery operations §8.4 calls for.

    Integer subscripting (``group[0]``) resolves within the *current* peer's
    pool — ``current_pool`` is passed in by :class:`JobContext` so lookups
    stay unambiguous when a job has multiple pools. String subscripting
    (``group["head"]``) searches every pool for a matching label.
    """

    _nodes: Tuple[NodeInfo, ...]
    _path: Optional[Path] = None
    # Name of the pool the current peer runs in. Drives ordinal lookup
    # semantics (``group[0]`` picks from this pool). ``None`` outside a
    # parallel allocation — ordinal lookups then raise with a clear
    # message.
    current_pool: Optional[str] = None

    def __len__(self) -> int:
        return len(self._nodes)

    def __iter__(self) -> Iterator[NodeInfo]:
        return iter(self._nodes)

    def __getitem__(self, key: "str | int") -> NodeInfo:
        """Look up a node by label (any pool) or ordinal (current pool)."""
        if isinstance(key, int):
            if self.current_pool is None:
                raise IndexError(
                    "NodeGroup[int] requires a current peer pool; this "
                    "JobContext is not part of a parallel allocation. "
                    "Use NodeGroup.in_pool(name) or by_hostname(host) "
                    "instead."
                )
            pool_nodes = [n for n in self._nodes if n.pool == self.current_pool]
            pool_nodes.sort(key=lambda n: n.ordinal)
            if key < 0 or key >= len(pool_nodes):
                raise IndexError(
                    f"Node ordinal {key} out of range for pool "
                    f"{self.current_pool!r} ({len(pool_nodes)} node(s))."
                )
            return pool_nodes[key]
        if isinstance(key, str):
            for node in self._nodes:
                if node.label == key:
                    return node
            raise KeyError(
                f"No node with label {key!r} — available labels: "
                f"{sorted({n.label for n in self._nodes if n.label})}"
            )
        raise TypeError(
            f"NodeGroup indexing accepts str (label) or int (ordinal), "
            f"got {type(key).__name__}"
        )

    def by_hostname(self, host: str) -> Optional[NodeInfo]:
        """Return the node with matching hostname, or ``None``."""
        for node in self._nodes:
            if node.hostname == host:
                return node
        return None

    def in_pool(self, pool_name: str) -> Iterator[NodeInfo]:
        """Iterate over nodes in ``pool_name`` in ordinal order."""
        matching = [n for n in self._nodes if n.pool == pool_name]
        matching.sort(key=lambda n: n.ordinal)
        return iter(matching)

    def refresh(self) -> "NodeGroup":
        """Re-read the registry from disk, returning the refreshed group."""
        if self._path is None:
            return self
        refreshed = _load_nodes_from_path(self._path, self.current_pool)
        self._nodes = refreshed._nodes
        return self


def _load_nodes_from_path(path: Path, current_pool: Optional[str] = None) -> NodeGroup:
    """Private reader: parse ``registry.json`` into a :class:`NodeGroup`.

    Errors log at DEBUG and return an empty group so discovery code never
    crashes on a transient registry read.
    """
    try:
        from .registry import read_registry
    except ImportError:  # pragma: no cover - defensive for partial trees
        return NodeGroup(_nodes=(), _path=path, current_pool=current_pool)

    try:
        registry = read_registry(path)
    except FileNotFoundError:
        logger.debug("registry.json not found at %s — no nodes visible", path)
        return NodeGroup(_nodes=(), _path=path, current_pool=current_pool)
    except (OSError, ValueError) as exc:
        logger.debug("Could not read registry at %s: %s", path, exc)
        return NodeGroup(_nodes=(), _path=path, current_pool=current_pool)

    nodes_section = registry.get("nodes") or {}
    derive_node_peers = "peers" in registry
    peer_membership = (
        _derive_node_peer_membership(registry) if derive_node_peers else {}
    )
    infos = []
    for entry in nodes_section.values():
        materialized = dict(entry)
        hostname = str(materialized.get("hostname", ""))
        if derive_node_peers:
            materialized["peers"] = list(peer_membership.get(hostname, ()))
        infos.append(NodeInfo.from_entry(materialized))
    # Sort by (component_index, ordinal) so iteration and ordinal lookup
    # semantics stay stable regardless of JSON key order.
    infos = tuple(sorted(infos, key=lambda n: (n.component_index, n.ordinal)))
    return NodeGroup(_nodes=infos, _path=path, current_pool=current_pool)


def _derive_node_peer_membership(
    registry: Mapping[str, object],
) -> dict[str, tuple[str, ...]]:
    """Return ``hostname -> peer names`` derived from peer runtime placement.

    The registry keeps a ``nodes[*].peers`` list for compatibility and for
    debugging the raw file, but user-facing discovery should treat the peer
    section as authoritative because it is the source that runners and the
    supervisor update directly. Membership is therefore recomputed on load,
    preserving peer declaration order and de-duplicating repeated replicas of
    the same peer on one host.
    """
    peers_section = registry.get("peers")
    if not isinstance(peers_section, Mapping):
        return {}

    membership: dict[str, list[str]] = {}
    for declared_name, entries in peers_section.items():
        if not isinstance(entries, (list, tuple)):
            continue
        for raw_entry in entries:
            if not isinstance(raw_entry, Mapping):
                continue
            entry = cast(Mapping[str, object], raw_entry)
            hostname = str(entry.get("hostname") or "")
            if hostname == "":
                continue
            peer_name = str(entry.get("name", declared_name))
            host_members = membership.setdefault(hostname, [])
            if peer_name not in host_members:
                host_members.append(peer_name)
    return {hostname: tuple(names) for hostname, names in membership.items()}
