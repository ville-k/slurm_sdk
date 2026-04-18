"""Peer registry — the runtime directory of who landed where.

The registry lives at ``$JOB_DIR/registry.json``. The bootstrap writes the
skeleton (peers marked ``pending``, hostnames pre-resolved) before any peer
runs. The supervisor updates peer entries as peers start / fail / finish.
User-facing APIs like ``ctx.peers`` (Phase 7) read the same file.

Concurrency model: atomic writes via ``tmp + os.replace`` so readers always
see a consistent file. Readers are lock-free — they just re-read the JSON
each time. A full file rewrite is fine at the sizes expected (dozens of
peers at most); the complexity of incremental updates is not worth it.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

if TYPE_CHECKING:
    from .node_info import NodeGroup
    from .peer_info import PeerGroup

logger = logging.getLogger("slurm.parallel.registry")


# Keys reserved by the peer registry — neither ``Peer.announce={...}`` (static,
# topology-declaration time) nor ``ctx.announce(**kv)`` (runtime, inside peer
# code) may write these names. They are owned by the bootstrap / supervisor
# and represent the peer's identity and lifecycle state.
#
# ``state`` is reserved because only the supervisor flips it to terminal
# values (``failed``, ``shutdown_by_leader``, etc.). The one exception is
# the ``ctx.announce(ready=True)`` signal that user code emits to mark
# itself ready — see :meth:`JobContext.announce`.
_RESERVED_ANNOUNCE_KEYS = frozenset(
    {
        "name",
        "replica_index",
        "replica_count",
        "pool",
        "hostname",
        "hostnames",
        "node_label",
        "step_id",
        "ports",
        "state",
        "restart_count",
        "component_index",
        "metadata",
    }
)


# Registry entry state values used across bootstrap / supervisor / runner.
STATE_PENDING = "pending"
STATE_READY = "ready"
STATE_RUNNING = "running"
STATE_FAILED = "failed"
STATE_SHUTTING_DOWN = "shutting_down"
STATE_SHUTDOWN_BY_LEADER = "shutdown_by_leader"
STATE_SUCCESS = "success"

# Outcome values recorded by the supervisor once a peer has reached a terminal
# state. These feed ``ParallelJob.peer_outcomes()`` — see
# :class:`slurm.parallel_job.PeerOutcome` for user-facing semantics.
OUTCOME_SUCCESS = "success"
OUTCOME_CONTINUE_ON_FAILURE = "continue_on_failure"
OUTCOME_RESTARTED = "restarted"
OUTCOME_FATAL = "fatal"
OUTCOME_SHUTDOWN_BY_LEADER = "shutdown_by_leader"
OUTCOME_NOT_STARTED = "not_started"


@dataclass
class PeerRegistryEntry:
    """One peer (or replica) as seen by the runtime registry.

    Fields mirror the design doc so later phases extend without breaking
    the JSON shape: ``ports`` / ``metadata`` stay empty in Phase 3; ``state``
    transitions are driven by the supervisor.
    """

    name: str
    pool: str
    replica_index: int = 0
    replica_count: int = 1
    hostname: str = ""
    hostnames: List[str] = field(default_factory=list)
    node_label: Optional[str] = None
    step_id: Optional[str] = None
    ports: Dict[str, int] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    state: str = STATE_PENDING
    restart_count: int = 0
    component_index: int = 0
    outcome: Optional[str] = None
    final_exit_code: Optional[int] = None
    message: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pool": self.pool,
            "replica_index": self.replica_index,
            "replica_count": self.replica_count,
            "hostname": self.hostname,
            "hostnames": list(self.hostnames),
            "node_label": self.node_label,
            "step_id": self.step_id,
            "ports": dict(self.ports),
            "metadata": dict(self.metadata),
            "state": self.state,
            "restart_count": self.restart_count,
            "component_index": self.component_index,
            "outcome": self.outcome,
            "final_exit_code": self.final_exit_code,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PeerRegistryEntry":
        exit_code_raw = data.get("final_exit_code")
        return cls(
            name=data["name"],
            pool=data["pool"],
            replica_index=int(data.get("replica_index", 0)),
            replica_count=int(data.get("replica_count", 1)),
            hostname=data.get("hostname", ""),
            hostnames=list(data.get("hostnames", [])),
            node_label=data.get("node_label"),
            step_id=data.get("step_id"),
            ports=dict(data.get("ports", {})),
            metadata=dict(data.get("metadata", {})),
            state=data.get("state", STATE_PENDING),
            restart_count=int(data.get("restart_count", 0)),
            component_index=int(data.get("component_index", 0)),
            outcome=data.get("outcome"),
            final_exit_code=int(exit_code_raw) if exit_code_raw is not None else None,
            message=data.get("message"),
        )


@dataclass
class NodeRegistryEntry:
    """One node in the allocation as seen by the registry."""

    hostname: str
    pool: str
    ordinal: int
    label: Optional[str] = None
    peers: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "hostname": self.hostname,
            "pool": self.pool,
            "ordinal": self.ordinal,
            "label": self.label,
            "peers": list(self.peers),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NodeRegistryEntry":
        return cls(
            hostname=data["hostname"],
            pool=data["pool"],
            ordinal=int(data.get("ordinal", 0)),
            label=data.get("label"),
            peers=list(data.get("peers", [])),
        )


def write_registry(path: "str | Path", registry: dict) -> None:
    """Atomically replace ``registry.json`` at ``path`` with ``registry``.

    Writes to a per-writer tmp file first, then ``os.replace`` swaps it in.
    ``os.replace`` is atomic on POSIX within the same filesystem, so readers
    never observe a half-written file.

    The tmp path embeds the caller's PID and a monotonic counter so two
    concurrent writers never race on the same tmp name. Without the unique
    suffix, one writer's ``os.replace`` could observe the other's tmp file
    vanish mid-rename and raise ``FileNotFoundError`` — rare on a single
    process, real as soon as threads (or the bootstrap + supervisor) both
    try to write.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # ``os.getpid()`` + monotonic_ns gives a unique sibling name per writer
    # without needing a cross-process lock.
    suffix = f".tmp.{os.getpid()}.{time.monotonic_ns()}"
    tmp = target.with_suffix(target.suffix + suffix)
    try:
        tmp.write_text(json.dumps(registry, indent=2))
        os.replace(tmp, target)
    finally:
        # If we crashed between write_text and replace, clean up the
        # orphaned tmp file so ``registry.json.tmp.*`` doesn't accumulate.
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def read_registry(path: "str | Path") -> dict:
    """Read and parse the registry JSON at ``path``.

    Caller gets a dict with ``peers`` (name → list[entry dict]) and
    ``nodes`` (hostname → entry dict). This low-level dict view is enough
    for Phase 3 (bootstrap builds, supervisor barely touches). Phase 7
    wraps it in :class:`~slurm.parallel.registry.PeerGroup` /
    :class:`NodeGroup` views.
    """
    return json.loads(Path(path).read_text())


def peers_from_registry(registry: dict) -> Dict[str, List[PeerRegistryEntry]]:
    """Materialize the ``peers`` section of a registry dict into dataclasses."""
    return {
        name: [PeerRegistryEntry.from_dict(entry) for entry in entries]
        for name, entries in registry.get("peers", {}).items()
    }


def nodes_from_registry(registry: dict) -> Dict[str, NodeRegistryEntry]:
    """Materialize the ``nodes`` section of a registry dict into dataclasses."""
    return {
        key: NodeRegistryEntry.from_dict(entry)
        for key, entry in registry.get("nodes", {}).items()
    }


def load_peer_groups(
    registry_path: "str | Path",
) -> Mapping[str, "PeerGroup"]:
    """Load the peer section of ``registry.json`` into :class:`PeerGroup` views.

    Returns an empty :class:`MappingProxyType` if the file is missing or
    unreadable — service discovery must be robust against racing the
    bootstrap during the first few hundred milliseconds of a peer's life.

    Args:
        registry_path: Path to ``registry.json``.

    Returns:
        Read-only mapping from peer name to :class:`PeerGroup`. Callers
        iterate keys / values or look up a specific peer by name.
    """
    from .peer_info import _load_groups_from_path

    path = Path(registry_path)
    groups = dict(_load_groups_from_path(path))
    return MappingProxyType(groups)


def load_node_group(
    registry_path: "str | Path",
    *,
    current_pool: Optional[str] = None,
) -> "NodeGroup":
    """Load the node section of ``registry.json`` into a :class:`NodeGroup`.

    Returns an empty :class:`NodeGroup` if the file is missing or unreadable
    — node discovery must be robust against racing the bootstrap during the
    first few hundred milliseconds of a peer's life.

    Args:
        registry_path: Path to ``registry.json``.
        current_pool: Pool name of the current peer. Passed through so the
            returned group's ordinal lookup resolves against the right pool.

    Returns:
        :class:`NodeGroup` populated from the registry's ``nodes`` section.
    """
    from .node_info import _load_nodes_from_path

    return _load_nodes_from_path(Path(registry_path), current_pool)


def announce_peer_metadata(
    path: "str | Path",
    peer_name: str,
    replica_index: int,
    *,
    fields: Mapping[str, Any],
    ready: bool = False,
) -> dict:
    """Merge ``fields`` into a peer's metadata (and optionally set ``state=ready``).

    This is the atomic-write helper backing ``JobContext.announce()``. It
    diverges from :func:`update_peer_entry` in two ways:

    - It *merges* into ``metadata`` rather than overwriting it, so
      ``ctx.announce(foo=1)`` followed by ``ctx.announce(bar=2)`` leaves
      both keys in place.
    - It rejects any reserved key from appearing in ``fields``. The caller
      is expected to have stripped the ``ready`` signal out first; the
      reserved-key check here is a defence-in-depth guard.

    Args:
        path: Path to ``registry.json``.
        peer_name: Peer identifier.
        replica_index: Which replica's entry to update.
        fields: Keys to merge into the entry's ``metadata`` dict.
        ready: If ``True``, also flip the entry's ``state`` to
            :data:`STATE_READY`.

    Returns:
        The updated registry dict (for callers that want to skip a second read).

    Raises:
        ValueError: If ``fields`` contains a reserved key.
        KeyError: If ``peer_name`` is not in the registry.
        IndexError: If ``replica_index`` is out of range.
    """
    conflicts = [k for k in fields if k in _RESERVED_ANNOUNCE_KEYS]
    if conflicts:
        raise ValueError(
            f"Reserved key(s) {sorted(conflicts)!r} cannot be announced. "
            f"Reserved keys are {sorted(_RESERVED_ANNOUNCE_KEYS)}."
        )

    registry = read_registry(path)
    peers = registry.setdefault("peers", {})
    entries = peers.get(peer_name)
    if not entries:
        raise KeyError(
            f"Peer {peer_name!r} not found in registry (known: {sorted(peers.keys())})"
        )
    if replica_index < 0 or replica_index >= len(entries):
        raise IndexError(
            f"replica_index {replica_index} out of range for peer "
            f"{peer_name!r} (have {len(entries)} entries)"
        )
    entry = dict(entries[replica_index])
    metadata = dict(entry.get("metadata") or {})
    metadata.update(fields)
    entry["metadata"] = metadata
    if ready:
        entry["state"] = STATE_READY
    entries[replica_index] = entry
    write_registry(path, registry)
    return registry


def update_peer_ports(
    path: "str | Path",
    peer_name: str,
    replica_index: int,
    *,
    ports: Mapping[str, int],
) -> dict:
    """Atomically replace a peer's ``ports`` map.

    Split from :func:`update_peer_entry` because ``ports`` is one of the
    reserved keys that :func:`announce_peer_metadata` refuses to touch —
    only the runner / supervisor is allowed to rewrite it (initial binding
    at startup, mid-function :meth:`JobContext.reserve_port` calls).
    Replaces rather than merges so callers pass a full desired map and we
    never leave stale entries behind.
    """
    registry = read_registry(path)
    peers = registry.setdefault("peers", {})
    entries = peers.get(peer_name)
    if not entries:
        raise KeyError(
            f"Peer {peer_name!r} not found in registry (known: {sorted(peers.keys())})"
        )
    if replica_index < 0 or replica_index >= len(entries):
        raise IndexError(
            f"replica_index {replica_index} out of range for peer "
            f"{peer_name!r} (have {len(entries)} entries)"
        )
    entry = dict(entries[replica_index])
    # JSON keys must be strings; values must be ints — the writer clamps here.
    entry["ports"] = {str(k): int(v) for k, v in ports.items()}
    entries[replica_index] = entry
    write_registry(path, registry)
    return registry


def update_peer_entry(
    path: "str | Path",
    peer_name: str,
    replica_index: int = 0,
    **changes: Any,
) -> dict:
    """Atomically read ``registry.json``, update one peer entry, rewrite.

    The registry is the system-of-record for restart counts, outcomes, and
    final exit codes. When the supervisor changes any of these it must go
    through this helper so concurrent readers never see a torn write.

    Args:
        path: Path to ``registry.json``.
        peer_name: Top-level ``peers[peer_name]`` key.
        replica_index: Index of the replica entry to update. Defaults to 0
            (the only entry for single-peer declarations).
        **changes: Fields to merge into the matching entry's dict. Unknown
            keys pass through — callers are responsible for using names that
            match :class:`PeerRegistryEntry`.

    Returns:
        The full registry dict after the update, for callers that want to
        avoid a second read.
    """
    registry = read_registry(path)
    peers = registry.setdefault("peers", {})
    entries = peers.get(peer_name)
    if not entries:
        raise KeyError(
            f"Peer {peer_name!r} not found in registry (known: {sorted(peers.keys())})"
        )
    if replica_index < 0 or replica_index >= len(entries):
        raise IndexError(
            f"replica_index {replica_index} out of range for peer "
            f"{peer_name!r} (have {len(entries)} entries)"
        )
    entry = dict(entries[replica_index])
    entry.update(changes)
    entries[replica_index] = entry
    write_registry(path, registry)
    return registry


__all__ = [
    "PeerRegistryEntry",
    "NodeRegistryEntry",
    "write_registry",
    "read_registry",
    "peers_from_registry",
    "nodes_from_registry",
    "update_peer_entry",
    "update_peer_ports",
    "announce_peer_metadata",
    "load_peer_groups",
    "load_node_group",
    "_RESERVED_ANNOUNCE_KEYS",
    "STATE_PENDING",
    "STATE_READY",
    "STATE_RUNNING",
    "STATE_FAILED",
    "STATE_SHUTTING_DOWN",
    "STATE_SHUTDOWN_BY_LEADER",
    "STATE_SUCCESS",
    "OUTCOME_SUCCESS",
    "OUTCOME_CONTINUE_ON_FAILURE",
    "OUTCOME_RESTARTED",
    "OUTCOME_FATAL",
    "OUTCOME_SHUTDOWN_BY_LEADER",
    "OUTCOME_NOT_STARTED",
]
