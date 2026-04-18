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
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


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

    Writes to a sibling ``.tmp`` file first, then ``os.replace`` swaps it in.
    ``os.replace`` is atomic on POSIX within the same filesystem, so readers
    never observe a half-written file.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(registry, indent=2))
    os.replace(tmp, target)


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
