"""Peer registry — the runtime directory of who landed where.

The registry lives at ``$JOB_DIR/registry.json``. Bootstrap writes the
skeleton before any peer runs: pool layout and the ``pending`` entry for
every peer. Peers carry ``hostname=""`` and ``state="pending"`` until
their runner publishes the real hostname and ``SLURM_STEP_ID`` via
:func:`update_peer_hostinfo` at startup. The supervisor then updates peer
entries as peers start / fail / finish. User-facing APIs like
``ctx.peers`` read the same file.

Peer entries own runtime state: hostname, step id, outcomes, and user
metadata.

Concurrency model: atomic writes via ``tmp + os.replace`` so readers
always see a consistent file, plus an ``fcntl.flock`` on a sibling
``<registry>.lock`` held across every read-modify-write so concurrent
announces / supervisor state changes never overwrite each other's
fields. Readers are lock-free — the atomic rename guarantees they see
either the full old file or the full new one.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Optional

if TYPE_CHECKING:
    from .peer_info import PeerGroup

logger = logging.getLogger("slurm.parallel.registry")


# Cross-process exclusion for read-modify-write cycles. Writers hold an
# exclusive advisory lock on ``<registry>.lock`` across the read → mutate →
# rename sequence, so concurrent announces / supervisor state changes never
# overwrite each other's fields. Readers remain
# lock-free — ``os.replace`` makes the swap atomic from their point of view.
#
# We use ``fcntl.flock`` which is supported on Linux and macOS (Slurm is
# Linux-only in production, macOS covers development). Windows is not a
# supported SDK platform; the context manager degrades to a no-op there so
# imports do not break.
def _load_fcntl() -> tuple[Any, bool]:
    """Import ``fcntl`` lazily and return it typed as :class:`Any`.

    Returning through a function — rather than binding a module-level name
    inside a ``try``/``except`` — keeps the static type as ``Any`` so
    downstream attribute access (``flock``, ``LOCK_EX`` etc.) doesn't get
    narrowed to a ``None`` branch on platforms without fcntl.
    """
    try:
        import fcntl as module  # noqa: F401

        return module, True
    except ImportError:  # pragma: no cover - non-POSIX platforms
        return None, False


_fcntl, _HAS_FCNTL = _load_fcntl()


@contextlib.contextmanager
def _with_registry_lock(path: "str | Path") -> Iterator[None]:
    """Hold an exclusive advisory lock on ``<path>.lock`` for this block.

    Used around every read-modify-write sequence on the registry so that
    ``announce_peer_metadata``, ``update_peer_entry``,
    and any future mutators serialize against each other across threads
    *and* processes. Without the lock two writers could both read the same
    state, each mutate their own copy, each rename-write — and the later
    writer would silently overwrite the earlier writer's fields.

    The lockfile is a sibling of the registry so it inherits the same
    parent directory permissions and lives on the same filesystem
    (``flock`` needs both locker and target on a local or properly-locking
    shared filesystem; NFSv4 with flock support is fine).

    Readers do not acquire this lock: the ``os.replace`` atomic swap in
    :func:`write_registry` already guarantees readers see either the full
    old file or the full new file, never a torn in-between state.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_suffix(target.suffix + ".lock")

    if not _HAS_FCNTL:  # pragma: no cover - non-POSIX platforms
        # Lock is best-effort; callers on unsupported platforms get
        # serialization guaranteed only within the same process.
        yield
        return

    # ``os.open`` + O_CREAT gives us a handle even if the file doesn't yet
    # exist; O_RDWR keeps flock happy on all POSIX variants.
    fd = os.open(
        str(lock_path),
        os.O_RDWR | os.O_CREAT,
        0o644,
    )
    try:
        _fcntl.flock(fd, _fcntl.LOCK_EX)
        try:
            yield
        finally:
            _fcntl.flock(fd, _fcntl.LOCK_UN)
    finally:
        os.close(fd)


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
        "step_id",
        "state",
        "metadata",
    }
)


# Registry entry state values used across bootstrap / supervisor / runner.
STATE_PENDING = "pending"
STATE_READY = "ready"
STATE_FAILED = "failed"
STATE_SHUTDOWN_BY_LEADER = "shutdown_by_leader"
STATE_SUCCESS = "success"

# Outcome values recorded by the supervisor once a peer has reached a terminal
# state. These feed ``ParallelJob.peer_outcomes()`` — see
# :class:`slurm.parallel_job.PeerOutcome` for user-facing semantics.
OUTCOME_SUCCESS = "success"
OUTCOME_CONTINUE_ON_FAILURE = "continue_on_failure"
OUTCOME_FATAL = "fatal"
OUTCOME_SHUTDOWN_BY_LEADER = "shutdown_by_leader"
OUTCOME_NOT_STARTED = "not_started"


@dataclass
class PeerRegistryEntry:
    """One peer (or replica) as seen by the runtime registry.

    ``metadata`` starts empty and is filled by peer announcements; ``state``
    transitions are driven by the supervisor.
    """

    name: str
    pool: str
    replica_index: int = 0
    replica_count: int = 1
    hostname: str = ""
    hostnames: List[str] = field(default_factory=list)
    step_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    state: str = STATE_PENDING
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
            "step_id": self.step_id,
            "metadata": dict(self.metadata),
            "state": self.state,
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
            step_id=data.get("step_id"),
            metadata=dict(data.get("metadata", {})),
            state=data.get("state", STATE_PENDING),
            outcome=data.get("outcome"),
            final_exit_code=int(exit_code_raw) if exit_code_raw is not None else None,
            message=data.get("message"),
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

    Caller gets a dict with a ``peers`` section (name → list[entry dict]).
    This low-level dict view is enough for the bootstrap to build and the
    supervisor to touch; user-facing code wraps it in
    :class:`~slurm.parallel.peer_info.PeerGroup` views.
    """
    return json.loads(Path(path).read_text())


def peers_from_registry(registry: dict) -> Dict[str, List[PeerRegistryEntry]]:
    """Materialize the ``peers`` section of a registry dict into dataclasses."""
    return {
        name: [PeerRegistryEntry.from_dict(entry) for entry in entries]
        for name, entries in registry.get("peers", {}).items()
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


@contextlib.contextmanager
def _locked_peer_entry(
    path: "str | Path", peer_name: str, replica_index: int
) -> "Iterator[tuple[dict, list, dict]]":
    """Lock the registry and yield ``(registry, entries, entry)`` for one peer.

    Holds the cross-process lock across read → mutate → write. ``entry`` is a
    *copy* of ``entries[replica_index]``; the caller mutates it (and may touch
    ``registry`` directly), and on a clean exit the copy is stored back and the
    whole registry is rewritten atomically.

    Raises:
        KeyError: If ``peer_name`` is not in the registry.
        IndexError: If ``replica_index`` is out of range.
    """
    with _with_registry_lock(path):
        registry = read_registry(path)
        peers = registry.setdefault("peers", {})
        entries = peers.get(peer_name)
        if not entries:
            raise KeyError(
                f"Peer {peer_name!r} not found in registry (known: "
                f"{sorted(peers.keys())})"
            )
        if replica_index < 0 or replica_index >= len(entries):
            raise IndexError(
                f"replica_index {replica_index} out of range for peer "
                f"{peer_name!r} (have {len(entries)} entries)"
            )
        entry = dict(entries[replica_index])
        yield registry, entries, entry
        entries[replica_index] = entry
        write_registry(path, registry)


def update_peer_hostinfo(
    path: "str | Path",
    peer_name: str,
    replica_index: int,
    *,
    hostname: str,
    step_id: Optional[str] = None,
) -> dict:
    """Record the peer's runtime-discovered hostname and Slurm step id.

    Bootstrap seeds each peer entry with ``hostname=""`` (pending) — it
    cannot know which node Slurm will pick until the ``srun`` step actually
    launches. The runner calls this helper at startup so service discovery
    (``ctx.peers[...].first.hostname``) returns the *actual* host rather
    than bootstrap's speculation.

    ``step_id`` comes from ``SLURM_STEP_ID`` when the runner is launched
    under srun. It is ``None`` in local-mode (no srun involved) and for
    tests.
    """
    with _with_registry_lock(path):
        registry = read_registry(path)
        peers = registry.setdefault("peers", {})
        entries = peers.get(peer_name)
        if not entries:
            raise KeyError(
                f"Peer {peer_name!r} not found in registry (known: "
                f"{sorted(peers.keys())})"
            )
        if replica_index < 0 or replica_index >= len(entries):
            raise IndexError(
                f"replica_index {replica_index} out of range for peer "
                f"{peer_name!r} (have {len(entries)} entries)"
            )
        entry = dict(entries[replica_index])
        # Skip the write if nothing actually changed — saves a rewrite
        # when a peer's hostname already matches the runner's.
        changed = False
        if entry.get("hostname") != hostname:
            entry["hostname"] = hostname
            changed = True
        if step_id is not None and entry.get("step_id") != step_id:
            entry["step_id"] = step_id
            changed = True
        if changed:
            entries[replica_index] = entry
            write_registry(path, registry)
    return registry


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

    with _locked_peer_entry(path, peer_name, replica_index) as (registry, _, entry):
        metadata = dict(entry.get("metadata") or {})
        metadata.update(fields)
        entry["metadata"] = metadata
        if ready:
            entry["state"] = STATE_READY
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
    with _locked_peer_entry(path, peer_name, replica_index) as (registry, _, entry):
        entry.update(changes)
    return registry


__all__ = [
    "PeerRegistryEntry",
    "write_registry",
    "read_registry",
    "peers_from_registry",
    "update_peer_entry",
    "update_peer_hostinfo",
    "announce_peer_metadata",
    "load_peer_groups",
    "_RESERVED_ANNOUNCE_KEYS",
    "STATE_PENDING",
    "STATE_READY",
    "STATE_FAILED",
    "STATE_SHUTDOWN_BY_LEADER",
    "STATE_SUCCESS",
    "OUTCOME_SUCCESS",
    "OUTCOME_CONTINUE_ON_FAILURE",
    "OUTCOME_FATAL",
    "OUTCOME_SHUTDOWN_BY_LEADER",
    "OUTCOME_NOT_STARTED",
]
