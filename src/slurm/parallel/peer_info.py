"""Read-only views over the peer registry — :class:`PeerInfo` / :class:`PeerGroup`.

These types are what peer code sees through :attr:`JobContext.peers`. They
wrap one (or a list of) :class:`~slurm.parallel.registry.PeerRegistryEntry`
into an immutable, typed surface so user code can discover peers without
poking at the raw JSON.

Design notes:

- **Snapshots, not live views.** Construction copies the relevant registry
  entries into immutable containers. Call :meth:`PeerGroup.refresh` to pick
  up updates — readers never hold open handles, and never mutate the file.
- **Mappings are read-only.** ``ports`` and ``metadata`` ship as
  :class:`types.MappingProxyType` over private dicts, so callers cannot
  mutate shared state by accident.
- **``wait_all`` polls with exponential backoff.** Starts at 50ms, doubles
  up to 1s, emits a DEBUG log every few seconds so hung waits are
  diagnosable. No locks, no inotify — the registry is small and atomic-
  write, a poll loop is simplest.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterator, Literal, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("slurm.parallel.peer_info")


_PeerState = Literal["pending", "ready", "failed"]


@dataclass(frozen=True)
class PeerInfo:
    """One peer replica as visible to user code.

    Mirrors :class:`~slurm.parallel.registry.PeerRegistryEntry` but is
    frozen, with read-only mappings for ``ports`` and ``metadata``. Peer
    code should treat a :class:`PeerInfo` as a snapshot — call
    :meth:`PeerGroup.refresh` on the containing group to pick up changes.

    Attributes:
        name: Peer identifier (matches the key in ``ctx.peers``).
        replica_index: Zero-based replica index within the peer's set.
        replica_count: Total replica count for this peer.
        pool: Pool the peer landed in.
        hostname: Canonical hostname for this replica (usually
            ``hostnames[0]``).
        hostnames: Every host the replica's step spans.
        node_label: Pool-local label assigned to this replica's node, if
            any.
        step_id: Slurm step id (``<jobid>.<stepid>``) once the peer has
            started — ``None`` during the pending window.
        ports: Declared ports; populated by Phase 9 port reservation. For
            now always empty.
        metadata: Arbitrary user-announced key/value pairs merged from
            :meth:`Peer.announce` (static) and :meth:`JobContext.announce`
            (runtime).
        state: ``"pending"``, ``"ready"``, or ``"failed"``. Other values
            (``"shutdown_by_leader"`` etc.) map to ``"failed"`` so user
            code stays simple.
        restart_count: Number of times the supervisor has re-launched this
            replica.
        component_index: Hetjob component index the peer landed in. For
            single-pool submissions always ``0``.
    """

    name: str
    replica_index: int
    replica_count: int
    pool: str
    hostname: str
    hostnames: Tuple[str, ...]
    node_label: Optional[str]
    step_id: Optional[str]
    ports: Mapping[str, int]
    metadata: Mapping[str, Any]
    state: _PeerState
    restart_count: int
    component_index: int

    @classmethod
    def from_entry(cls, data: Mapping[str, Any]) -> "PeerInfo":
        """Build a :class:`PeerInfo` from a raw registry entry dict."""
        raw_state = data.get("state", "pending")
        # Collapse supervisor-internal states into the user-facing trio so
        # callers can rely on the Literal type without knowing the full
        # lifecycle vocabulary.
        if raw_state == "ready":
            state: _PeerState = "ready"
        elif raw_state in ("pending", "running"):
            state = "pending"
        else:
            state = "failed"

        return cls(
            name=str(data["name"]),
            replica_index=int(data.get("replica_index", 0)),
            replica_count=int(data.get("replica_count", 1)),
            pool=str(data.get("pool", "")),
            hostname=str(data.get("hostname", "")),
            hostnames=tuple(data.get("hostnames") or ()),
            node_label=data.get("node_label"),
            step_id=data.get("step_id"),
            ports=MappingProxyType(dict(data.get("ports") or {})),
            metadata=MappingProxyType(dict(data.get("metadata") or {})),
            state=state,
            restart_count=int(data.get("restart_count", 0)),
            component_index=int(data.get("component_index", 0)),
        )


@dataclass
class PeerGroup:
    """All replicas of a single named peer.

    Obtained via ``ctx.peers["<name>"]``. Supports index lookup, iteration,
    ready filtering, and :meth:`wait_all` — the core service-discovery
    operations design §8.3 specifies.

    The group holds a private ``_path`` pointing at ``registry.json`` so
    :meth:`refresh` and :meth:`wait_all` can re-read without the caller
    passing paths around.
    """

    name: str
    _replicas: Tuple[PeerInfo, ...]
    _path: Optional[Path] = None

    def __len__(self) -> int:
        return len(self._replicas)

    def __iter__(self) -> Iterator[PeerInfo]:
        return iter(self._replicas)

    def __getitem__(self, index: int) -> PeerInfo:
        return self._replicas[index]

    @property
    def first(self) -> PeerInfo:
        """Return the first replica.

        Raises:
            IndexError: If the group is empty. In practice the bootstrap
                always seeds at least one replica per peer, so an empty
                group indicates a misconstructed group.
        """
        if not self._replicas:
            raise IndexError(f"PeerGroup {self.name!r} has no replicas")
        return self._replicas[0]

    @property
    def hostnames(self) -> Tuple[str, ...]:
        """Tuple of hostnames across all replicas (in replica_index order)."""
        return tuple(r.hostname for r in self._replicas)

    def ready_only(self) -> Iterator[PeerInfo]:
        """Iterator over replicas whose ``state == "ready"``."""
        return (r for r in self._replicas if r.state == "ready")

    def refresh(self) -> "PeerGroup":
        """Re-read the registry from disk, returning the refreshed group.

        The receiver mutates in-place AND returns ``self`` so both
        ``group.refresh()`` and ``group = group.refresh()`` work.
        """
        if self._path is None:
            return self
        groups = _load_groups_from_path(self._path)
        refreshed = groups.get(self.name)
        if refreshed is None:
            # Peer disappeared from registry — leave cached replicas in
            # place and log. This shouldn't happen in practice.
            logger.debug(
                "refresh(): peer %r missing from %s — keeping cached replicas",
                self.name,
                self._path,
            )
            return self
        self._replicas = refreshed._replicas
        return self

    def wait_all(
        self,
        keys: Optional[Sequence[str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Block until every replica has announced the requested ``keys``.

        When ``keys`` is ``None``, waits for every replica's ``state`` to
        reach ``"ready"``. When provided, waits until every replica's
        ``metadata`` contains every name in ``keys`` (values can be
        anything; presence is the signal).

        Polling starts at 50ms and doubles up to 1s. A DEBUG log fires
        every ~3 seconds of waiting so hung waits are diagnosable without
        attaching a debugger.

        Args:
            keys: Metadata keys every replica must have announced. ``None``
                means wait for ``state == "ready"`` instead.
            timeout: Seconds before raising :class:`TimeoutError`. ``None``
                waits forever.

        Raises:
            TimeoutError: If ``timeout`` elapses before the condition is met.
            RuntimeError: If this group was built without a registry path
                (e.g. synthetic test group).
        """
        if self._path is None:
            # With no registry path we can't re-read — if the initial
            # snapshot already satisfies the condition, return; otherwise
            # there's no mechanism to make progress.
            if _group_condition_met(self, keys):
                return
            raise RuntimeError(
                f"PeerGroup {self.name!r} has no registry path; cannot "
                "wait_all without a way to refresh. Construct via "
                "load_peer_groups(...) or pass a registry path."
            )

        deadline = None if timeout is None else time.monotonic() + timeout
        delay = 0.05
        start = time.monotonic()
        last_log = start
        while True:
            self.refresh()
            if _group_condition_met(self, keys):
                return
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                waited = now - start
                raise TimeoutError(
                    f"wait_all({self.name!r}, keys={keys!r}) timed out after "
                    f"{waited:.2f}s — replicas not ready: "
                    f"{_describe_pending(self, keys)}"
                )
            if now - last_log > 3.0:
                logger.debug(
                    "wait_all(%r, keys=%r): still waiting after %.1fs; pending=%s",
                    self.name,
                    keys,
                    now - start,
                    _describe_pending(self, keys),
                )
                last_log = now
            sleep_for = delay
            if deadline is not None:
                sleep_for = min(sleep_for, max(deadline - now, 0.0))
            if sleep_for > 0:
                time.sleep(sleep_for)
            # Exponential backoff, capped at 1s — the registry is cheap
            # to re-read but we don't want a hung wait to spin the CPU.
            delay = min(delay * 2.0, 1.0)


def _group_condition_met(group: PeerGroup, keys: Optional[Sequence[str]]) -> bool:
    """Return True when every replica in ``group`` satisfies the wait condition."""
    if not group._replicas:
        return False
    if keys is None:
        return all(r.state == "ready" for r in group._replicas)
    key_list = list(keys)
    return all(all(k in r.metadata for k in key_list) for r in group._replicas)


def _describe_pending(group: PeerGroup, keys: Optional[Sequence[str]]) -> str:
    """Human-readable summary of which replicas haven't met the condition."""
    missing: list[str] = []
    for r in group._replicas:
        if keys is None:
            if r.state != "ready":
                missing.append(f"{r.name}[{r.replica_index}]={r.state}")
        else:
            absent = [k for k in keys if k not in r.metadata]
            if absent:
                missing.append(f"{r.name}[{r.replica_index}] missing {absent}")
    return ", ".join(missing) if missing else "<none>"


def _load_groups_from_path(path: Path) -> Mapping[str, PeerGroup]:
    """Private reader: parse ``registry.json`` into ``{name: PeerGroup}``.

    Errors are logged at DEBUG and return an empty mapping — service
    discovery inside peer code should never crash because the registry is
    momentarily missing or malformed. Bootstrapping issues surface through
    supervisor logs and the job's exit code.
    """
    try:
        from .registry import read_registry
    except ImportError:  # pragma: no cover - defensive for partial trees
        return {}

    try:
        registry = read_registry(path)
    except FileNotFoundError:
        logger.debug("registry.json not found at %s — no peers visible", path)
        return {}
    except (OSError, ValueError) as exc:
        logger.debug("Could not read registry at %s: %s", path, exc)
        return {}

    peers_section = registry.get("peers") or {}
    out: dict[str, PeerGroup] = {}
    for name, entries in peers_section.items():
        infos = tuple(PeerInfo.from_entry(entry) for entry in entries)
        # Sort by replica_index so indexing is stable regardless of JSON
        # key order (older Python versions preserved insertion order, but
        # future merge semantics should not rely on it).
        infos = tuple(sorted(infos, key=lambda i: i.replica_index))
        out[str(name)] = PeerGroup(name=str(name), _replicas=infos, _path=path)
    return out


__all__ = [
    "PeerInfo",
    "PeerGroup",
]
