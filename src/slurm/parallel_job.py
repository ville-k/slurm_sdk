"""Result type for ``parallel(...)`` submissions.

A ``ParallelJob`` represents one Slurm allocation running N peer tasks as
sibling ``srun`` steps. Peers share the Slurm job id but each has its own
per-peer ``Job`` object whose result file lives next to the others under the
shared job directory. This module provides the aggregate surface that users
interact with — lookup by peer name, aggregate ``wait()``, aggregate
``get_results()``.

Phase 4 adds :class:`PeerOutcome` and :meth:`ParallelJob.peer_outcomes`
so callers can inspect exactly what happened to each peer (success,
restarted, tolerated failure, fatal, shut down by leader, never started).
When any peer's outcome is ``"fatal"``, :meth:`ParallelJob.get_results`
now raises :class:`CompositeJobError` aggregating every fatal peer's
:class:`PeerFailureError`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

from .errors import CompositeJobError, PeerFailureError
from .parallel.registry import (
    OUTCOME_CONTINUE_ON_FAILURE,
    OUTCOME_NOT_STARTED,
    OUTCOME_RESTARTED,
    OUTCOME_SHUTDOWN_BY_LEADER,
    OUTCOME_SUCCESS,
    read_registry,
)

if TYPE_CHECKING:
    from .cluster import Cluster
    from .job import Job
    from .parallel.types import _ParallelSpec


@dataclass(frozen=True)
class PeerOutcome:
    """Terminal outcome of a single peer, derived from the supervisor's registry.

    Attributes:
        status: One of the six outcome strings:

            - ``"success"`` — exited 0 on first launch.
            - ``"continue_on_failure"`` — failed but ``on_failure="continue"``
              kept the group alive; the peer's result is ``None``.
            - ``"restarted"`` — eventually succeeded after at least one
              restart. Success is success even when it took multiple tries;
              ``restart_count`` carries the retry count.
            - ``"fatal"`` — peer's failure aborted the group (``on_failure=
              "kill"`` directly, restart budget exhausted, or callback
              returned ``"kill"``).
            - ``"shutdown_by_leader"`` — another peer (leader or fatal) won
              the shutdown race and this peer received SIGTERM before it
              could complete.
            - ``"not_started"`` — scheduled but never launched. Currently
              only possible if an upstream failure aborted the allocation
              before this peer's Popen fired.
        exit_code: Final process exit code, or ``None`` when the peer never
            ran.
        restart_count: Number of times the supervisor re-launched the peer.
            0 means the first launch was terminal.
        message: Free-form diagnostic string from the supervisor (e.g.
            ``"restart budget exhausted after 3 attempt(s)"``). ``None`` for
            plain success.
    """

    status: str
    exit_code: Optional[int]
    restart_count: int
    message: Optional[str] = None


class ParallelJob:
    """Aggregate handle for a submitted ``parallel(...)`` allocation.

    Every peer resolves to a :class:`Job` that shares the base Slurm job id but
    points at its own per-peer result file. Access a peer with
    ``job["<peer_name>"]`` and read its result with ``.get_result()``.
    ``get_results()`` is the idiomatic aggregate — it returns a dict keyed by
    peer name.

    Deliberate non-inheritance from :class:`Job`: a parallel job has no single
    return value, and pretending otherwise would force ``get_result()`` to pick
    a peer arbitrarily. The plural nature is surfaced at the type level, the
    same way :class:`ArrayJob` does.

    Attributes:
        cluster: The :class:`Cluster` the job was submitted through.
        job_id: The base Slurm job id (shared by every peer step).
        peer_jobs: Mapping from peer name to the per-peer :class:`Job`.

    Example:
        >>> job = parallel(
        ...     ocean.partial(cfg=ocean_cfg),
        ...     atmo.partial(cfg=atmo_cfg),
        ... )
        >>> job.wait()
        >>> results = job.get_results()
        >>> ocean_r, atmo_r = results["ocean"], results["atmo"]
    """

    def __init__(
        self,
        cluster: "Cluster",
        job_id: str,
        peer_jobs: Dict[str, "Job"],
        spec: "_ParallelSpec",
        target_job_dir: Optional[str] = None,
    ) -> None:
        if not peer_jobs:
            raise ValueError("ParallelJob requires at least one peer job")
        self.cluster = cluster
        self._job_id = job_id
        self._peer_jobs: Dict[str, "Job"] = dict(peer_jobs)
        self._spec = spec
        # Snapshot the target job dir so ``peer_outcomes`` can find
        # ``registry.json`` even if the per-peer Jobs get re-parented later.
        self._target_job_dir = target_job_dir

    @property
    def job_id(self) -> str:
        """The base Slurm job id (shared by every peer step)."""
        return self._job_id

    @property
    def peer_jobs(self) -> Dict[str, "Job"]:
        """Read-only mapping from peer name to per-peer :class:`Job`."""
        return dict(self._peer_jobs)

    def __iter__(self):
        return iter(self._peer_jobs.values())

    def __len__(self) -> int:
        return len(self._peer_jobs)

    def __contains__(self, name: object) -> bool:
        return name in self._peer_jobs

    def __getitem__(self, name: str) -> "Job":
        """Return the :class:`Job` for the peer with the given name.

        Raises:
            KeyError: If ``name`` is not a peer in this allocation.
        """
        try:
            return self._peer_jobs[name]
        except KeyError as exc:
            available = ", ".join(sorted(self._peer_jobs.keys()))
            raise KeyError(
                f"ParallelJob has no peer {name!r}. Available peers: {available}"
            ) from exc

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until every peer has reached a terminal state.

        Every peer shares the same Slurm job id, so we only need to wait on one
        of them — but we iterate for symmetry with :class:`ArrayJob` and to
        surface a ``False`` return if any per-peer wait times out.

        Args:
            timeout: Optional wall-clock timeout in seconds.

        Returns:
            ``True`` if every peer reached a terminal state, ``False`` on
            timeout.
        """
        for job in self._peer_jobs.values():
            if not job.wait(timeout=timeout):
                return False
        return True

    def _registry_path(self) -> Optional[str]:
        """Locate ``registry.json`` for this job, preferring stored metadata.

        We cache ``target_job_dir`` on construction because some Job objects
        overload ``_target_job_dir`` at runtime (workflow driver) — reading
        from the peer Job would give us a moving target.
        """
        if self._target_job_dir:
            return os.path.join(self._target_job_dir, "registry.json")
        # Fall back to the first peer's job directory.
        first_job = next(iter(self._peer_jobs.values()))
        tdir = getattr(first_job, "_target_job_dir", None) or getattr(
            first_job, "target_job_dir", None
        )
        if not tdir:
            return None
        return os.path.join(tdir, "registry.json")

    def peer_outcomes(self) -> Dict[str, PeerOutcome]:
        """Return ``{peer_name: PeerOutcome}`` for every peer in the job.

        Outcomes are derived from ``registry.json``, which the supervisor
        updates atomically as peers exit. Peers whose registry entry has
        no ``outcome`` field (because the supervisor is mid-run, or the
        registry never materialised) map to ``"not_started"`` so callers
        always see a fully-populated dict.

        Returns:
            A mapping keyed by :attr:`Peer.resolved_name`. One entry per
            peer declared in the spec.
        """
        registry: Dict[str, Any] = {}
        reg_path = self._registry_path()
        if reg_path and os.path.exists(reg_path):
            try:
                registry = read_registry(reg_path)
            except (OSError, ValueError):
                registry = {}

        peers_section = registry.get("peers", {})
        outcomes: Dict[str, PeerOutcome] = {}
        for peer in self._spec.peers:
            name = peer.resolved_name
            entries = peers_section.get(name, [])
            entry = entries[0] if entries else {}
            outcome_str = entry.get("outcome") or OUTCOME_NOT_STARTED
            exit_code = entry.get("final_exit_code")
            if exit_code is not None:
                exit_code = int(exit_code)
            restart_count = int(entry.get("restart_count", 0) or 0)
            message = entry.get("message")
            outcomes[name] = PeerOutcome(
                status=outcome_str,
                exit_code=exit_code,
                restart_count=restart_count,
                message=message,
            )
        return outcomes

    def get_results(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Collect each peer's result into a ``{peer_name: result}`` dict.

        Outcome-driven behavior (Phase 4):

        - ``success`` / ``restarted`` — peer's result is returned normally.
        - ``continue_on_failure`` — slot is ``None``; the tolerated failure
          does not abort ``get_results``.
        - ``shutdown_by_leader`` — slot is ``None`` (the peer never finished
          writing a result, but the group is not in an error state).
        - ``fatal`` / ``not_started`` — contributes a :class:`PeerFailureError`
          to a :class:`CompositeJobError` raised at the end. Every fatal peer
          is reported, so the user sees the full failure surface.

        The registry is the system-of-record — the per-peer ``Job.get_result``
        call is still what deserializes user data. When no registry is
        available (tests using mocks that bypass the supervisor), this falls
        back to the Phase 3 behavior: propagate exceptions for ``on_failure=
        "kill"`` peers, map ``on_failure="continue"`` failures to ``None``.

        Args:
            timeout: Optional per-peer timeout. Applied independently to each
                peer's :meth:`Job.get_result`.

        Returns:
            Dict keyed by peer name.

        Raises:
            CompositeJobError: If any peer outcome is ``"fatal"`` or
                ``"not_started"``.
        """
        outcomes = self.peer_outcomes()
        has_registry_data = any(
            outcome.status != OUTCOME_NOT_STARTED for outcome in outcomes.values()
        )

        results: Dict[str, Any] = {}
        failures: list[PeerFailureError] = []

        for peer_name, job in self._peer_jobs.items():
            peer = self._spec.peer_by_name(peer_name)
            outcome = outcomes.get(peer_name)

            if has_registry_data and outcome is not None:
                # Authoritative path — the supervisor recorded an outcome.
                if outcome.status in (OUTCOME_SUCCESS, OUTCOME_RESTARTED):
                    results[peer_name] = job.get_result(timeout=timeout)
                elif outcome.status in (
                    OUTCOME_CONTINUE_ON_FAILURE,
                    OUTCOME_SHUTDOWN_BY_LEADER,
                ):
                    results[peer_name] = None
                else:
                    # fatal / not_started — gather for CompositeJobError.
                    results[peer_name] = None
                    failures.append(
                        PeerFailureError(
                            peer_name=peer_name,
                            replica_index=None,
                            exit_code=outcome.exit_code
                            if outcome.exit_code is not None
                            else -1,
                            message=outcome.message
                            or f"peer outcome: {outcome.status}",
                        )
                    )
                continue

            # Legacy path — no registry; fall back to Phase 3 behavior.
            try:
                results[peer_name] = job.get_result(timeout=timeout)
            except Exception:
                if peer.on_failure == "continue":
                    results[peer_name] = None
                else:
                    raise

        if failures:
            raise CompositeJobError(failures)

        return results

    def __repr__(self) -> str:
        peers = ", ".join(sorted(self._peer_jobs.keys()))
        return f"ParallelJob(job_id={self._job_id!r}, peers=[{peers}])"
