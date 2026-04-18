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
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from .errors import CompositeJobError, PeerFailureError
from .job import JobSnapshot
from .parallel.registry import (
    OUTCOME_CONTINUE_ON_FAILURE,
    OUTCOME_NOT_STARTED,
    OUTCOME_RESTARTED,
    OUTCOME_SHUTDOWN_BY_LEADER,
    OUTCOME_SUCCESS,
    read_registry,
)

if TYPE_CHECKING:
    from .array_job import ArrayJob
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


def _outcome_from_entry(entry: Dict[str, Any]) -> "PeerOutcome":
    """Build a :class:`PeerOutcome` from a raw registry entry dict."""
    outcome_str = entry.get("outcome") or OUTCOME_NOT_STARTED
    exit_code = entry.get("final_exit_code")
    if exit_code is not None:
        exit_code = int(exit_code)
    restart_count = int(entry.get("restart_count", 0) or 0)
    message = entry.get("message")
    return PeerOutcome(
        status=outcome_str,
        exit_code=exit_code,
        restart_count=restart_count,
        message=message,
    )


class ReplicaGroup:
    """Handle for the replicas of a single replica peer.

    Returned by ``ParallelJob["<name>"]`` when ``<name>`` refers to a peer
    created via :meth:`Peer.replicas`. Exposes list-like access to the
    underlying per-replica :class:`Job` objects plus aggregate
    :meth:`wait` / :meth:`get_results`.

    **Outcome atomicity (Phase 5 limitation):** the Slurm step that runs a
    replica peer is one ``srun --ntasks=<count>`` — the step has one exit
    code for all replicas. Phase 5 records that exit code as a *group*
    outcome: if it is non-zero and the peer's ``on_failure`` policy resolves
    to ``kill``, every replica's registry entry reads ``OUTCOME_FATAL``;
    on success, every replica reads ``OUTCOME_SUCCESS``. Per-replica
    granular outcome recording (one replica succeeds, another fails inside
    the same step) requires the runner to write per-task outcome sentinels
    to the registry and is deferred to Phase 7.

    Attributes:
        peer_name: Name of the replica peer.
        replica_jobs: Ordered list of per-replica :class:`Job` objects.
    """

    def __init__(self, peer_name: str, replica_jobs: List["Job"]) -> None:
        if not replica_jobs:
            raise ValueError(
                f"ReplicaGroup for peer {peer_name!r} must contain at least "
                "one replica Job"
            )
        self.peer_name = peer_name
        self._jobs: List["Job"] = list(replica_jobs)

    @property
    def replica_jobs(self) -> List["Job"]:
        """Shallow copy of the per-replica Job list (ordered by index)."""
        return list(self._jobs)

    def __len__(self) -> int:
        return len(self._jobs)

    def __iter__(self) -> Iterator["Job"]:
        return iter(self._jobs)

    def __getitem__(self, index: int) -> "Job":
        return self._jobs[index]

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for every replica to reach a terminal state.

        Returns ``True`` if every replica completed within ``timeout``,
        ``False`` otherwise. Replicas share one Slurm job id so they
        terminate together — this loops for symmetry with
        :meth:`ParallelJob.wait`.
        """
        for job in self._jobs:
            if not job.wait(timeout=timeout):
                return False
        return True

    def get_results(self, timeout: Optional[float] = None) -> List[Any]:
        """Collect each replica's result in index order.

        Raises whatever the per-replica :meth:`Job.get_result` raises — this
        surface is deliberately thin. For outcome-aware reading (fatal vs.
        shutdown-by-leader vs. continue) use
        :meth:`ParallelJob.get_results`, which consults the registry.
        """
        return [job.get_result(timeout=timeout) for job in self._jobs]

    def __repr__(self) -> str:
        return f"ReplicaGroup(peer={self.peer_name!r}, count={len(self._jobs)})"


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
        peer_replica_jobs: Optional[Dict[str, List["Job"]]] = None,
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
        # ``peer_replica_jobs`` holds the full per-replica Job list for every
        # replica peer; ``peer_jobs[name]`` only tracks the representative
        # (replica 0) Job to keep the legacy surface working. Singleton peers
        # never appear here.
        self._peer_replica_jobs: Dict[str, List["Job"]] = {
            name: list(jobs) for name, jobs in (peer_replica_jobs or {}).items()
        }

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

    def __getitem__(
        self, key: Union[str, Tuple[str, int]]
    ) -> Union["Job", "ReplicaGroup"]:
        """Look up a peer by name, or a specific replica by ``(name, index)``.

        * ``job["name"]`` for a singleton peer → the peer's :class:`Job`.
        * ``job["name"]`` for a replica peer → a :class:`ReplicaGroup`
          wrapping the per-replica Jobs (iterate / index / ``.get_results()``).
        * ``job["name", i]`` for a replica peer → the individual :class:`Job`
          at replica index ``i``.

        Raises:
            KeyError: If ``name`` is not a peer in this allocation, or the
                replica index is out of range.
            TypeError: If ``(name, i)`` is passed for a non-replica peer.
        """
        if isinstance(key, tuple):
            if (
                len(key) != 2
                or not isinstance(key[0], str)
                or not isinstance(key[1], int)
            ):
                raise KeyError(
                    "ParallelJob tuple indexing requires (peer_name, "
                    f"replica_index); got {key!r}"
                )
            name, idx = key
            if name not in self._peer_replica_jobs:
                if name in self._peer_jobs:
                    raise TypeError(
                        f"Peer {name!r} is not a replica set — use "
                        f'job["{name}"] without an index.'
                    )
                available = ", ".join(sorted(self._peer_jobs.keys()))
                raise KeyError(
                    f"ParallelJob has no peer {name!r}. Available peers: {available}"
                )
            jobs = self._peer_replica_jobs[name]
            if idx < 0 or idx >= len(jobs):
                raise KeyError(
                    f"Replica index {idx} out of range for peer {name!r} "
                    f"(count={len(jobs)})"
                )
            return jobs[idx]

        # Plain string key — singleton Job or ReplicaGroup.
        name = key
        if name in self._peer_replica_jobs:
            return ReplicaGroup(name, self._peer_replica_jobs[name])
        try:
            return self._peer_jobs[name]
        except KeyError as exc:
            available = ", ".join(sorted(self._peer_jobs.keys()))
            raise KeyError(
                f"ParallelJob has no peer {name!r}. Available peers: {available}"
            ) from exc

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until every peer (and replica) has reached a terminal state.

        Every peer shares the same Slurm job id, so we only need to wait on one
        of them — but we iterate for symmetry with :class:`ArrayJob` and to
        surface a ``False`` return if any wait times out.

        Args:
            timeout: Optional wall-clock timeout in seconds.

        Returns:
            ``True`` if every peer/replica reached a terminal state,
            ``False`` on timeout.
        """
        for name, job in self._peer_jobs.items():
            if name in self._peer_replica_jobs:
                # Replica peers — waiting on replica 0 is enough (same job
                # id) but iterate for symmetry and timeout aggregation.
                for rjob in self._peer_replica_jobs[name]:
                    if not rjob.wait(timeout=timeout):
                        return False
            else:
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

        Replica peers are flattened to per-replica keys — a replica peer
        named ``worker`` with ``count=3`` produces ``worker[0]``,
        ``worker[1]``, ``worker[2]`` in the returned dict. That matches how
        :class:`PeerFailureError` renders replica failures (``peer[i]``)
        and lets callers write uniform handling against
        ``peer_outcomes().values()``.

        Returns:
            A mapping with one entry per singleton peer and one entry per
            replica of every replica peer, using ``<name>[<i>]`` keys for
            replicas.
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
            if peer.is_replica_set:
                for replica_index in range(peer.count):
                    entry = (
                        entries[replica_index] if replica_index < len(entries) else {}
                    )
                    outcomes[f"{name}[{replica_index}]"] = _outcome_from_entry(entry)
            else:
                entry = entries[0] if entries else {}
                outcomes[name] = _outcome_from_entry(entry)
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

        Replica peers (``count > 1``) produce a ``list`` value of length
        ``count``, one entry per replica. Singleton peers produce a scalar
        value. The top-level dict mixes both when a job has both shapes.

        The registry is the system-of-record — the per-peer ``Job.get_result``
        call is still what deserializes user data. When no registry is
        available (tests using mocks that bypass the supervisor), this falls
        back to the Phase 3 behavior: propagate exceptions for ``on_failure=
        "kill"`` peers, map ``on_failure="continue"`` failures to ``None``.

        Args:
            timeout: Optional per-peer timeout. Applied independently to each
                peer's :meth:`Job.get_result`.

        Returns:
            Dict keyed by peer name. Scalar value for singleton peers, list
            value (length = replica count) for replica peers.

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

        for peer in self._spec.peers:
            peer_name = peer.resolved_name
            if peer.is_replica_set:
                replica_jobs = self._peer_replica_jobs.get(peer_name, [])
                replica_results: List[Any] = []
                for replica_index in range(peer.count):
                    outcome = outcomes.get(f"{peer_name}[{replica_index}]")
                    rjob = (
                        replica_jobs[replica_index]
                        if replica_index < len(replica_jobs)
                        else None
                    )
                    replica_results.append(
                        self._collect_replica_result(
                            peer,
                            replica_index,
                            rjob,
                            outcome,
                            has_registry_data,
                            timeout,
                            failures,
                        )
                    )
                results[peer_name] = replica_results
            else:
                job = self._peer_jobs[peer_name]
                outcome = outcomes.get(peer_name)
                results[peer_name] = self._collect_singleton_result(
                    peer, job, outcome, has_registry_data, timeout, failures
                )

        if failures:
            raise CompositeJobError(failures)

        return results

    def _collect_singleton_result(
        self,
        peer: Any,
        job: "Job",
        outcome: Optional[PeerOutcome],
        has_registry_data: bool,
        timeout: Optional[float],
        failures: "list[PeerFailureError]",
    ) -> Any:
        """Resolve one singleton peer's result slot."""
        if has_registry_data and outcome is not None:
            if outcome.status in (OUTCOME_SUCCESS, OUTCOME_RESTARTED):
                return job.get_result(timeout=timeout)
            if outcome.status in (
                OUTCOME_CONTINUE_ON_FAILURE,
                OUTCOME_SHUTDOWN_BY_LEADER,
            ):
                return None
            failures.append(
                PeerFailureError(
                    peer_name=peer.resolved_name,
                    replica_index=None,
                    exit_code=outcome.exit_code
                    if outcome.exit_code is not None
                    else -1,
                    message=outcome.message or f"peer outcome: {outcome.status}",
                )
            )
            return None

        # Legacy path — no registry; fall back to Phase 3 behavior.
        try:
            return job.get_result(timeout=timeout)
        except Exception:
            if peer.on_failure == "continue":
                return None
            raise

    def _collect_replica_result(
        self,
        peer: Any,
        replica_index: int,
        job: Optional["Job"],
        outcome: Optional[PeerOutcome],
        has_registry_data: bool,
        timeout: Optional[float],
        failures: "list[PeerFailureError]",
    ) -> Any:
        """Resolve one replica's result slot (see :meth:`get_results`)."""
        if has_registry_data and outcome is not None:
            if outcome.status in (OUTCOME_SUCCESS, OUTCOME_RESTARTED):
                if job is None:
                    return None
                return job.get_result(timeout=timeout)
            if outcome.status in (
                OUTCOME_CONTINUE_ON_FAILURE,
                OUTCOME_SHUTDOWN_BY_LEADER,
            ):
                return None
            failures.append(
                PeerFailureError(
                    peer_name=peer.resolved_name,
                    replica_index=replica_index,
                    exit_code=outcome.exit_code
                    if outcome.exit_code is not None
                    else -1,
                    message=outcome.message or f"peer outcome: {outcome.status}",
                )
            )
            return None

        if job is None:
            return None
        try:
            return job.get_result(timeout=timeout)
        except Exception:
            if peer.on_failure == "continue":
                return None
            raise

    def snapshot(
        self, tail_lines: int = 80
    ) -> Dict[str, Union[JobSnapshot, List[JobSnapshot]]]:
        """Capture a per-peer :class:`JobSnapshot` for this allocation.

        The returned dict carries one entry per peer:

        * Singleton peer → ``JobSnapshot`` (the peer's one Job).
        * Replica peer → ``list[JobSnapshot]`` (one entry per replica, in
          index order).

        Every snapshot is a point-in-time read of the underlying per-peer
        :class:`Job`. Because every peer shares the same Slurm job id, the
        ``state`` / ``exit_code`` / ``is_terminal`` fields will largely
        agree across peers — the per-peer layer is meaningful for
        ``stdout_tail`` / ``stderr_tail`` since each peer writes its own
        step output, and for ``result_path`` accounting.

        Args:
            tail_lines: Forwarded to each underlying :meth:`Job.snapshot`
                — the number of trailing stdout/stderr lines each snapshot
                captures. Defaults to 80.

        Returns:
            Dict keyed by peer name. Replica peers map to a list of
            snapshots indexed by replica position.
        """
        out: Dict[str, Union[JobSnapshot, List[JobSnapshot]]] = {}
        for peer in self._spec.peers:
            name = peer.resolved_name
            if peer.is_replica_set:
                replica_jobs = self._peer_replica_jobs.get(name, [])
                out[name] = [
                    job.snapshot(tail_lines=tail_lines) for job in replica_jobs
                ]
            else:
                job = self._peer_jobs[name]
                out[name] = job.snapshot(tail_lines=tail_lines)
        return out

    @property
    def leader_result(self) -> Any:
        """Return the single ``leader=True`` peer's result.

        Shorthand for the common "leader + helpers" pattern — the user
        only cares about the leader's return value and treats helpers as
        supporting services whose results are ``None``. Only legal when
        the allocation has exactly one ``leader=True`` peer.

        Returns:
            The leader peer's deserialised return value.

        Raises:
            RuntimeError: If the allocation has zero or more than one
                ``leader=True`` peer. The error message spells out the
                actionable fix ("declare one leader" / "use
                ``get_results()``").
        """
        leaders = [peer for peer in self._spec.peers if peer.leader]
        if not leaders:
            raise RuntimeError(
                "ParallelJob.leader_result requires exactly one leader peer, "
                "but none of the peers declared leader=True. Mark one peer "
                "with Peer(..., leader=True) or call get_results() to collect "
                "every peer's return value explicitly."
            )
        if len(leaders) > 1:
            names = ", ".join(p.resolved_name for p in leaders)
            raise RuntimeError(
                "ParallelJob.leader_result is ambiguous: multiple leader "
                f"peers declared (leader=True on {names}). A parallel "
                "allocation should have at most one leader. Use "
                "get_results() to collect every peer explicitly."
            )
        leader = leaders[0]
        if leader.is_replica_set:
            # A replica peer can't be a leader — the spec validator should
            # have caught this, but belt-and-braces.
            raise RuntimeError(
                f"Leader peer {leader.resolved_name!r} is declared as a "
                "replica set. A leader should be a singleton peer."
            )
        return self._peer_jobs[leader.resolved_name].get_result()

    def after(self, *deps: "Job | ArrayJob | ParallelJob") -> "ParallelJob":
        """Return a new :class:`ParallelJob` that waits for *deps* first.

        Propagates ``#SBATCH --dependency=afterok:<id>[:<id>...]`` onto
        every hetjob component of the re-submitted allocation so Slurm
        only schedules the parallel allocation after every upstream job
        has completed successfully.

        Because :func:`parallel` submits eagerly, ``.after()`` cancels the
        currently-queued allocation and submits a fresh one with the
        dependency string wired in. Call ``.after()`` promptly after
        ``parallel(...)`` — before any peer has started running — so the
        cancel is a no-op in practice.

        Args:
            *deps: One or more upstream handles. Accepts :class:`Job`,
                :class:`ArrayJob`, or :class:`ParallelJob`. ArrayJob is
                flattened to its per-item Jobs; ParallelJob contributes
                its base job id (every peer shares it, so one id per
                allocation suffices).

        Returns:
            A new :class:`ParallelJob` whose Slurm job id is distinct
            from the original. The original handle is cancelled.

        Raises:
            TypeError: If any element of ``deps`` is not a Job /
                ArrayJob / ParallelJob.
            RuntimeError: If the cluster is unavailable (shouldn't happen
                in practice — the original submission required one).
        """
        from .array_job import ArrayJob
        from .job import Job

        if not deps:
            # ``.after()`` with no args is ambiguous (do you want to
            # re-submit with no deps? keep the original?). Fail loudly
            # rather than silently no-op.
            raise TypeError(
                "ParallelJob.after() requires at least one upstream Job, "
                "ArrayJob, or ParallelJob to depend on."
            )

        dep_ids: List[str] = []
        for dep in deps:
            if isinstance(dep, ParallelJob):
                dep_ids.append(dep.job_id)
            elif isinstance(dep, ArrayJob):
                if not dep._submitted:
                    dep._submit()
                dep_ids.extend(j.id for j in dep._jobs)
            elif isinstance(dep, Job):
                dep_ids.append(dep.id)
            else:
                raise TypeError(
                    "ParallelJob.after() expects Job, ArrayJob, or "
                    f"ParallelJob arguments, got {type(dep).__name__}"
                )

        # Cancel the original allocation so we don't leak a second
        # sbatch. Slurm's scancel on a still-pending job is free; if it
        # has already started (unlikely for a fresh parallel() call),
        # the original side-effects are the user's responsibility.
        try:
            self.cancel()
        except Exception:
            # Cancellation is best-effort — an unreachable scheduler
            # will surface on the re-submit anyway.
            pass

        if self.cluster is None:
            raise RuntimeError(
                "ParallelJob.after() cannot re-submit because the "
                "original handle has no cluster reference. This should "
                "not happen for jobs created via parallel(...)."
            )

        from ._parallel_submission import submit_parallel_spec

        return submit_parallel_spec(self.cluster, self._spec, dependency_ids=dep_ids)

    def cancel(self) -> bool:
        """Cancel the allocation via ``scancel``.

        Every peer shares the same Slurm job id, so one scancel tears the
        whole allocation down. Returns ``True`` on a successful
        cancellation command, ``False`` if the backend reported a
        failure.
        """
        # All peers share one job id; just call cancel on the first Job.
        first_job = next(iter(self._peer_jobs.values()))
        return first_job.cancel()

    def __repr__(self) -> str:
        peers = ", ".join(sorted(self._peer_jobs.keys()))
        return f"ParallelJob(job_id={self._job_id!r}, peers=[{peers}])"
