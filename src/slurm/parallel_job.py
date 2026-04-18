"""Result type for ``parallel(...)`` submissions.

A ``ParallelJob`` represents one Slurm allocation running N peer tasks as
sibling ``srun`` steps. Peers share the Slurm job id but each has its own
per-peer ``Job`` object whose result file lives next to the others under the
shared job directory. This module provides the aggregate surface that users
interact with — lookup by peer name, aggregate ``wait()``, aggregate
``get_results()``.

Later phases expand the surface (``leader_result``, ``peer_outcomes``,
``snapshot``, ``after``, restart/continue semantics). Phase 2 keeps it small:
the minimum needed to drive the end-to-end submission path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from .cluster import Cluster
    from .job import Job
    from .parallel.types import _ParallelSpec


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
    ) -> None:
        if not peer_jobs:
            raise ValueError("ParallelJob requires at least one peer job")
        self.cluster = cluster
        self._job_id = job_id
        self._peer_jobs: Dict[str, "Job"] = dict(peer_jobs)
        self._spec = spec

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

    def get_results(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Collect each peer's result into a ``{peer_name: result}`` dict.

        Peers whose ``on_failure`` is ``"continue"`` are allowed to die
        silently — their slot in the dict is ``None``. Peers whose
        ``on_failure`` is ``"kill"`` propagate the underlying
        :class:`DownloadError` from :meth:`Job.get_result`. Composite /
        structured error reporting (``CompositeJobError``, ``peer_outcomes()``)
        arrives in Phase 4 alongside the richer failure-policy machinery.

        Args:
            timeout: Optional per-peer timeout. Applied independently to each
                peer's :meth:`Job.get_result`.

        Returns:
            Dict keyed by peer name.
        """
        results: Dict[str, Any] = {}
        for peer_name, job in self._peer_jobs.items():
            peer = self._spec.peer_by_name(peer_name)
            try:
                results[peer_name] = job.get_result(timeout=timeout)
            except Exception:
                if peer.on_failure == "continue":
                    results[peer_name] = None
                else:
                    raise
        return results

    def __repr__(self) -> str:
        peers = ", ".join(sorted(self._peer_jobs.keys()))
        return f"ParallelJob(job_id={self._job_id!r}, peers=[{peers}])"
