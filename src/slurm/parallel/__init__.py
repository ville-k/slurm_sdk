"""Public API for ``parallel(...)`` — declarative multi-peer submissions.

``parallel(...)`` submits **one Slurm allocation running N peer tasks** —
each a step inside the allocation. It unifies three shapes behind one entry
point:

- Symmetric peers (no ``leader=``) — every peer must succeed.
- Leader + sidecars (``leader=True`` on the primary) — sidecars shut down
  gracefully when the leader exits.
- Heterogeneous multi-pool topologies (``topology=Topology(pools=...)``) —
  different peers on different node types via Slurm hetjobs.

See ``unified_design_parallel_tasks.md`` at the repo root for the full design.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Union

from ..errors import TopologyError
from .types import (
    ArgsSpec,
    FailureCallback,
    NodeRef,
    OnFailurePolicy,
    Peer,
    Pool,
    PortValue,
    Topology,
    _ParallelSpec,
)
from .validation import validate_spec

if TYPE_CHECKING:
    from ..task import BoundTask, SlurmTask


__all__ = [
    "parallel",
    "Peer",
    "Pool",
    "Topology",
    "ArgsSpec",
    "FailureCallback",
    "NodeRef",
    "OnFailurePolicy",
    "PortValue",
]


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------


PeerInput = Union[Peer, "SlurmTask", "BoundTask"]


def _coerce_peer(
    item: PeerInput,
    *,
    kwarg_name: Optional[str] = None,
) -> Peer:
    """Wrap a bare SlurmTask / BoundTask as a Peer; pass Peer through.

    When ``kwarg_name`` is set (the caller passed this as a keyword argument),
    the name becomes the peer's identifier — overriding whatever the Peer
    itself declared (or its task's ``__name__``). This is the "named peers"
    convenience in ``parallel(learner=..., inference=...)``.
    """
    from ..task import BoundTask, SlurmTask

    if isinstance(item, Peer):
        peer = item
    elif isinstance(item, (SlurmTask, BoundTask)):
        peer = Peer(task=item)
    else:
        raise TypeError(
            f"parallel() arguments must be SlurmTask, BoundTask, or Peer; "
            f"got {type(item).__name__}"
        )

    if kwarg_name is not None:
        # Replace name with the keyword (dataclass is frozen, so rebuild).
        peer = Peer(
            task=peer.task,
            pool=peer.pool,
            name=kwarg_name,
            leader=peer.leader,
            on_failure=peer.on_failure,
            max_restarts=peer.max_restarts,
            callback=peer.callback,
            exclusive=peer.exclusive,
            on_node=peer.on_node,
            colocate_with=peer.colocate_with,
            srun_args=peer.srun_args,
            announce=peer.announce,
            count=peer.count,
            args=peer.args,
            tasks_per_node=peer.tasks_per_node,
            on_nodes=peer.on_nodes,
        )

    return peer


def _infer_default_topology(peers: tuple[Peer, ...]) -> Topology:
    """Build a single-pool Topology whose shape covers the peers' demand.

    Used when the caller passed ``topology=None``. The implicit pool is named
    ``"default"`` and sized generously: ``nodes=1`` (most common case),
    with per-node CPU/GPU/mem computed as the sum of peer claims. This is
    intentionally loose — users who need tight resource control provide an
    explicit ``Topology``.
    """
    from ..task import BoundTask

    total_cpus = 0
    any_cpu = False
    total_gpus = 0
    any_gpu = False

    for peer in peers:
        sbatch = {}
        if isinstance(peer.task, BoundTask):
            sbatch = peer.task.task.sbatch_options

        cpt = sbatch.get("cpus_per_task")
        if cpt is not None:
            total_cpus += cpt * peer.count
            any_cpu = True
        gpt = sbatch.get("gpus_per_task")
        if gpt is not None:
            total_gpus += gpt * peer.count
            any_gpu = True

    pool = Pool(
        nodes=1,
        cpus_per_node=total_cpus if any_cpu else None,
        gpus_per_node=total_gpus if any_gpu else None,
    )
    return Topology(pools={"default": pool}, default_pool="default")


def _resolve_peer_pool(peer: Peer, default_pool: Optional[str]) -> Peer:
    """Return a copy of ``peer`` with ``pool`` filled in from the default."""
    if peer.pool is not None:
        return peer
    return Peer(
        task=peer.task,
        pool=default_pool,
        name=peer.name,
        leader=peer.leader,
        on_failure=peer.on_failure,
        max_restarts=peer.max_restarts,
        callback=peer.callback,
        exclusive=peer.exclusive,
        on_node=peer.on_node,
        colocate_with=peer.colocate_with,
        srun_args=peer.srun_args,
        announce=peer.announce,
        count=peer.count,
        args=peer.args,
        tasks_per_node=peer.tasks_per_node,
        on_nodes=peer.on_nodes,
    )


def _build_spec(
    *,
    positional: tuple[PeerInput, ...],
    named: dict[str, PeerInput],
    topology: Optional[Topology],
    time: Optional[str],
    account: Optional[str],
    qos: Optional[str],
    reservation: Optional[str],
    network: Optional[str],
    grace_period_seconds: int,
) -> _ParallelSpec:
    """Assemble a ``_ParallelSpec`` from raw ``parallel(...)`` inputs.

    This function only *builds* the spec. It does not call
    :func:`validate_spec`, so tests can exercise construction separately from
    validation.
    """
    if not positional and not named:
        raise TopologyError(
            "parallel() requires at least one peer. Pass peers positionally "
            "or as keyword arguments."
        )

    peers_list: list[Peer] = []
    for item in positional:
        peers_list.append(_coerce_peer(item))
    for kw_name, item in named.items():
        peers_list.append(_coerce_peer(item, kwarg_name=kw_name))

    peers_tuple = tuple(peers_list)

    resolved_topology = (
        topology if topology is not None else _infer_default_topology(peers_tuple)
    )
    default_pool = resolved_topology.default_pool

    peers_tuple = tuple(_resolve_peer_pool(p, default_pool) for p in peers_tuple)

    return _ParallelSpec(
        peers=peers_tuple,
        topology=resolved_topology,
        time=time,
        account=account,
        qos=qos,
        reservation=reservation,
        network=network,
        grace_period_seconds=grace_period_seconds,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parallel(
    *peers: PeerInput,
    topology: Optional[Topology] = None,
    time: Optional[str] = None,
    account: Optional[str] = None,
    qos: Optional[str] = None,
    reservation: Optional[str] = None,
    network: Optional[str] = None,
    grace_period_seconds: int = 10,
    after: object = None,
    **named_peers: PeerInput,
):
    """Submit a single Slurm allocation running N peer tasks concurrently.

    Args:
        *peers: Peer declarations. Each is a :class:`Peer`, a ``SlurmTask``
            (auto-wrapped in a singleton peer), or a ``BoundTask`` (from
            :meth:`SlurmTask.partial`).
        topology: Optional :class:`Topology` for heterogeneous multi-pool
            deployments. When ``None``, the SDK infers a single implicit pool
            sized from the peers' resource claims.
        time: Default walltime applied to every pool that doesn't override it.
        account: ``#SBATCH --account=...`` default.
        qos: ``#SBATCH --qos=...`` default.
        reservation: ``#SBATCH --reservation=...`` default.
        network: ``#SBATCH --network=...`` default.
        grace_period_seconds: Window between SIGTERM and SIGKILL during
            shutdown. Default 10.
        after: Optional upstream Job / ArrayJob / ParallelJob (or a list
            thereof). When provided, Slurm only schedules this allocation
            once every upstream job has completed successfully — every
            hetjob component carries ``#SBATCH --dependency=afterok:...``.
            Equivalent to calling ``.after(...)`` on the returned handle
            but avoids the cancel-and-resubmit round-trip.
        **named_peers: Peer declarations keyed by name. The keyword becomes
            the peer's ``name``, overriding any ``name=`` on the ``Peer``.
            Enables idiomatic result access: ``job["inference"]``.

    Returns:
        :class:`ParallelJob` representing the submitted allocation.

    Raises:
        TopologyError: If any peer/pool/placement/resource constraint is
            violated. The error aggregates every problem found.
        NotImplementedError: If the submission uses functionality that ships
            in a later phase (replica sets, multi-pool topologies, per-peer
            packaging, restart/callback failure policies, named-node placement).
        RuntimeError: If called outside a :class:`Cluster` context.

    Example:
        Leader + sidecars:

            >>> job = parallel(
            ...     Peer(train.partial(cfg=cfg), leader=True),
            ...     Peer(metrics, on_failure="continue"),
            ... )

        Heterogeneous topology:

            >>> job = parallel(
            ...     Peer(learner.partial(cfg=cfg), pool="gpu"),
            ...     Peer.replicas(simulator, count=64, pool="cpu",
            ...                   args=lambda i: {"env_id": i}),
            ...     topology=Topology(pools={
            ...         "gpu": Pool(nodes=1, gpus_per_node=8, gpu_type="h100"),
            ...         "cpu": Pool(nodes=4, cpus_per_node=96),
            ...     }),
            ... )
    """
    spec = _build_spec(
        positional=peers,
        named=named_peers,
        topology=topology,
        time=time,
        account=account,
        qos=qos,
        reservation=reservation,
        network=network,
        grace_period_seconds=grace_period_seconds,
    )
    validate_spec(spec)

    # Resolve the active Cluster the same way @task-decorated calls do. This
    # keeps the user-facing contract consistent: ``with Cluster(...) as c:``
    # is the entry path for both ``train(cfg)`` and ``parallel(train, metrics)``.
    from ..context import _resolve_cluster
    from .._parallel_submission import submit_parallel_spec

    cluster = _resolve_cluster("parallel")

    dependency_ids: Optional[List[str]] = None
    if after is not None:
        # Import lazily to avoid a circular import at module load time.
        from ..array_job import ArrayJob
        from ..job import Job
        from ..parallel_job import ParallelJob

        deps_list: List[object]
        if isinstance(after, (list, tuple)):
            deps_list = list(after)
        else:
            deps_list = [after]

        dependency_ids = []
        for dep in deps_list:
            if isinstance(dep, ParallelJob):
                dependency_ids.append(dep.job_id)
            elif isinstance(dep, ArrayJob):
                if not dep._submitted:
                    dep._submit()
                dependency_ids.extend(j.id for j in dep._jobs)
            elif isinstance(dep, Job):
                dependency_ids.append(dep.id)
            else:
                raise TypeError(
                    "parallel(after=...) expects Job, ArrayJob, or "
                    f"ParallelJob; got {type(dep).__name__}"
                )

    return submit_parallel_spec(cluster, spec, dependency_ids=dependency_ids)
