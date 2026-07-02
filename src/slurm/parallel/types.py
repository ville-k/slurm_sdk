"""Declarative types for ``parallel(...)`` — Peer, Pool, Topology.

These dataclasses describe *what* a parallel submission should run. They do
not carry runtime state. :func:`slurm.parallel.parallel` consumes them, builds
a :class:`_ParallelSpec`, validates it, and hands it to the rendering /
submission pipeline.

Everything is frozen — every modifier returns a new object. ``Peer`` /
``Peer.replicas`` are frozen dataclasses with tuple-valued collection fields
so an instance is safe to share between threads and across ``parallel(...)``
calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
)

if TYPE_CHECKING:
    from ..task import BoundTask, SlurmTask


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

OnFailurePolicy = Literal["kill", "continue"]
"""Per-peer failure disposition.

- ``"kill"`` (default) — peer failure aborts the allocation.
- ``"continue"`` — peer failure is tolerated; the rest of the job keeps running.
"""

ArgsSpec = Union[Sequence[Any], Callable[[int], Any], range, None]
"""How per-replica args are generated in :meth:`Peer.replicas`.

Same semantics as :meth:`SlurmTask.map`:

- ``list``/``tuple``: one entry per replica; dict entries become kwargs, tuple
  entries become positional args, scalars are passed as the first positional.
- ``Callable[[int], ...]``: invoked with the replica index to produce args.
- ``range``: each replica gets its index value as the first positional arg.
- ``None``: every replica gets identical args (from the upstream
  :meth:`SlurmTask.partial` binding).
"""


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Pool:
    """One homogeneous hardware reservation.

    A pool declares the *shape* of a reservation: how many nodes, of what
    per-node size, satisfying what partition / constraint / account. Peers
    target a pool by name; Slurm allocates the pool and runs each peer's
    step inside it.

    Attributes:
        nodes: Exact number of nodes to reserve. Must be ``>= 1``.
        cpus_per_node: CPUs available per node. Optional; when set, the
            validator checks that the sum of per-peer CPU claims fits.
        mem_per_node: Per-node memory (Slurm format: ``"64G"``, ``"1T"``,
            ``"384G"``). Optional.
        gpus_per_node: GPUs per node. Required for peers with
            ``gpus_per_task`` > 0.
        partition: ``--partition=...`` — which Slurm partition to allocate in.
        qos: ``--qos=...``.
        account: ``--account=...``.
        constraint: Node feature expression, e.g. ``"h100&nvlink4"``.
        gpu_type: Convenience sugar — compiles to ``--gres=gpu:<type>:<gpus_per_node>``.
            Mutually exclusive with ``gres["gpu"]``.
        features: List of features, AND-joined with ``constraint``.
        reservation: ``--reservation=...``.
        exclude_nodes: List of hostnames to exclude.
        time: Per-pool walltime override; defaults to ``parallel(time=)``.
        exclusive: Emit ``--exclusive`` on the component.
        gres: Raw GRES passthrough — e.g. ``{"gpu": "h100:8", "bb": "capacity=1TB"}``.
        extra_sbatch: Raw ``#SBATCH`` directives for flags the SDK does not model.

    Example:
        >>> Pool(
        ...     nodes=2,
        ...     gpus_per_node=8,
        ...     cpus_per_node=128,
        ...     mem_per_node="1.5T",
        ...     gpu_type="h100",
        ...     partition="gpu-fat",
        ... )
    """

    nodes: int
    cpus_per_node: Optional[int] = None
    mem_per_node: Optional[str] = None
    gpus_per_node: Optional[int] = None
    partition: Optional[str] = None
    qos: Optional[str] = None
    account: Optional[str] = None
    constraint: Optional[str] = None
    gpu_type: Optional[str] = None
    features: tuple[str, ...] = ()
    reservation: Optional[str] = None
    exclude_nodes: Optional[tuple[str, ...]] = None
    time: Optional[str] = None
    exclusive: bool = False
    gres: Mapping[str, str] = field(default_factory=dict)
    extra_sbatch: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Coerce iterables to tuples for immutability; build fresh dicts.
        if self.features is not None and not isinstance(self.features, tuple):
            object.__setattr__(self, "features", tuple(self.features))
        if self.exclude_nodes is not None and not isinstance(self.exclude_nodes, tuple):
            object.__setattr__(self, "exclude_nodes", tuple(self.exclude_nodes))
        if not isinstance(self.gres, dict):
            object.__setattr__(self, "gres", dict(self.gres))
        else:
            object.__setattr__(self, "gres", dict(self.gres))
        if not isinstance(self.extra_sbatch, dict):
            object.__setattr__(self, "extra_sbatch", dict(self.extra_sbatch))
        else:
            object.__setattr__(self, "extra_sbatch", dict(self.extra_sbatch))

        # Field-level validation (cheap, obvious errors — surface them early)
        if self.nodes < 1:
            raise ValueError(f"Pool nodes must be >= 1, got {self.nodes!r}")
        if self.cpus_per_node is not None and self.cpus_per_node < 1:
            raise ValueError(
                f"Pool cpus_per_node must be >= 1 when set, got {self.cpus_per_node!r}"
            )
        if self.gpus_per_node is not None and self.gpus_per_node < 0:
            raise ValueError(
                f"Pool gpus_per_node must be >= 0 when set, got {self.gpus_per_node!r}"
            )
        if self.gpu_type and "gpu" in self.gres:
            raise ValueError(
                "Pool: gpu_type and gres['gpu'] are mutually exclusive — pick one"
            )


# ---------------------------------------------------------------------------
# Peer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Peer:
    """One service in a parallel allocation.

    A ``Peer`` wraps a task (``SlurmTask`` or :class:`BoundTask`) with
    lifecycle directives. It compiles to one ``srun`` step inside the
    allocation.

    Use :meth:`Peer.replicas` to declare a replica set (multiple tasks running
    the same function with per-replica args). Bare ``Peer(...)`` is a
    single-replica peer.

    Attributes:
        task: The function to run — a ``SlurmTask`` or a ``BoundTask`` (from
            ``SlurmTask.partial(...)``). Normalized to ``BoundTask`` internally.
        pool: Destination pool name. Must match a key in
            ``Topology.pools``. ``None`` means "use the topology's default pool."
        name: Unique identifier for this peer in the job. Defaults to the
            task function's ``__name__``. Used for registry lookup
            (``ctx.peers["<name>"]``) and result access (``job["<name>"]``).
        leader: When any ``leader=True`` peer exits — success or failure —
            the supervisor signals graceful shutdown to every other peer.
            Multiple leaders are allowed; the first to exit wins.
        on_failure: See :data:`OnFailurePolicy`.
        exclusive: Emit ``--exclusive`` on the step — no other step on the
            same node shares its CPUs / GPUs.
        srun_args: Extra flags appended verbatim to the step's ``srun`` line.
            The escape hatch for advanced placement
            (``--gpu-bind=closest``, ``--distribution=block:block``, ...).
        announce: Static registry metadata written at launch, before user
            code runs. Visible as
            ``ctx.peers["<name>"][i].metadata``. Reserved keys
            (``name``, ``replica_index``, ``replica_count``, ``hostname``,
            ``hostnames``, ``step_id``, ``state``, ``pool``, ``metadata``)
            are rejected.
        count: Replica count. ``1`` for singletons (use :meth:`Peer.replicas`
            for ``count > 1``).
        args: Per-replica arg generator. See :data:`ArgsSpec`. Only valid
            with ``count > 1``.
        tasks_per_node: ``--ntasks-per-node=N``. Only valid with ``count > 1``.
            Default: computed from pool shape so replicas pack densely.

    Example:
        Singleton peer:

            >>> Peer(train.partial(cfg=cfg), pool="gpu", leader=True)

        Replica set:

            >>> Peer.replicas(
            ...     inference, count=8, pool="serve",
            ...     args=lambda i: {"worker_id": i},
            ...     on_failure="continue",
            ... )
    """

    task: Any  # SlurmTask | BoundTask — normalized to BoundTask in __post_init__
    pool: Optional[str] = None
    name: Optional[str] = None
    leader: bool = False
    on_failure: OnFailurePolicy = "kill"
    exclusive: bool = False
    srun_args: tuple[str, ...] = ()
    announce: Optional[Mapping[str, Any]] = None

    # Replica-set fields — use Peer.replicas(...) for the intended path.
    count: int = 1
    args: ArgsSpec = None
    tasks_per_node: Optional[int] = None

    def __post_init__(self) -> None:
        # Normalize task → BoundTask. Done lazily via import so types.py does
        # not depend on task.py at import time.
        from ..task import BoundTask, SlurmTask

        if isinstance(self.task, SlurmTask):
            object.__setattr__(self, "task", BoundTask(task=self.task))
        elif not isinstance(self.task, BoundTask):
            raise TypeError(
                "Peer task must be a SlurmTask (from @task) or a BoundTask "
                f"(from task.partial(...)), got {type(self.task).__name__}"
            )

        # Coerce tuple-valued collections.
        if self.srun_args and not isinstance(self.srun_args, tuple):
            object.__setattr__(self, "srun_args", tuple(self.srun_args))
        if self.announce is not None:
            # Freeze into a plain dict so callers can't mutate through a
            # shared reference.
            object.__setattr__(self, "announce", dict(self.announce))

        # Cheap, obvious validation — richer cross-peer checks happen in
        # validation.py at spec assembly time.
        if self.on_failure not in ("kill", "continue"):
            raise ValueError(
                f"on_failure must be one of kill/continue, got {self.on_failure!r}"
            )
        if self.count < 1:
            raise ValueError(f"Peer count must be >= 1, got {self.count!r}")
        if self.count == 1:
            if self.args is not None:
                raise ValueError(
                    "Peer(args=...) is only valid for replica sets; use "
                    "Peer.replicas(..., args=...) or drop args"
                )
            if self.tasks_per_node is not None:
                raise ValueError(
                    "Peer(tasks_per_node=...) is only valid for replica sets"
                )
        if self.leader and self.on_failure == "continue":
            raise ValueError(
                "leader=True + on_failure='continue' is meaningless: the "
                "leader's exit triggers shutdown, but a silently-failing "
                "leader leaves the group without a termination condition. "
                "Pick one: leader=True with a real policy, or drop leader=True."
            )

    @classmethod
    def replicas(
        cls,
        task: "SlurmTask | BoundTask",
        *,
        count: int,
        args: ArgsSpec = None,
        pool: Optional[str] = None,
        name: Optional[str] = None,
        on_failure: OnFailurePolicy = "kill",
        exclusive: bool = False,
        tasks_per_node: Optional[int] = None,
        srun_args: Sequence[str] = (),
        announce: Optional[Mapping[str, Any]] = None,
    ) -> "Peer":
        """Declare a replica set — multiple instances of the same task.

        Compiles to one ``srun --ntasks=<count>`` step. Each replica gets its
        own ``SLURM_PROCID`` (0..count-1) and picks up its args from
        ``args_fn(procid)`` / ``args_list[procid]`` at runtime.

        Args:
            task: Function to run. Common args come from ``task.partial(...)``;
                per-replica variance comes from ``args=``.
            count: Number of replicas.
            args: Per-replica arg generator. See :data:`ArgsSpec`.
            pool: Destination pool.
            name: Shared name for all replicas (``ctx.peers[name][i]``).
            on_failure: Failure policy applied per-replica.
            exclusive: Per-step ``--exclusive``.
            tasks_per_node: ``--ntasks-per-node=N``.
            srun_args: Verbatim srun flags.
            announce: Static metadata attached to every replica.
        """
        return cls(
            task=task,
            pool=pool,
            name=name,
            leader=False,  # replica sets are never leaders
            on_failure=on_failure,
            exclusive=exclusive,
            srun_args=tuple(srun_args),
            announce=announce,
            count=count,
            args=args,
            tasks_per_node=tasks_per_node,
        )

    @property
    def is_replica_set(self) -> bool:
        """Whether this peer is a replica set (``count > 1``)."""
        return self.count > 1

    def replica_indices(self) -> range:
        """Iterate over replica indices ``0..count-1``.

        Returns ``range(1)`` for singleton peers so callers can uniformly
        enumerate replicas without branching on :attr:`is_replica_set`.
        """
        return range(self.count)

    @property
    def resolved_name(self) -> str:
        """The peer's name, falling back to the task function's ``__name__``."""
        if self.name is not None:
            return self.name
        # Walk through BoundTask → SlurmTask → func
        from ..task import BoundTask

        bt = self.task if isinstance(self.task, BoundTask) else None
        if bt is None:
            return "peer"
        return bt.task.func.__name__


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Topology:
    """Collection of named pools for one ``parallel(...)`` call.

    A topology holds exactly one pool in this release; multi-pool
    (heterogeneous Slurm jobs) is rejected at construction time.

    Attributes:
        pools: Mapping of pool name to :class:`Pool`. Must contain exactly
            one entry. Peers reference the pool by name.
        default_pool: Pool used by peers that omit ``pool=``. Auto-set to
            the single pool's name.
        network: ``#SBATCH --network=...`` applied to the pool (e.g. ``"efa"``).
        prolog: Shell snippet inserted into the batch script before the first
            peer launches. Runs once per allocation on the head node.
        epilog: Shell snippet inserted after the supervisor exits. Runs once.

    Example:
        >>> Topology(
        ...     pools={
        ...         "learn": Pool(nodes=2, gpus_per_node=8, gpu_type="h100"),
        ...     },
        ... )
    """

    pools: Mapping[str, Pool]
    default_pool: Optional[str] = None
    network: Optional[str] = None
    prolog: Optional[str] = None
    epilog: Optional[str] = None

    def __post_init__(self) -> None:
        # Freeze the pools mapping into a plain dict to detach from any
        # shared caller reference.
        object.__setattr__(self, "pools", dict(self.pools))

        if not self.pools:
            raise ValueError("Topology must contain at least one pool")

        for name, pool in self.pools.items():
            if not isinstance(name, str) or not name:
                raise ValueError(
                    f"Topology pool key must be a non-empty string, got {name!r}"
                )
            if not isinstance(pool, Pool):
                raise TypeError(
                    f"Topology.pools[{name!r}] must be a Pool, got "
                    f"{type(pool).__name__}"
                )

        if len(self.pools) > 1:
            raise ValueError(
                "Multi-pool topologies (heterogeneous Slurm jobs) are not "
                "supported in this release; declare a single Pool. "
                "See fable_plan.md."
            )

        if self.default_pool is None:
            # Auto-resolve the default for the single pool.
            object.__setattr__(self, "default_pool", next(iter(self.pools.keys())))

        if self.default_pool is not None and self.default_pool not in self.pools:
            raise ValueError(
                f"Topology default_pool={self.default_pool!r} is not in "
                f"pools={list(self.pools)}"
            )


# ---------------------------------------------------------------------------
# Internal: the normalized spec passed to rendering / validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ParallelSpec:
    """Normalized representation of a ``parallel(...)`` call.

    Every peer has a resolved ``name`` and ``pool``; ``topology`` is
    non-optional (a single-pool implicit default is constructed when the
    caller omitted ``topology=``).
    """

    peers: tuple[Peer, ...]
    topology: Topology
    time: Optional[str] = None
    account: Optional[str] = None
    qos: Optional[str] = None
    reservation: Optional[str] = None
    network: Optional[str] = None
    grace_period_seconds: int = 10

    def peer_by_name(self, name: str) -> Peer:
        """Look up a peer by resolved name; raises ``KeyError`` if missing."""
        for peer in self.peers:
            if peer.resolved_name == name:
                return peer
        raise KeyError(name)

    def leaders(self) -> tuple[Peer, ...]:
        """Tuple of peers with ``leader=True``."""
        return tuple(p for p in self.peers if p.leader)
