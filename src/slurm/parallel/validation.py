"""Submission-time validation for ``_ParallelSpec``.

The validator aggregates every problem it can find before raising, so the
user sees a single ``TopologyError`` listing all issues. This is deliberate:
a 24-hour RL job must not fail 30 seconds into ``sbatch`` because of a
typo'd pool name — all mistakes should surface before submission.

Design choices driving the checks here:

- Pools are placement groups: same pool = co-located; different pools =
  potentially different node types. Cross-pool ``colocate_with`` is always
  an error.
- Names are the peers' runtime identity (``ctx.peers["<name>"]``,
  ``job["<name>"]``). They must be unique.
- A peer can declare capacity via its underlying ``@task`` decorator's
  sbatch options; the validator sums per-node demand and compares to
  ``Pool.*_per_node`` when both are known.
- ``on_failure="continue"`` on a ``leader=True`` peer would leave the group
  without a defined termination condition → explicitly rejected.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..errors import TopologyError
from .types import Peer, Pool, _ParallelSpec

# Keys reserved by the peer registry; ``Peer.announce`` cannot use them.
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
        "metadata",
    }
)


def validate_spec(spec: _ParallelSpec) -> None:
    """Validate a parallel spec; raise :class:`TopologyError` on any issue.

    Problems are aggregated: every check runs even if earlier ones failed,
    so the user sees the full set in one error.

    Args:
        spec: The normalized spec to validate.

    Raises:
        TopologyError: If any pool/peer/placement/resource/name constraint is
            violated. The error's ``.problems`` attribute contains one string
            per problem.
    """
    problems: list[str] = []

    _check_unique_names(spec, problems)
    _check_pool_references(spec, problems)
    _check_announce_keys(spec, problems)
    _check_replica_args_length(spec, problems)
    _check_on_node(spec, problems)
    _check_colocate_with(spec, problems)
    _check_capacity(spec, problems)
    _check_outer_options(spec, problems)
    _check_callback_resolvability(spec, problems)

    if problems:
        lines = [f"  • {p}" for p in problems]
        message = (
            f"{len(problems)} problem(s) with parallel() submission:\n"
            + "\n".join(lines)
        )
        raise TopologyError(message, problems=problems)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _check_unique_names(spec: _ParallelSpec, problems: list[str]) -> None:
    seen: dict[str, int] = {}
    for peer in spec.peers:
        name = peer.resolved_name
        seen[name] = seen.get(name, 0) + 1
    for name, count in seen.items():
        if count > 1:
            problems.append(
                f"Peer name {name!r} is used by {count} peers. Each peer "
                "needs a unique name — set name=... on the Peer(...) "
                "call or pass the peer as a keyword argument "
                f"(parallel({name}=...))."
            )


def _check_pool_references(spec: _ParallelSpec, problems: list[str]) -> None:
    pool_keys = set(spec.topology.pools.keys())
    for peer in spec.peers:
        # By this point spec building has resolved pool=None to the default.
        if peer.pool is None:
            problems.append(
                f"Peer {peer.resolved_name!r} has no pool and no default pool "
                f"is set on the topology. Available pools: {sorted(pool_keys)}."
            )
        elif peer.pool not in pool_keys:
            problems.append(
                f"Peer {peer.resolved_name!r} references unknown pool "
                f"{peer.pool!r}. Available pools: {sorted(pool_keys)}."
            )


def _check_announce_keys(spec: _ParallelSpec, problems: list[str]) -> None:
    for peer in spec.peers:
        if not peer.announce:
            continue
        conflicts = [k for k in peer.announce if k in _RESERVED_ANNOUNCE_KEYS]
        if conflicts:
            problems.append(
                f"Peer {peer.resolved_name!r} announces reserved key(s) "
                f"{conflicts!r}. Reserved keys are "
                f"{sorted(_RESERVED_ANNOUNCE_KEYS)}."
            )


def _check_replica_args_length(spec: _ParallelSpec, problems: list[str]) -> None:
    for peer in spec.peers:
        if not peer.is_replica_set:
            continue
        args = peer.args
        if args is None or callable(args):
            continue  # length unknown until runtime; skip static check
        if isinstance(args, range):
            if len(args) != peer.count:
                problems.append(
                    f"Peer {peer.resolved_name!r} has args=range(...) with "
                    f"len {len(args)} but count={peer.count}."
                )
        elif isinstance(args, Sequence):
            if len(args) != peer.count:
                problems.append(
                    f"Peer {peer.resolved_name!r} has args= with len "
                    f"{len(args)} but count={peer.count}."
                )


def _check_on_node(spec: _ParallelSpec, problems: list[str]) -> None:
    pools = spec.topology.pools
    for peer in spec.peers:
        pool_name = peer.pool
        pool = pools.get(pool_name) if pool_name else None
        if pool is None:
            continue  # already reported in _check_pool_references

        if peer.on_node is not None:
            _check_single_node_ref(peer.resolved_name, pool, peer.on_node, problems)
        if peer.on_nodes is not None:
            for idx, node in enumerate(peer.on_nodes):
                _check_single_node_ref(
                    f"{peer.resolved_name}[{idx}]", pool, node, problems
                )


def _check_single_node_ref(
    who: str, pool: Pool, node: Any, problems: list[str]
) -> None:
    if isinstance(node, int):
        if node < 0 or node >= pool.nodes:
            problems.append(
                f"{who} pins to node ordinal {node} but pool has "
                f"{pool.nodes} node(s) (0..{pool.nodes - 1})."
            )
    elif isinstance(node, str):
        if pool.node_labels is None:
            problems.append(
                f"{who} pins to node label {node!r} but pool has no "
                "node_labels declared. Either add node_labels=[...] to "
                "the Pool(...) or use an integer ordinal."
            )
        elif node not in pool.node_labels:
            problems.append(
                f"{who} pins to unknown label {node!r}. Pool labels: "
                f"{list(pool.node_labels)}."
            )
    else:
        problems.append(
            f"{who} has invalid node reference {node!r}; must be a string "
            "label or integer ordinal."
        )


def _check_colocate_with(spec: _ParallelSpec, problems: list[str]) -> None:
    peers_by_name = {p.resolved_name: p for p in spec.peers}

    # First pass: cross-pool and unknown-target checks.
    for peer in spec.peers:
        target_name = peer.colocate_with
        if target_name is None:
            continue
        if target_name not in peers_by_name:
            problems.append(
                f"Peer {peer.resolved_name!r} has colocate_with="
                f"{target_name!r} but no peer with that name exists."
            )
            continue
        target = peers_by_name[target_name]
        if peer.pool != target.pool:
            problems.append(
                f"Peer {peer.resolved_name!r} colocate_with="
                f"{target_name!r} but peers are in different pools "
                f"({peer.pool!r} vs {target.pool!r}). colocation must stay "
                "within a single pool — move one peer or pick a different "
                "strategy."
            )

    # Second pass: cycle detection using DFS.
    # Build a graph: peer_name -> colocate_with_target (or None).
    graph: dict[str, str] = {}
    for peer in spec.peers:
        if peer.colocate_with and peer.colocate_with in peers_by_name:
            graph[peer.resolved_name] = peer.colocate_with

    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {name: WHITE for name in graph}

    def dfs(node: str, stack: list[str]) -> list[str] | None:
        color[node] = GREY
        stack.append(node)
        nxt = graph.get(node)
        if nxt is not None:
            if color.get(nxt, BLACK) == GREY:
                # cycle — return the cycle path
                cycle_start = stack.index(nxt)
                return stack[cycle_start:] + [nxt]
            if color.get(nxt, BLACK) == WHITE:
                cycle = dfs(nxt, stack)
                if cycle is not None:
                    return cycle
        color[node] = BLACK
        stack.pop()
        return None

    reported_cycles: set[tuple[str, ...]] = set()
    for start in list(graph.keys()):
        if color[start] == WHITE:
            cycle = dfs(start, [])
            if cycle is not None:
                key = tuple(sorted(cycle))
                if key in reported_cycles:
                    continue
                reported_cycles.add(key)
                problems.append(
                    "Cycle in colocate_with: "
                    + " → ".join(repr(n) for n in cycle)
                    + ". Break the cycle by pinning at least one peer "
                    "directly with on_node= or on_nodes=."
                )


def _check_capacity(spec: _ParallelSpec, problems: list[str]) -> None:
    """Compare per-pool per-node demand against declared capacity.

    Demand is computed conservatively: every peer contributes
    ``per_task_claim × replicas_on_a_node`` to the node's budget. For
    replicas with ``tasks_per_node=K``, demand per node is
    ``per_task_claim × K``; otherwise the validator assumes the densest
    possible packing — ``per_task_claim × ceil(count / nodes)``.

    Unknown claims (missing ``cpus_per_task`` on the task, missing
    ``cpus_per_node`` on the pool) are silently skipped — the validator is
    advisory for partial declarations. Fully-declared topologies get
    fully-checked budgets.
    """
    pools = spec.topology.pools

    # Group peers by pool so we can sum across peers targeting the same pool.
    peers_by_pool: dict[str, list[Peer]] = {}
    for peer in spec.peers:
        if peer.pool is None or peer.pool not in pools:
            continue
        peers_by_pool.setdefault(peer.pool, []).append(peer)

    for pool_name, peers in peers_by_pool.items():
        pool = pools[pool_name]
        demand = _compute_per_node_demand(peers, pool.nodes)

        if pool.cpus_per_node is not None and demand["cpus"] is not None:
            if demand["cpus"] > pool.cpus_per_node:
                problems.append(
                    f"Pool {pool_name!r} CPU overflow: demand "
                    f"{demand['cpus']} per node > capacity "
                    f"{pool.cpus_per_node} per node. Peers contributing: "
                    + _describe_peer_claims(peers, "cpus")
                )
        if pool.gpus_per_node is not None and demand["gpus"] is not None:
            if demand["gpus"] > pool.gpus_per_node:
                problems.append(
                    f"Pool {pool_name!r} GPU overflow: demand "
                    f"{demand['gpus']} per node > capacity "
                    f"{pool.gpus_per_node} per node. Peers contributing: "
                    + _describe_peer_claims(peers, "gpus")
                )


def _compute_per_node_demand(
    peers: list[Peer], pool_nodes: int
) -> dict[str, int | None]:
    """Sum per-node task demand for a group of peers sharing a pool."""
    total_cpus = 0
    total_gpus = 0
    any_cpu_known = False
    any_gpu_known = False

    for peer in peers:
        sbatch = _task_sbatch_options(peer)
        cpus_per_task = sbatch.get("cpus_per_task")
        gpus_per_task = sbatch.get("gpus_per_task")

        # How many replicas live on one node worst-case?
        if peer.tasks_per_node is not None:
            per_node = peer.tasks_per_node
        elif peer.count <= pool_nodes:
            per_node = 1  # spread across all nodes
        else:
            # Pack densely — ceil(count / nodes)
            per_node = -(-peer.count // pool_nodes)  # ceil division

        if cpus_per_task is not None:
            total_cpus += cpus_per_task * per_node
            any_cpu_known = True
        if gpus_per_task is not None:
            total_gpus += gpus_per_task * per_node
            any_gpu_known = True

    return {
        "cpus": total_cpus if any_cpu_known else None,
        "gpus": total_gpus if any_gpu_known else None,
    }


def _task_sbatch_options(peer: Peer) -> dict:
    """Extract the sbatch options declared on a peer's task."""
    from ..task import BoundTask

    if isinstance(peer.task, BoundTask):
        return dict(peer.task.task.sbatch_options)
    return {}


def _describe_peer_claims(peers: list[Peer], resource: str) -> str:
    """Human-friendly summary of which peers contribute a resource claim."""
    key_map = {"cpus": "cpus_per_task", "gpus": "gpus_per_task"}
    key = key_map[resource]
    parts: list[str] = []
    for peer in peers:
        sbatch = _task_sbatch_options(peer)
        claim = sbatch.get(key)
        if claim is None:
            continue
        label = f"{peer.resolved_name!r} ({claim}×{peer.count})"
        parts.append(label)
    return ", ".join(parts) if parts else "<none declared>"


def _check_outer_options(spec: _ParallelSpec, problems: list[str]) -> None:
    if spec.grace_period_seconds < 0:
        problems.append(
            f"grace_period_seconds must be >= 0, got {spec.grace_period_seconds!r}."
        )


def _check_callback_resolvability(spec: _ParallelSpec, problems: list[str]) -> None:
    """Reject callbacks the supervisor cannot import by fully-qualified name.

    The supervisor runs in its own process, so live callables cannot travel
    across the submission boundary via pickle reliably — we serialize them
    by ``module:qualname`` instead. That works for top-level functions and
    class methods with stable locations but fails for lambdas, nested
    functions, and anything else whose ``__qualname__`` contains ``<locals>``
    or ``<lambda>``. Detecting this at spec-build time gives users a clear
    error instead of a confusing ``AttributeError`` when the supervisor
    tries to resolve the callback.
    """
    for peer in spec.peers:
        if peer.on_failure != "callback" or peer.callback is None:
            continue
        cb = peer.callback
        module = getattr(cb, "__module__", None)
        qualname = getattr(cb, "__qualname__", None)
        if not module or not qualname:
            problems.append(
                f"Peer {peer.resolved_name!r} callback has no resolvable "
                "module/qualname — use a top-level function defined in an "
                "importable module."
            )
            continue
        if "<lambda>" in qualname or "<locals>" in qualname:
            problems.append(
                f"Peer {peer.resolved_name!r} callback {module}:{qualname} "
                "is a lambda or nested function — the supervisor cannot "
                "resolve it by name. Define the callback as a top-level "
                "function in an importable module."
            )
