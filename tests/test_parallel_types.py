"""Unit tests for the parallel types: Peer, Pool, Topology, BoundTask."""

from __future__ import annotations

import pytest

from slurm import BoundTask, Peer, Pool, Topology, task
from slurm.parallel.types import _ParallelSpec


# ---------------------------------------------------------------------------
# Shared task fixtures
# ---------------------------------------------------------------------------


@task(cpus_per_task=4, mem="16G")
def _train(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=2, mem="4G")
def _monitor() -> None:
    pass


@task(cpus_per_task=1, gpus_per_task=1)
def _sim(env_id: int) -> int:
    return env_id


# ---------------------------------------------------------------------------
# BoundTask
# ---------------------------------------------------------------------------


def test_bound_task_captures_args_and_kwargs():
    bound = _train.partial(cfg={"lr": 0.001})
    assert bound.args == ()
    assert bound.kwargs == {"cfg": {"lr": 0.001}}
    assert bound.task is _train


def test_bound_task_partial_composes():
    base = _train.partial(cfg={"lr": 0.001})
    extended = base.partial(extra="value")
    assert extended.kwargs == {"cfg": {"lr": 0.001}, "extra": "value"}
    # Later kwargs override earlier
    overridden = base.partial(cfg={"lr": 0.01})
    assert overridden.kwargs == {"cfg": {"lr": 0.01}}


def test_bound_task_not_callable():
    bound = _train.partial(cfg={"lr": 0.001})
    with pytest.raises(TypeError, match="not directly callable"):
        bound()


def test_bound_task_requires_slurm_task():
    with pytest.raises(TypeError, match="requires a SlurmTask"):
        BoundTask(task=lambda x: x)  # type: ignore[arg-type]


def test_bound_task_repr_contains_name():
    bound = _train.partial(cfg={"lr": 0.001})
    r = repr(bound)
    assert "_train" in r
    assert "cfg" in r


# ---------------------------------------------------------------------------
# Pool field validation
# ---------------------------------------------------------------------------


def test_pool_minimal():
    p = Pool(nodes=1)
    assert p.nodes == 1
    assert p.cpus_per_node is None
    assert p.gpus_per_node is None


def test_pool_node_labels_roundtrip_to_tuple():
    p = Pool(nodes=2, node_labels=["head", "ops"])
    assert p.node_labels == ("head", "ops")


def test_pool_node_labels_length_mismatch_rejected():
    with pytest.raises(ValueError, match="node_labels length"):
        Pool(nodes=3, node_labels=["head", "ops"])


def test_pool_node_labels_uniqueness_required():
    with pytest.raises(ValueError, match="must be unique"):
        Pool(nodes=2, node_labels=["head", "head"])


def test_pool_node_labels_invalid_characters_rejected():
    with pytest.raises(ValueError, match="must not contain"):
        Pool(nodes=1, node_labels=["node.01"])
    with pytest.raises(ValueError, match="must not contain"):
        Pool(nodes=1, node_labels=["node[0]"])


def test_pool_node_labels_empty_string_rejected():
    with pytest.raises(ValueError, match="must be non-empty"):
        Pool(nodes=1, node_labels=[""])


def test_pool_nodes_must_be_positive():
    with pytest.raises(ValueError, match="nodes must be >= 1"):
        Pool(nodes=0)


def test_pool_cpus_per_node_positive():
    with pytest.raises(ValueError, match="cpus_per_node"):
        Pool(nodes=1, cpus_per_node=0)


def test_pool_gpu_type_and_gres_gpu_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Pool(nodes=1, gpus_per_node=8, gpu_type="h100", gres={"gpu": "h100:8"})


def test_pool_collections_are_frozen_from_input():
    labels = ["a", "b"]
    p = Pool(nodes=2, node_labels=labels)
    labels.append("c")  # mutate original; shouldn't leak into Pool
    assert p.node_labels == ("a", "b")

    gres_in = {"gpu": "h100:8"}
    p2 = Pool(nodes=1, gpus_per_node=8, gres=gres_in)
    gres_in["extra"] = "x"
    assert "extra" not in p2.gres


def test_pool_frozen():
    p = Pool(nodes=1)
    with pytest.raises(Exception):  # dataclass frozen raises FrozenInstanceError
        p.nodes = 2  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Peer field validation
# ---------------------------------------------------------------------------


def test_peer_singleton_defaults():
    p = Peer(_train)
    assert p.resolved_name == "_train"
    assert p.leader is False
    assert p.on_failure == "kill"
    assert p.count == 1
    assert p.is_replica_set is False


def test_peer_coerces_slurm_task_to_bound_task():
    p = Peer(_train)
    assert isinstance(p.task, BoundTask)
    assert p.task.task is _train


def test_peer_accepts_bound_task():
    bound = _train.partial(cfg={"lr": 0.001})
    p = Peer(bound)
    assert p.task is bound


def test_peer_rejects_raw_callable():
    with pytest.raises(TypeError, match="must be a SlurmTask"):
        Peer(task=lambda x: x)  # type: ignore[arg-type]


def test_peer_explicit_name_wins():
    p = Peer(_train, name="my_leader")
    assert p.resolved_name == "my_leader"


def test_peer_on_failure_values():
    for policy in ("kill", "continue"):
        Peer(_train, on_failure=policy)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="on_failure"):
        Peer(_train, on_failure="explode")  # type: ignore[arg-type]


def test_peer_leader_continue_rejected():
    with pytest.raises(ValueError, match="leader=True"):
        Peer(_train, leader=True, on_failure="continue")


def test_peer_count_must_be_positive():
    with pytest.raises(ValueError, match="count must be >= 1"):
        Peer.replicas(_sim, count=0)


def test_peer_singleton_cannot_set_args():
    with pytest.raises(ValueError, match="args="):
        Peer(_train, args=[1, 2, 3])  # type: ignore[arg-type]


def test_peer_singleton_cannot_set_on_nodes():
    with pytest.raises(ValueError, match="on_nodes"):
        Peer(_train, on_nodes=["head"])


def test_peer_singleton_cannot_set_tasks_per_node():
    with pytest.raises(ValueError, match="tasks_per_node"):
        Peer(_train, tasks_per_node=2)


def test_peer_replicas_on_nodes_length_check():
    with pytest.raises(ValueError, match="on_nodes length"):
        Peer.replicas(_sim, count=4, on_nodes=["a", "b"])


def test_peer_replicas_factory():
    p = Peer.replicas(_sim, count=3, args=[{"env_id": i} for i in range(3)])
    assert p.count == 3
    assert p.is_replica_set is True
    assert p.leader is False
    assert p.args == [{"env_id": 0}, {"env_id": 1}, {"env_id": 2}]


def test_peer_frozen():
    p = Peer(_train)
    with pytest.raises(Exception):
        p.leader = True  # type: ignore[misc]


def test_peer_on_node_plus_on_nodes_rejected():
    # Direct Peer(...) construction exercises the mutual-exclusion check.
    # Peer.replicas(...) never sets on_node, so this path can only be hit via
    # __init__ directly.
    with pytest.raises(ValueError, match="not both"):
        Peer(
            task=_train,
            count=2,
            on_node="head",
            on_nodes=("head", "ops"),
        )


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------


def test_topology_requires_at_least_one_pool():
    with pytest.raises(ValueError, match="at least one pool"):
        Topology(pools={})


def test_topology_auto_resolves_default_pool_when_single():
    t = Topology(pools={"main": Pool(nodes=1)})
    assert t.default_pool == "main"


def test_topology_multi_pool_requires_explicit_default_or_per_peer():
    # Not an error at Topology construction; the spec builder only errors
    # if a peer actually omits pool=.
    t = Topology(pools={"gpu": Pool(nodes=1, gpus_per_node=4), "cpu": Pool(nodes=2)})
    assert t.default_pool is None


def test_topology_default_pool_must_exist():
    with pytest.raises(ValueError, match="default_pool"):
        Topology(
            pools={"gpu": Pool(nodes=1)},
            default_pool="cpu",
        )


def test_topology_pools_must_be_pools():
    with pytest.raises(TypeError, match="must be a Pool"):
        Topology(pools={"bogus": "not a pool"})  # type: ignore[dict-item]


def test_topology_pool_keys_must_be_non_empty_strings():
    with pytest.raises(ValueError, match="non-empty string"):
        Topology(pools={"": Pool(nodes=1)})


def test_topology_detaches_pools_mapping_from_caller():
    pools = {"a": Pool(nodes=1)}
    t = Topology(pools=pools)
    pools["b"] = Pool(nodes=2)  # should not leak into t
    assert "b" not in t.pools


# ---------------------------------------------------------------------------
# _ParallelSpec helpers
# ---------------------------------------------------------------------------


def test_spec_peer_by_name_and_leaders():
    leader = Peer(_train, name="leader", leader=True)
    helper = Peer(_monitor, name="helper", on_failure="continue")
    spec = _ParallelSpec(
        peers=(leader, helper),
        topology=Topology(pools={"p": Pool(nodes=1)}, default_pool="p"),
    )
    assert spec.peer_by_name("leader") is leader
    assert spec.peer_by_name("helper") is helper
    with pytest.raises(KeyError):
        spec.peer_by_name("nonexistent")
    assert spec.leaders() == (leader,)
