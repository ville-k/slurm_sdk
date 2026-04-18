"""Unit tests for parallel() spec validation.

These tests go through the public ``parallel(...)`` entry point where possible
— catching the ``NotImplementedError`` that guards Phase 1's unwired
submission path — and fall back to the lower-level ``_build_spec`` +
``validate_spec`` helpers for cases that can't be exercised via
``parallel(...)`` alone.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from slurm import Peer, Pool, Topology, task
from slurm.errors import TopologyError
from slurm.parallel import _build_spec, parallel
from slurm.parallel.validation import validate_spec


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


@task(cpus_per_task=8, gpus_per_task=2)
def _inference(worker_id: int) -> None:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_parallel(*peers, **kwargs):
    """Call parallel() and return the TopologyError raised (if any).

    ``parallel()`` runs validation first, then resolves the active Cluster,
    then submits. A ``TopologyError`` means validation caught something;
    anything later (``RuntimeError`` for missing cluster context,
    ``NotImplementedError`` for scope boundaries) means validation was clean.
    """
    try:
        parallel(*peers, **kwargs)
    except TopologyError as e:
        return e
    except (NotImplementedError, RuntimeError):
        return None
    raise AssertionError("parallel() returned instead of raising")


def _build(*peers, topology=None, **kwargs):
    """Build a spec without calling validate_spec (for driving the validator
    directly with hand-crafted malformed specs)."""
    return _build_spec(
        positional=tuple(peers),
        named=kwargs.pop("named", {}),
        topology=topology,
        time=kwargs.pop("time", None),
        account=kwargs.pop("account", None),
        qos=kwargs.pop("qos", None),
        reservation=kwargs.pop("reservation", None),
        network=kwargs.pop("network", None),
        grace_period_seconds=kwargs.pop("grace_period_seconds", 10),
    )


@contextmanager
def _expect_topology_error(*substrings: str):
    with pytest.raises(TopologyError) as exc_info:
        yield
    msg = str(exc_info.value)
    for sub in substrings:
        assert sub in msg, f"expected {sub!r} in error message: {msg}"


# ---------------------------------------------------------------------------
# Successful validation paths
# ---------------------------------------------------------------------------


def test_simple_two_peer_validates():
    err = _run_parallel(_train.partial(cfg={"lr": 0.001}), _monitor)
    assert err is None  # no TopologyError = validation passed


def test_leader_sidecar_validates():
    err = _run_parallel(
        Peer(_train.partial(cfg={"lr": 0.001}), leader=True),
        Peer(_monitor, on_failure="continue"),
    )
    assert err is None


def test_explicit_topology_validates():
    top = Topology(pools={"main": Pool(nodes=1, cpus_per_node=32)})
    err = _run_parallel(
        Peer(_train.partial(cfg={"lr": 0.001}), pool="main"),
        Peer(_monitor, pool="main"),
        topology=top,
    )
    assert err is None


def test_named_peer_keyword_wins():
    # parallel(my_name=peer) should make resolved_name == 'my_name'
    spec = _build(named={"leader": _train.partial(cfg={"lr": 0.001})})
    validate_spec(spec)
    assert spec.peers[0].resolved_name == "leader"


# ---------------------------------------------------------------------------
# Name uniqueness
# ---------------------------------------------------------------------------


def test_duplicate_peer_names_rejected():
    with _expect_topology_error("unique name", "'_train'"):
        parallel(
            _train.partial(cfg={"lr": 0.001}),
            _train.partial(cfg={"lr": 0.01}),
        )


def test_duplicate_names_via_explicit_name_rejected():
    with _expect_topology_error("unique name"):
        parallel(
            Peer(_train, name="x"),
            Peer(_monitor, name="x"),
        )


# ---------------------------------------------------------------------------
# Pool references
# ---------------------------------------------------------------------------


def test_peer_references_unknown_pool():
    top = Topology(pools={"main": Pool(nodes=1)})
    with _expect_topology_error("unknown pool", "'bogus'"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), pool="bogus"),
            topology=top,
        )


def test_multi_pool_missing_default_and_explicit_pool():
    top = Topology(pools={"gpu": Pool(nodes=1, gpus_per_node=4), "cpu": Pool(nodes=2)})
    with _expect_topology_error("no pool and no default pool"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001})),  # no pool= and no default
            Peer(_monitor, pool="cpu"),
            topology=top,
        )


# ---------------------------------------------------------------------------
# Replica args length
# ---------------------------------------------------------------------------


def test_replica_args_length_mismatch_list():
    with _expect_topology_error("len 2", "count=4"):
        parallel(
            Peer.replicas(_sim, count=4, args=[{"env_id": 0}, {"env_id": 1}]),
        )


def test_replica_args_length_mismatch_range():
    with _expect_topology_error("range", "len 3", "count=5"):
        parallel(
            Peer.replicas(_sim, count=5, args=range(3)),
        )


def test_replica_args_callable_is_skipped():
    err = _run_parallel(
        Peer.replicas(_sim, count=4, args=lambda i: {"env_id": i}),
    )
    assert err is None


def test_replica_args_none_is_fine():
    err = _run_parallel(
        Peer.replicas(_sim, count=4),  # args=None; all replicas get identical args
    )
    assert err is None


# ---------------------------------------------------------------------------
# on_node / on_nodes
# ---------------------------------------------------------------------------


def test_on_node_ordinal_out_of_range():
    top = Topology(pools={"main": Pool(nodes=2)})
    with _expect_topology_error("ordinal 5", "2 node"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), pool="main", on_node=5),
            topology=top,
        )


def test_on_node_label_without_pool_labels():
    top = Topology(pools={"main": Pool(nodes=2)})
    with _expect_topology_error("has no", "node_labels"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), pool="main", on_node="head"),
            topology=top,
        )


def test_on_node_unknown_label():
    top = Topology(pools={"main": Pool(nodes=2, node_labels=["head", "ops"])})
    with _expect_topology_error("unknown label", "'spare'"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), pool="main", on_node="spare"),
            topology=top,
        )


def test_on_node_label_resolves():
    top = Topology(pools={"main": Pool(nodes=2, node_labels=["head", "ops"])})
    err = _run_parallel(
        Peer(_train.partial(cfg={"lr": 0.001}), pool="main", on_node="head"),
        topology=top,
    )
    assert err is None


def test_on_nodes_length_matches_count():
    top = Topology(
        pools={"main": Pool(nodes=4, gpus_per_node=1, node_labels=["a", "b", "c", "d"])}
    )
    err = _run_parallel(
        Peer.replicas(_sim, count=4, pool="main", on_nodes=["a", "b", "c", "d"]),
        topology=top,
    )
    assert err is None


def test_on_nodes_individual_ref_validated():
    top = Topology(
        pools={"main": Pool(nodes=4, gpus_per_node=1, node_labels=["a", "b", "c", "d"])}
    )
    with _expect_topology_error("unknown label", "'zzz'"):
        parallel(
            Peer.replicas(_sim, count=4, pool="main", on_nodes=["a", "b", "zzz", "d"]),
            topology=top,
        )


# ---------------------------------------------------------------------------
# colocate_with
# ---------------------------------------------------------------------------


def test_colocate_with_unknown_target():
    with _expect_topology_error("colocate_with", "'ghost'"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), name="train"),
            Peer(_monitor, name="mon", colocate_with="ghost"),
        )


def test_colocate_with_across_pools_rejected():
    top = Topology(pools={"gpu": Pool(nodes=1, gpus_per_node=4), "cpu": Pool(nodes=1)})
    with _expect_topology_error("different pools"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), name="train", pool="gpu"),
            Peer(_monitor, name="mon", pool="cpu", colocate_with="train"),
            topology=top,
        )


def test_colocate_with_two_peers_same_pool_ok():
    top = Topology(pools={"main": Pool(nodes=1, cpus_per_node=32)})
    err = _run_parallel(
        Peer(_train.partial(cfg={"lr": 0.001}), name="train", pool="main"),
        Peer(_monitor, name="mon", pool="main", colocate_with="train"),
        topology=top,
    )
    assert err is None


def test_colocate_with_cycle_detected():
    with _expect_topology_error("Cycle in colocate_with"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), name="a", colocate_with="b"),
            Peer(_monitor, name="b", colocate_with="c"),
            Peer(_sim.partial(env_id=0), name="c", colocate_with="a"),
        )


def test_colocate_with_self_cycle_detected():
    with _expect_topology_error("Cycle in colocate_with"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), name="a", colocate_with="a"),
        )


# ---------------------------------------------------------------------------
# Capacity
# ---------------------------------------------------------------------------


def test_pool_gpu_overflow_reported():
    top = Topology(pools={"gpu": Pool(nodes=1, gpus_per_node=4, cpus_per_node=64)})
    # 4 replicas × 2 gpus_per_task = 8 per node, but pool only has 4 gpus/node.
    with _expect_topology_error("GPU overflow", "gpu", "demand"):
        parallel(
            Peer.replicas(
                _inference, count=4, pool="gpu", args=lambda i: {"worker_id": i}
            ),
            topology=top,
        )


def test_gpu_peer_in_cpu_only_pool_flagged():
    """Phase 6: peer requesting GPUs in a CPU-only pool should be flagged as 'wrong pool'."""
    top = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=8),
            "cpu": Pool(nodes=1, cpus_per_node=32),
        }
    )
    with _expect_topology_error("wrong pool", "'cpu'", "Pools with GPUs"):
        parallel(
            Peer(_inference, pool="cpu"),
            topology=top,
        )


def test_gpu_peer_in_correct_pool_accepted():
    top = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=4, cpus_per_node=32),
            "cpu": Pool(nodes=1, cpus_per_node=32),
        }
    )
    # inference needs gpus_per_task=2 — placing it in gpu pool fits.
    err = _run_parallel(
        Peer(_inference, pool="gpu"),
        topology=top,
    )
    assert err is None


def test_pool_cpu_overflow_reported():
    top = Topology(pools={"cpu": Pool(nodes=1, cpus_per_node=8)})
    # train(cpus_per_task=4) + monitor(cpus_per_task=2) + sim(cpus_per_task=1)
    # = 7 CPUs per node, fits. Now add three more sim replicas sharing the node.
    with _expect_topology_error("CPU overflow"):
        parallel(
            Peer.replicas(
                _inference, count=2, pool="cpu", args=lambda i: {"worker_id": i}
            ),  # 8*2 = 16 CPUs
            topology=top,
        )


def test_capacity_partial_claims_not_checked():
    # Pool declares capacity but no peer declares claims → no overflow.
    @task()  # no cpus_per_task / gpus_per_task
    def _nop():
        pass

    top = Topology(pools={"main": Pool(nodes=1, cpus_per_node=4)})
    err = _run_parallel(
        Peer(_nop, pool="main"),
        topology=top,
    )
    assert err is None


def test_capacity_fits_exactly():
    top = Topology(pools={"gpu": Pool(nodes=1, gpus_per_node=8, cpus_per_node=32)})
    err = _run_parallel(
        Peer.replicas(_inference, count=4, pool="gpu", args=lambda i: {"worker_id": i}),
        # count=4, cpus_per_task=8 × 4 = 32, gpus_per_task=2 × 4 = 8 → fits
        topology=top,
    )
    assert err is None


# ---------------------------------------------------------------------------
# announce
# ---------------------------------------------------------------------------


def test_announce_reserved_key_rejected():
    with _expect_topology_error("reserved key", "'hostname'"):
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), announce={"hostname": "foo"}),
        )


def test_announce_non_reserved_key_ok():
    err = _run_parallel(
        Peer(_train.partial(cfg={"lr": 0.001}), announce={"model_version": "r3"}),
    )
    assert err is None


# ---------------------------------------------------------------------------
# Outer options
# ---------------------------------------------------------------------------


def test_grace_period_non_negative():
    with _expect_topology_error("grace_period"):
        parallel(
            _train.partial(cfg={"lr": 0.001}),
            grace_period_seconds=-1,
        )


# ---------------------------------------------------------------------------
# Aggregation: multiple errors reported at once
# ---------------------------------------------------------------------------


def test_multiple_problems_aggregated():
    top = Topology(pools={"main": Pool(nodes=1)})
    with pytest.raises(TopologyError) as exc_info:
        parallel(
            Peer(_train.partial(cfg={"lr": 0.001}), pool="bogus", name="a"),
            Peer(_monitor, pool="main", name="a"),  # duplicate name + unknown pool
            topology=top,
        )
    err = exc_info.value
    # Expect at least 2 problems reported
    assert len(err.problems) >= 2
    assert "problem(s)" in err.message


# ---------------------------------------------------------------------------
# Empty parallel()
# ---------------------------------------------------------------------------


def test_parallel_requires_at_least_one_peer():
    with pytest.raises(TopologyError, match="at least one peer"):
        parallel()


# ---------------------------------------------------------------------------
# Callback resolvability (Phase 4)
# ---------------------------------------------------------------------------


def _module_level_cb(snap):  # noqa: ARG001
    return "kill"


def test_lambda_callback_is_rejected():
    spec = _build_spec(
        positional=(
            Peer(
                _train.partial(cfg={}), on_failure="callback", callback=lambda s: "kill"
            ),
        ),
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    with pytest.raises(TopologyError, match="lambda or nested function"):
        validate_spec(spec)


def test_nested_function_callback_is_rejected():
    def _nested(snap):  # noqa: ARG001
        return "kill"

    spec = _build_spec(
        positional=(
            Peer(_train.partial(cfg={}), on_failure="callback", callback=_nested),
        ),
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    with pytest.raises(TopologyError, match="lambda or nested function"):
        validate_spec(spec)


def test_module_level_callback_is_accepted():
    spec = _build_spec(
        positional=(
            Peer(
                _train.partial(cfg={}),
                on_failure="callback",
                callback=_module_level_cb,
            ),
        ),
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    # Must not raise.
    validate_spec(spec)
