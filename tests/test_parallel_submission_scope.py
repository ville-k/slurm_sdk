"""Scope-boundary tests — ensure deferred features raise NotImplementedError.

Phase 2 supports single-pool, single-replica peers with kill/continue failure
policies only. Anything else must raise :class:`NotImplementedError` with a
message that points at the phase that delivers the missing piece. These
tests lock that contract in so callers who hit a deferred feature get a
clear next step.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from slurm import Peer, Pool, Topology, task
from slurm._parallel_submission import _check_phase2_scope
from slurm.parallel import _build_spec
from slurm.parallel.validation import validate_spec


@task(cpus_per_task=1, mem="1G")
def _a(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, mem="1G")
def _b(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, mem="1G", packaging="wheel")
def _wheel_task() -> None:
    pass


@task(cpus_per_task=1, mem="1G", packaging="none")
def _none_task() -> None:
    pass


def _spec(*peers, topology=None):
    spec = _build_spec(
        positional=tuple(peers),
        named={},
        topology=topology,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    validate_spec(spec)
    return spec


def test_multi_pool_passes_scope_check():
    # Phase 6 enables multi-pool / hetjob submissions — the scope gate must
    # accept them.
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=1, cpus_per_node=1),
        }
    )
    spec = _spec(
        Peer(_a.partial(cfg={}), pool="gpu"),
        Peer(_b.partial(cfg={}), pool="cpu"),
        topology=topo,
    )
    # Does not raise.
    _check_phase2_scope(spec)


def test_replica_set_passes_scope_check():
    # Phase 5 enables replica sets — the scope gate must accept them.
    spec = _spec(Peer.replicas(_a.partial(cfg={}), count=4))
    _check_phase2_scope(spec)


def test_restart_policy_passes_scope_check():
    # Phase 4 enables restart/callback — these must pass the scope gate now.
    spec = _spec(
        Peer(_a.partial(cfg={}), on_failure="restart", max_restarts=2),
    )
    _check_phase2_scope(spec)


def _module_level_callback(snap: Any) -> str:
    return "kill"


def test_callback_policy_passes_scope_check():
    spec = _spec(
        Peer(
            _a.partial(cfg={}),
            on_failure="callback",
            callback=_module_level_callback,
        ),
    )
    _check_phase2_scope(spec)


def test_heterogeneous_packaging_raises_not_implemented():
    # _wheel_task and _none_task have different packaging dicts — Phase 2
    # requires a shared config.
    spec = _spec(
        Peer(_wheel_task),
        Peer(_none_task),
    )
    with pytest.raises(NotImplementedError, match="Phase 10"):
        _check_phase2_scope(spec)


def test_on_node_placement_raises_not_implemented():
    topo = Topology(
        pools={
            "default": Pool(nodes=2, cpus_per_node=4, node_labels=["h", "w"]),
        }
    )
    spec = _spec(
        Peer(_a.partial(cfg={}), on_node="h"),
        Peer(_b.partial(cfg={}), on_node="w"),
        topology=topo,
    )
    with pytest.raises(NotImplementedError, match="Phase 8"):
        _check_phase2_scope(spec)


def test_homogeneous_packaging_passes_scope_check():
    # Two peers, both default packaging → scope check succeeds.
    spec = _spec(
        Peer(_a.partial(cfg={}), leader=True),
        Peer(_b.partial(cfg={}), on_failure="continue"),
    )
    # Does not raise.
    _check_phase2_scope(spec)


# Silence unused-imports the helpers above pull in for typing only.
_unused: List[Dict[str, Any]] = []
