"""Unit tests for :func:`slurm.parallel.placement.resolve_placement`.

The resolver takes a validated spec plus a ``{pool: hostnames}`` mapping
and returns the hostnames each peer should pin to. These tests cover the
declarative shapes from the design doc §6.3 / §6.4:

- ``on_node="<label>"`` / ``on_node=<ordinal>``.
- ``on_nodes=[...]`` on replica sets.
- ``colocate_with`` chains (pinned target, unpinned target, multi-hop).
- Mixed pinned + unpinned peers with a free-list.

The tests don't go through ``parallel(...)`` — they build ``_ParallelSpec``
directly — so the signal stays narrow.
"""

from __future__ import annotations

from typing import Optional

import pytest

from slurm import task
from slurm.errors import TopologyError
from slurm.parallel.placement import resolve_placement
from slurm.parallel.types import Peer, Pool, Topology, _ParallelSpec


@task(time="00:05:00")
def _noop() -> None:  # placeholder task body
    return None


@task(time="00:05:00")
def _noop2() -> None:
    return None


@task(time="00:05:00")
def _noop3() -> None:
    return None


def _spec(peers: list[Peer], pools: dict[str, Pool]) -> _ParallelSpec:
    """Build a _ParallelSpec without going through the validator.

    These tests rely on the spec already being well-formed because the
    resolver assumes validation ran. A dedicated test below exercises the
    resolver's defensive checks for cases that would slip past validation.
    """
    default_pool: Optional[str] = None
    if len(pools) == 1:
        default_pool = next(iter(pools))
    topology = Topology(pools=pools, default_pool=default_pool)
    # Fill peer.pool from default_pool when unset, so spec peers are
    # canonical for the resolver.
    resolved = []
    for p in peers:
        if p.pool is None:
            resolved.append(
                Peer(
                    task=p.task,
                    pool=default_pool,
                    name=p.name,
                    leader=p.leader,
                    on_failure=p.on_failure,
                    max_restarts=p.max_restarts,
                    callback=p.callback,
                    exclusive=p.exclusive,
                    on_node=p.on_node,
                    colocate_with=p.colocate_with,
                    srun_args=p.srun_args,
                    announce=p.announce,
                    count=p.count,
                    args=p.args,
                    tasks_per_node=p.tasks_per_node,
                    on_nodes=p.on_nodes,
                )
            )
        else:
            resolved.append(p)
    return _ParallelSpec(peers=tuple(resolved), topology=topology)


def test_on_node_label_resolves_to_correct_hostname():
    pool = Pool(nodes=2, node_labels=("head", "ops"))
    peer = Peer(task=_noop, name="primary", on_node="head")
    spec = _spec([peer], {"gpu": pool})
    placement = resolve_placement(spec, {"gpu": ("gpu-01", "gpu-02")})
    assert placement.for_peer("primary") == ("gpu-01",)


def test_on_node_ordinal_resolves_to_correct_hostname():
    pool = Pool(nodes=3)
    peer = Peer(task=_noop, name="worker", on_node=2)
    spec = _spec([peer], {"cpu": pool})
    placement = resolve_placement(spec, {"cpu": ("n-01", "n-02", "n-03")})
    assert placement.for_peer("worker") == ("n-03",)


def test_on_nodes_per_replica_pinning():
    pool = Pool(nodes=3, node_labels=("a", "b", "c"))
    peer = Peer.replicas(_noop, count=3, name="workers", on_nodes=["a", "c", "b"])
    spec = _spec([peer], {"pool": pool})
    placement = resolve_placement(spec, {"pool": ("host-a", "host-b", "host-c")})
    assert placement.for_peer("workers") == ("host-a", "host-c", "host-b")


def test_colocate_with_pinned_target_inherits_pin():
    pool = Pool(nodes=2, node_labels=("head", "ops"))
    primary = Peer(task=_noop, name="primary", on_node="head")
    sidecar = Peer(task=_noop2, name="sidecar", colocate_with="primary")
    spec = _spec([primary, sidecar], {"gpu": pool})
    placement = resolve_placement(spec, {"gpu": ("gpu-01", "gpu-02")})
    assert placement.for_peer("primary") == ("gpu-01",)
    assert placement.for_peer("sidecar") == ("gpu-01",)


def test_colocate_with_unpinned_target_assigns_first_free_node():
    # Neither peer has an explicit pin. The colocator still wants to land
    # on the same host as primary, so the resolver must pin primary to
    # the first unused node and share that pin with sidecar.
    pool = Pool(nodes=2)
    primary = Peer(task=_noop, name="primary")
    sidecar = Peer(task=_noop2, name="sidecar", colocate_with="primary")
    spec = _spec([primary, sidecar], {"p": pool})
    placement = resolve_placement(spec, {"p": ("a", "b")})
    assert placement.for_peer("primary") == ("a",)
    assert placement.for_peer("sidecar") == ("a",)


def test_chain_of_three_colocates_share_one_node():
    pool = Pool(nodes=3, node_labels=("head", "x", "y"))
    a = Peer(task=_noop, name="a", on_node="head")
    b = Peer(task=_noop2, name="b", colocate_with="a")
    c = Peer(task=_noop3, name="c", colocate_with="b")
    spec = _spec([a, b, c], {"p": pool})
    placement = resolve_placement(spec, {"p": ("h1", "h2", "h3")})
    assert placement.for_peer("a") == ("h1",)
    assert placement.for_peer("b") == ("h1",)
    assert placement.for_peer("c") == ("h1",)


def test_mixed_pinned_and_unpinned_peers_free_list_handled():
    # 3 hosts; peer A pins to head (host 0). Peer B has no intent — it
    # should be left unpinned (absent from placement map), so Slurm can
    # place freely.
    pool = Pool(nodes=3, node_labels=("head", "x", "y"))
    a = Peer(task=_noop, name="a", on_node="head")
    b = Peer(task=_noop2, name="b")
    spec = _spec([a, b], {"p": pool})
    placement = resolve_placement(spec, {"p": ("h0", "h1", "h2")})
    assert placement.for_peer("a") == ("h0",)
    # Unpinned peers are absent from the map.
    assert placement.for_peer("b") is None
    assert not placement.is_pinned("b")


def test_colocate_after_pinned_uses_pinned_host_not_freelist():
    # A pinned peer takes its host, then a colocator with an unpinned
    # primary finds the first *unclaimed* host, not the pinned one.
    pool = Pool(nodes=3, node_labels=("head", "x", "y"))
    pinned = Peer(task=_noop, name="pinned", on_node="head")
    primary = Peer(task=_noop2, name="primary")
    sidecar = Peer(task=_noop3, name="sidecar", colocate_with="primary")
    spec = _spec([pinned, primary, sidecar], {"p": pool})
    placement = resolve_placement(spec, {"p": ("h0", "h1", "h2")})
    assert placement.for_peer("pinned") == ("h0",)
    # primary + sidecar land on the first unclaimed host (h1).
    assert placement.for_peer("primary") == ("h1",)
    assert placement.for_peer("sidecar") == ("h1",)


def test_unknown_label_raises_topology_error():
    # The validator should have caught this; defensive test ensures the
    # resolver's error path does raise rather than crash obscurely.
    pool = Pool(nodes=1, node_labels=("head",))
    peer = Peer(task=_noop, name="x", on_node="head")
    spec = _spec([peer], {"p": pool})
    # Force a mismatch by giving a bogus on_node. The Phase 1 validator
    # would reject this at the public entry point; here we build the spec
    # directly to exercise the resolver's defensive error path.
    bad = Peer(task=_noop, name="broken", pool="p", on_node="nope")
    bad_spec = _ParallelSpec(
        peers=(bad,),
        topology=Topology(
            pools={"p": Pool(nodes=1, node_labels=("head",))}, default_pool="p"
        ),
    )
    with pytest.raises(TopologyError, match="unknown label"):
        resolve_placement(bad_spec, {"p": ("h0",)})
    # The well-formed spec above keeps resolving fine.
    assert resolve_placement(spec, {"p": ("h0",)}).for_peer("x") == ("h0",)


def test_ordinal_out_of_range_raises_topology_error():
    pool = Pool(nodes=2)
    peer = Peer(task=_noop, name="x", on_node=5)
    spec = _ParallelSpec(
        peers=(peer,),
        topology=Topology(pools={"p": pool}, default_pool="p"),
    )
    with pytest.raises(TopologyError, match="on_node=5"):
        resolve_placement(spec, {"p": ("a", "b")})


def test_placement_map_for_peer_returns_none_for_missing():
    pool = Pool(nodes=1)
    peer = Peer(task=_noop, name="unpinned")
    spec = _spec([peer], {"p": pool})
    placement = resolve_placement(spec, {"p": ("host",)})
    assert placement.for_peer("unpinned") is None
    assert placement.for_peer("does-not-exist") is None


def test_replica_set_colocating_with_singleton_broadcasts_pin():
    # Design §6.4: replica sets colocating with a singleton target share
    # the target's single host across all replicas (Slurm schedules tasks
    # inside the step).
    pool = Pool(nodes=2, node_labels=("head", "aux"))
    primary = Peer(task=_noop, name="primary", on_node="head")
    workers = Peer.replicas(_noop2, count=4, name="workers", colocate_with="primary")
    spec = _spec([primary, workers], {"p": pool})
    placement = resolve_placement(spec, {"p": ("h0", "h1")})
    assert placement.for_peer("primary") == ("h0",)
    assert placement.for_peer("workers") == ("h0", "h0", "h0", "h0")


def test_empty_pool_hostnames_leaves_peer_unpinned():
    # When the allocation produces no hostnames (test env without SLURM
    # vars), the resolver falls back to "unpinned" for unresolvable peers
    # rather than crashing.
    pool = Pool(nodes=1)
    peer = Peer(task=_noop, name="maybe", colocate_with="x")
    # Need a second peer for colocate to reference.
    x = Peer(task=_noop2, name="x")
    spec = _spec([x, peer], {"p": pool})
    placement = resolve_placement(spec, {"p": ()})
    # With no hostnames at all, x gets no pin (empty free list) and the
    # colocator can't inherit either.
    assert placement.for_peer("x") is None
    assert placement.for_peer("maybe") is None
