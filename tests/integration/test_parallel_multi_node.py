"""Multi-node end-to-end integration tests for ``parallel(...)``.

These tests require a cluster with **two or more IDLE nodes**. The
session-scoped ``multi_node_cluster`` fixture in
``tests/integration/conftest.py`` skips cleanly when only one node is
available (the default single-node dev setup), so developers running
``docker compose up -d slurm registry`` don't see noise from failing
multi-node tests.

Coverage this file closes — behaviours that trivially passed on one node
because every peer landed on the same hostname:

1. ``parallel(...)`` hetjob submission where each component demands its
   own node (``Pool(nodes=1, exclusive=True)``) and both complete rather
   than blocking on ``Reason=Resources``.
2. ``wait_all([...])`` on an announced endpoint across a real node
   boundary — the observed ``coord.hostname`` is not the same as the
   worker's own ``ctx.node.hostname``.
3. Named-node pinning via ``Pool(node_labels=...)`` + ``Peer(on_node=)``
   exercises the bootstrap's ``scontrol show hostnames`` expansion
   against a real multi-node allocation.
4. ``Peer(colocate_with=)`` with three peers on a two-node allocation —
   the colocated peers share a hostname, the floating peer may differ.
5. Multi-node ``srun`` step — a replica pair with ``tasks_per_node=1``
   forces Slurm to reserve two nodes; every replica observes the same
   ``SLURM_JOB_NUM_NODES=2`` and a ``$SLURM_JOB_NODELIST`` enumerating
   both cluster hostnames.
"""

from __future__ import annotations

import pytest

from slurm import Peer, Pool, Topology, parallel
from slurm.examples.integration_test_task import (
    parallel_coordinator_task,
    parallel_cross_node_worker_task,
    parallel_hostname_identity_task,
    parallel_multi_node_task,
)


# ---------------------------------------------------------------------------
# 1. Two-pool hetjob — both components actually schedule on distinct nodes.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_two_pool_hetjob_full_scheduling(slurm_cluster, multi_node_cluster):
    """Two-pool hetjob completes — components land on distinct nodes.

    The single-node suite can only assert sbatch accepts the hetjob
    header; the second component blocks on ``Reason=Resources`` forever.
    With 2+ nodes, ``Pool(nodes=1, exclusive=True)`` on each component
    forces Slurm to pick two distinct nodes for the allocation.
    """
    topo = Topology(
        pools={
            "alpha": Pool(nodes=1, partition="debug", exclusive=True),
            "beta": Pool(nodes=1, partition="debug", exclusive=True),
        },
    )
    with slurm_cluster:
        job = parallel(
            Peer(parallel_hostname_identity_task, pool="alpha", name="alpha"),
            Peer(parallel_hostname_identity_task, pool="beta", name="beta"),
            topology=topo,
        )
        assert job.wait(timeout=300)
        results = job.get_results()

    alpha_host = results["alpha"]["hostname"]
    beta_host = results["beta"]["hostname"]
    assert alpha_host and beta_host
    assert alpha_host != beta_host, (
        f"hetjob components shared a node ({alpha_host!r}); expected distinct nodes"
    )


# ---------------------------------------------------------------------------
# 2. Cross-node service discovery — wait_all sees an announce from another node.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_cross_node_service_discovery(slurm_cluster, multi_node_cluster):
    """Worker's observed ``coord.hostname`` is not its own hostname.

    Proves ``wait_all(keys=["endpoint"])`` reads the registry across a
    real node boundary and ``coord.hostname`` reflects the coordinator's
    actual Slurm-populated hostname, not a synthesized local one.
    """
    pool = Pool(
        nodes=2,
        partition="debug",
        node_labels=("head", "worker"),
    )
    topo = Topology(pools={"default": pool})
    with slurm_cluster:
        job = parallel(
            Peer(parallel_coordinator_task, name="coordinator", on_node="head"),
            Peer(parallel_cross_node_worker_task, name="worker", on_node="worker"),
            topology=topo,
        )
        assert job.wait(timeout=300)
        results = job.get_results()

    worker = results["worker"]
    assert worker["observed_coord_hostname"], "coordinator hostname still empty"
    assert worker["observed_coord_hostname"] != worker["worker_hostname"], (
        "coordinator + worker landed on the same host — allocation did not span 2 nodes: "
        f"coord={worker['observed_coord_hostname']!r}, worker={worker['worker_hostname']!r}"
    )
    assert worker["observed_coord_endpoint"], "announced endpoint never observed"


# ---------------------------------------------------------------------------
# 3. Named-node pinning — peers land on the nodes their labels resolve to.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_named_nodes_pin_per_role(slurm_cluster, multi_node_cluster):
    """``Peer(on_node="label")`` lands on the hostname the label resolved to.

    Exercises the bootstrap's ``scontrol show hostnames`` path — the
    ``node_labels`` map to specific hostnames in allocation order and each
    peer's ``ctx.node.hostname`` matches its own ``socket.gethostname()``
    (i.e. it actually ran on the node it claimed to).
    """
    pool = Pool(
        nodes=2,
        partition="debug",
        node_labels=("head", "worker"),
    )
    topo = Topology(pools={"default": pool})
    with slurm_cluster:
        job = parallel(
            Peer(parallel_hostname_identity_task, name="head_peer", on_node="head"),
            Peer(parallel_hostname_identity_task, name="worker_peer", on_node="worker"),
            topology=topo,
        )
        assert job.wait(timeout=300)
        results = job.get_results()

    head = results["head_peer"]
    worker = results["worker_peer"]
    # Each peer's ctx.node.hostname agrees with socket.gethostname(): the
    # registry's published node identity matches the physical host the
    # runner actually executed on.
    assert head["hostname"] == head["node_hostname"]
    assert worker["hostname"] == worker["node_hostname"]
    # Distinct labels resolved to distinct hostnames.
    assert head["hostname"] != worker["hostname"], (
        f"head and worker pinned to same host {head['hostname']!r}"
    )


# ---------------------------------------------------------------------------
# 4. colocate_with — two peers share a node, third may float.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_colocate_with_shares_host(slurm_cluster, multi_node_cluster):
    """``Peer(colocate_with="A")`` ends up on the same host as peer A."""
    pool = Pool(
        nodes=2,
        partition="debug",
        node_labels=("head", "worker"),
    )
    topo = Topology(pools={"default": pool})
    with slurm_cluster:
        job = parallel(
            Peer(parallel_hostname_identity_task, name="A", on_node="head"),
            Peer(parallel_hostname_identity_task, name="B", colocate_with="A"),
            Peer(parallel_hostname_identity_task, name="C"),
            topology=topo,
        )
        assert job.wait(timeout=300)
        results = job.get_results()

    a_host = results["A"]["hostname"]
    b_host = results["B"]["hostname"]
    c_host = results["C"]["hostname"]
    assert a_host and b_host and c_host
    assert a_host == b_host, (
        f"colocate_with failed: A={a_host!r}, B={b_host!r} should match"
    )
    # C is unpinned — either host is fine. Assert it actually landed on
    # one of the two nodes the scheduler allocated to this job (not some
    # unrelated host on the network). The allocation's host list is
    # identical across peers within a single-pool allocation; we pull it
    # from C's own ``ctx.hostnames`` so the assertion covers the same
    # peer under test.
    allocation_hosts = set(results["C"]["allocation_hostnames"])
    assert len(allocation_hosts) == 2, (
        f"allocation did not span 2 nodes: {allocation_hosts}"
    )
    assert a_host in allocation_hosts, (
        f"A's hostname {a_host!r} is not in the allocation hosts {allocation_hosts}"
    )
    assert c_host in allocation_hosts, (
        f"C's hostname {c_host!r} is not in the allocation hosts {allocation_hosts}"
    )


# ---------------------------------------------------------------------------
# 5. srun spans two nodes — one peer, @task(nodes=2), spans the allocation.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_srun_spans_two_nodes(slurm_cluster, multi_node_cluster):
    """Replica pair (``count=2``, ``tasks_per_node=1``) spans both nodes.

    A single non-replica peer under ``parallel(...)`` always renders as
    ``srun --ntasks=1``, so the outer allocation shrinks to one node even
    if ``Pool(nodes=2)`` was requested. A 2-replica peer with
    ``tasks_per_node=1`` gets ``srun --ntasks=2 --ntasks-per-node=1``,
    which forces Slurm to reserve and dispatch across two distinct
    nodes — what the test actually cares about.
    """
    pool = Pool(nodes=2, partition="debug")
    topo = Topology(pools={"default": pool})
    with slurm_cluster:
        job = parallel(
            Peer.replicas(
                parallel_multi_node_task,
                count=2,
                tasks_per_node=1,
                name="span",
            ),
            topology=topo,
        )
        assert job.wait(timeout=300)
        replicas = job["span"].get_results()

    assert len(replicas) == 2
    nodelists = {r["job_nodelist"] for r in replicas}
    assert len(nodelists) == 1, f"replicas disagree on SLURM_JOB_NODELIST: {nodelists}"
    nodelist = next(iter(nodelists)) or ""
    # SLURM_JOB_NUM_NODES is allocation-level and agrees across replicas.
    assert {r["job_num_nodes"] for r in replicas} == {"2"}, (
        f"expected SLURM_JOB_NUM_NODES=2 on every replica, got "
        f"{[r['job_num_nodes'] for r in replicas]!r}"
    )
    controller = multi_node_cluster["controller_hostname"]
    worker = multi_node_cluster["worker_hostname"]
    # SLURM_JOB_NODELIST is compressed (e.g. "slurm-[control,worker]");
    # checking each component substring is portable across expansions.
    assert "control" in nodelist, f"{controller!r} missing from nodelist {nodelist!r}"
    assert "worker" in nodelist, f"{worker!r} missing from nodelist {nodelist!r}"
