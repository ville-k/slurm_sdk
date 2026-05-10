"""Multi-node integration tests against the existing single-task surface.

These tests exercise the two-node test cluster (``slurm-test`` +
``slurm-worker``) using only the existing ``@task`` decorator and
``cluster.submit(...)``-style submission paths. They prove the test
infrastructure works end-to-end before any higher-level multi-peer
abstractions are layered on top.

Each test depends on the session-scoped ``multi_node_cluster`` fixture
(in ``conftest.py``) which skips cleanly on single-node dev setups and
hard-fails in CI when ``SLURM_TEST_REQUIRE_MULTI_NODE=1`` is set.
"""

from __future__ import annotations

import pytest

from slurm.examples.integration_test_task import multi_node_identity_task


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
def test_multi_node_cluster_fixture_smoke(multi_node_cluster):
    """Sanity check: when both containers are up, the fixture sees 2 nodes.

    Without this, a misconfigured ``slurm-worker`` could let the rest of
    the suite skip everything silently and look green on CI.
    """
    idle_nodes = multi_node_cluster["idle_nodes"]
    assert len(idle_nodes) == 2, f"expected 2 IDLE nodes, sinfo reports {idle_nodes}"
    assert multi_node_cluster["controller_hostname"] in idle_nodes
    assert multi_node_cluster["worker_hostname"] in idle_nodes


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_task_targets_specific_node(slurm_cluster, multi_node_cluster):
    """``@task(nodelist="slurm-worker")`` lands the job on the worker.

    Proves the existing ``@task`` decorator's sbatch-kwargs passthrough
    correctly emits ``#SBATCH --nodelist=...`` and Slurm honours it
    against a two-node cluster — exercises node-pinning end-to-end
    without relying on any newer placement APIs.
    """
    pinned = multi_node_identity_task.with_options(
        nodelist=multi_node_cluster["worker_hostname"]
    )
    with slurm_cluster:
        job = pinned()
        assert job.wait(timeout=240)
        result = job.get_result()

    assert result["hostname"] == multi_node_cluster["worker_hostname"], (
        f"expected job to run on {multi_node_cluster['worker_hostname']!r}, "
        f"got {result['hostname']!r}"
    )


@pytest.mark.integration_test
@pytest.mark.integration_test_multi_node
@pytest.mark.slow_integration_test
def test_task_allocation_spans_two_nodes(slurm_cluster, multi_node_cluster):
    """``@task(nodes=2)`` reserves a 2-node allocation visible via env.

    Asserts ``SLURM_JOB_NUM_NODES=2`` and that ``SLURM_JOB_NODELIST``
    enumerates both compute hosts. The function only runs on one of the
    nodes (single srun task) but the allocation-level env reflects the
    full reservation.
    """
    spanning = multi_node_identity_task.with_options(nodes=2)
    with slurm_cluster:
        job = spanning()
        assert job.wait(timeout=240)
        result = job.get_result()

    assert result["nnodes"] == "2", (
        f"expected SLURM_JOB_NUM_NODES=2, got {result['nnodes']!r}"
    )
    nodelist = result["nodelist"] or ""
    # SLURM_JOB_NODELIST is compressed (e.g. "slurm-[control,worker]");
    # substring checks are portable across compressed/expanded forms.
    assert "control" in nodelist, f"controller hostname not in nodelist {nodelist!r}"
    assert "worker" in nodelist, f"worker hostname not in nodelist {nodelist!r}"
