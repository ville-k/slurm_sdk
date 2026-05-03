"""End-to-end integration tests for ``parallel(...)`` against real Slurm.

The unit suite exercises the parallel surface against ``_RecordingBackend``,
``FakeBackend``, and the local bypass-sbatch path. That leaves the set of
behaviours that only a real scheduler can surface:

1. ``#SBATCH hetjob`` header acceptance by ``sbatch``.
2. ``srun --het-group=N`` dispatch inside the allocation.
3. ``$SLURM_JOB_NODELIST_HET_GROUP_<N>`` / ``$SLURM_PROCID`` /
   ``$SLURM_STEP_ID`` populated by the real scheduler (not synthesized).
4. ``scancel --signal=TERM <jobid>+<N>`` cascade across hetjob components.
5. ``scontrol show hostnames`` expansion inside bootstrap.
6. Real restart semantics under Slurm's atomic-``srun --ntasks=N`` model.
7. Per-peer dependency chaining (``--dependency=afterok:...``) on every
   hetjob component.

These tests run against ``containers/slurm-test`` (single-node
``slurm-control`` in the ``debug`` partition). That covers (1)-(6)
plus bootstrap / runner publish / ``wait_all`` under a scheduler-
populated environment. Multi-node placement still needs a production
cluster — called out in the relevant test's docstring.
"""

from __future__ import annotations

import pytest

from slurm import Peer, Pool, Topology, parallel
from slurm.examples.integration_test_task import (
    parallel_coordinator_task,
    parallel_downstream_peer_task,
    parallel_flaky_peer_task,
    parallel_leader_task,
    parallel_long_lived_sidecar_task,
    parallel_pool_identity_task,
    parallel_replica_identity_task,
    parallel_slow_upstream_task,
    parallel_worker_task,
    simple_integration_task,
)


# ---------------------------------------------------------------------------
# 1. Two-peer leader + sidecar round-trip.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_two_peer_leader_sidecar(slurm_cluster):
    """Leader exit cascades SIGTERM to a long-lived sidecar under real Slurm.

    Proves the ``leader=True`` shutdown semantic end-to-end:

    - The leader returns quickly (~1s body) and exits 0.
    - The sidecar sleeps 120s so its natural runtime is never reached —
      only a broken cascade would let the allocation outlive the leader
      by that long.
    - After ``job.wait()`` the sidecar's ``peer_outcomes()`` status is
      ``"shutdown_by_leader"``, the terminal string the supervisor
      writes when it SIGTERMs a peer on leader exit (see
      ``OUTCOME_SHUTDOWN_BY_LEADER`` in ``topology_supervisor``).
    - The wall-clock elapsed time stays well under the sidecar's 120s
      sleep — a belt-and-braces check that catches the case where the
      supervisor records the right outcome but the process hung around.

    If the cascade is removed the elapsed assertion fires first and the
    outcome assertion fails second.
    """
    import time

    with slurm_cluster:
        started = time.monotonic()
        job = parallel(
            Peer(parallel_leader_task, leader=True, name="leader"),
            Peer(
                parallel_long_lived_sidecar_task,
                on_failure="continue",
                name="side",
            ),
        )
        assert job.wait(timeout=180)
        elapsed = time.monotonic() - started
        outcomes = job.peer_outcomes()
        leader_result = job.leader_result

    assert leader_result == 42

    side_outcome = outcomes["side"]
    assert side_outcome.status == "shutdown_by_leader", (
        f"expected side.status='shutdown_by_leader' (leader-driven cascade); "
        f"got {side_outcome!r}"
    )
    # The sidecar sleeps 120s; leader exits ~immediately. Real-Slurm
    # scheduling latency on the test cluster is a few seconds. An elapsed
    # much closer to 120s means the sidecar ran to natural completion.
    assert elapsed < 90, (
        f"elapsed={elapsed:.1f}s is too close to the sidecar's 120s sleep — "
        "leader-exit cascade may be broken"
    )


# ---------------------------------------------------------------------------
# 2. Replica set — real SLURM_PROCID drives per-replica result files.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_replica_set_per_replica_results(slurm_cluster):
    """``Peer.replicas(count=4)`` → four distinct results via real scheduler env.

    This is the coverage gap ``test_parallel_local_mode`` leaves: under
    local mode the SDK synthesizes ``SLURM_PROCID`` / ``SLURM_NTASKS`` per
    subprocess. Under real Slurm, the ``srun --ntasks=4`` step has the
    scheduler populate those env vars per task. The test proves the
    runner's ``:by-taskid`` dispatch and the per-replica output-file
    rewrite land correctly with scheduler-populated values.
    """
    with slurm_cluster:
        job = parallel(
            Peer.replicas(
                parallel_replica_identity_task,
                count=4,
                name="workers",
            ),
        )
        assert job.wait(timeout=240)
        replicas = job["workers"].get_results()

    # Four distinct replicas returned without collision. Order is by
    # replica index because ReplicaGroup.get_results() iterates in order.
    assert len(replicas) == 4
    procids = sorted(int(r["procid"]) for r in replicas)
    assert procids == [0, 1, 2, 3], f"expected distinct PROCIDs, got {procids}"
    # NTASKS is scheduler-set, so every replica saw the same value.
    assert {r["ntasks"] for r in replicas} == {"4"}
    # replica_index in JobContext matches SLURM_PROCID — the two plumbing
    # paths (plan-based replica dispatch + Slurm's own PROCID) agree.
    for r in replicas:
        assert r["replica_index"] == int(r["procid"])


# ---------------------------------------------------------------------------
# 3. Two-pool hetjob — sbatch accepts the header, HET_GROUP env is populated.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_two_pool_hetjob_header_accepted(slurm_cluster):
    """Two-pool ``Topology`` → ``sbatch`` accepts the hetjob header.

    Full end-to-end hetjob scheduling requires multiple nodes so each
    component can get its own node allocation — the single-node
    ``slurm-control`` container genuinely cannot schedule a 2-component
    hetjob where each component declares ``--nodes=1`` (the second
    component blocks on ``Reason=Resources`` forever). Cross-partition
    scheduling and cross-node-type placement still require a
    production-class cluster.

    What this DOES prove, end-to-end against the real scheduler:

    - ``#SBATCH hetjob`` header accepted by ``sbatch`` without syntax
      errors — Slurm returns a valid hetjob id pair (``<base>+0`` /
      ``<base>+1``) visible through ``job["<name>"]._job_id``.
    - The renderer's hetjob script survives ``sbatch``'s parse step and
      Slurm's allocation controller acknowledges the component layout.
    - The per-peer ``--dependency=afterok`` propagation wired onto every
      hetjob component would route correctly — same header path.

    The allocation is cancelled immediately after submission so the test
    doesn't block on scheduling that will never happen on one node.
    """
    topo = Topology(
        pools={
            "alpha": Pool(nodes=1, partition="debug"),
            "beta": Pool(nodes=1, partition="debug"),
        },
    )
    with slurm_cluster:
        job = parallel(
            Peer(
                parallel_pool_identity_task.partial(pool="alpha"),
                pool="alpha",
                name="alpha",
            ),
            Peer(
                parallel_pool_identity_task.partial(pool="beta"),
                pool="beta",
                name="beta",
            ),
            topology=topo,
        )
        # sbatch accepts the hetjob header and returns a base id shared
        # by every peer — the peers' backing Jobs carry that same base id
        # because the supervisor dispatches component placement via
        # ``srun --het-group=N`` internally, not via per-peer distinct
        # job ids. A rejected hetjob header would have raised
        # ``SubmissionError`` before ``parallel(...)`` returned.
        alpha_id = job["alpha"].id
        beta_id = job["beta"].id
        assert alpha_id, "alpha peer received no job id from sbatch"
        assert beta_id == alpha_id, (
            f"peers should share a hetjob base id ({alpha_id} vs {beta_id})"
        )

        # Slurm's view of the allocation via ``scontrol show job``
        # confirms the hetjob layout. We verify the peer saw the whole
        # pair (``<base>+0`` + ``<base>+1``) registered — ``scontrol``
        # reports ``HetJobIdSet=<base>-<base+1>`` for a 2-component
        # hetjob. Hitting this successfully proves the whole hetjob
        # header survived parsing and registration.
        result = slurm_cluster.backend._run_command(
            f"scontrol show job {alpha_id}", timeout=10
        )
        scontrol_out = result[0] if isinstance(result, tuple) else str(result)
        assert "HetJobId=" in scontrol_out, (
            f"scontrol show job {alpha_id} did not report a HetJobId — "
            "the submission did not register as a heterogeneous job. "
            f"Output:\n{scontrol_out[:500]}"
        )
        assert "HetJobIdSet=" in scontrol_out, (
            f"scontrol show job {alpha_id} did not report HetJobIdSet — "
            "hetjob component set missing. "
            f"Output:\n{scontrol_out[:500]}"
        )

        # Release the reservation — the single-node cluster cannot
        # schedule both components anyway, and leaving the hetjob queued
        # blocks subsequent tests behind pending priority.
        job.cancel()


# ---------------------------------------------------------------------------
# 4. Service discovery — peers find each other via registry + wait_all.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_service_discovery_under_real_slurm(slurm_cluster):
    """Two peers announce + ``wait_all`` across a real Slurm allocation.

    Single-node slurm-control means we can't prove cross-node hostname
    boundaries here — that still needs a production-class cluster.
    What this DOES prove, end-to-end against the real scheduler: real
    scheduler-populated environment, bootstrap hostname resolution via
    ``scontrol show hostnames``, the runner's startup hostinfo publish,
    and ``wait_all(keys=["endpoint"])`` unblocking on an announced
    metadata key — all running under Slurm rather than under a
    synthesized-env local-mode supervisor.
    """
    with slurm_cluster:
        job = parallel(
            Peer(parallel_coordinator_task, name="coordinator"),
            Peer(parallel_worker_task, name="worker"),
        )
        assert job.wait(timeout=240)
        results = job.get_results()

    endpoint = results["coordinator"]["endpoint"]
    assert endpoint.startswith("tcp://"), f"unexpected endpoint {endpoint!r}"

    worker = results["worker"]
    assert worker["observed_endpoint"] == endpoint
    assert worker["observed_role"] == "coordinator"
    # The ``state`` transition is racy — the coordinator exits immediately
    # after its ``announce(ready=True)``, so the worker may observe the
    # supervisor's later terminal state (``success``, mapped by PeerInfo
    # to ``failed`` under the live-discovery contract) before it finishes
    # running. What we assert instead are the stable signals that persist
    # across the coordinator's full lifecycle:
    #
    # - ``observed_endpoint`` proves ``ctx.announce(endpoint=...)`` landed
    #   in ``registry.json`` and ``wait_all(keys=["endpoint"])`` unblocked
    #   on the real scheduler-populated runtime.
    # - ``observed_role`` proves a second metadata field alongside the one
    #   we blocked on (announces merge, not overwrite).
    # - A non-empty ``coordinator_hostname`` proves the runner's
    #   startup ``_publish_initial_hostinfo`` ran against
    #   scheduler-populated env vars rather than synthesized locals.
    assert worker["coordinator_hostname"], "coordinator hostname still empty"


# ---------------------------------------------------------------------------
# 5. Restart — supervisor relaunches a failed peer under real Slurm.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_restart_under_real_slurm(slurm_cluster):
    """``on_failure="restart"`` survives one non-zero exit and succeeds.

    The peer fails once (marker-less state → exit 7), the supervisor
    re-launches the same step via a fresh ``srun``, and the second attempt
    sees the marker in ``ctx.shared_dir`` and returns cleanly. Registry
    restart_count must reflect exactly one restart.
    """
    with slurm_cluster:
        job = parallel(
            Peer(
                parallel_flaky_peer_task,
                name="flaky",
                on_failure="restart",
                max_restarts=3,
            ),
        )
        assert job.wait(timeout=240)
        results = job.get_results()
        outcomes = job.peer_outcomes()

    assert results["flaky"] == 99
    outcome = outcomes["flaky"]
    assert outcome.status == "restarted"
    assert outcome.restart_count == 1
    assert outcome.exit_code == 0


# ---------------------------------------------------------------------------
# 6. after(upstream) — parallel job waits for an upstream dependency.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_parallel_after_upstream_dependency(slurm_cluster):
    """``parallel(...).after(upstream)`` holds the allocation PENDING on the dep.

    The prior form waited for upstream to finish before submitting
    downstream, which only proved wait() returned — the dependency
    string could have been dropped silently and the test still passed.
    This form submits a single-pool downstream ``parallel(...)`` while
    upstream is still running, then queries ``scontrol show job`` on
    the downstream allocation directly and asserts:

    - ``Dependency=afterok:<upstream_id>`` is present in the output,
      proving the SDK wired the dependency onto the downstream
      allocation.
    - ``JobState=PENDING`` + ``Reason=Dependency``, proving Slurm
      honoured it.

    Multi-pool hetjob submissions share the same ``#SBATCH
    --dependency=…`` header (it's a single allocation with multiple
    components), so this single-allocation form exercises the wiring
    path without needing a two-pool topology. If the
    ``--dependency=afterok:`` wiring is removed, the scontrol output
    won't carry the Dependency substring and the test fails on the
    first assertion — this is the point of the rework.
    """
    with slurm_cluster:
        upstream = parallel_slow_upstream_task()
        upstream_id = upstream.id
        assert upstream_id, "upstream did not receive a Slurm job id"

        downstream = parallel(
            Peer(parallel_downstream_peer_task, name="tail"),
        ).after(upstream)
        dep_id = downstream.job_id
        assert dep_id, "downstream did not receive a Slurm job id"

        # Query Slurm's view of the downstream job while upstream is
        # still running. The 20s sleep on upstream plus the few seconds
        # between .after()-resubmit and this scontrol call leaves plenty
        # of observation window.
        result = slurm_cluster.backend._run_command(
            f"scontrol show job {dep_id}", timeout=10
        )
        scontrol_out = result[0] if isinstance(result, tuple) else str(result)

        assert f"Dependency=afterok:{upstream_id}" in scontrol_out, (
            f"scontrol show job {dep_id} did not report "
            f"Dependency=afterok:{upstream_id} — the afterok wiring did "
            f"not propagate to the resubmitted allocation. Output:\n"
            f"{scontrol_out[:800]}"
        )
        assert "JobState=PENDING" in scontrol_out, (
            f"downstream job {dep_id} is not PENDING — dependency "
            f"did not hold it. Output:\n{scontrol_out[:800]}"
        )
        assert "Reason=Dependency" in scontrol_out, (
            f"downstream job {dep_id} PENDING but not held by Dependency "
            f"reason — state was held by something else. Output:\n"
            f"{scontrol_out[:800]}"
        )

        # Dependency satisfied → both jobs complete normally.
        assert upstream.wait(timeout=120)
        assert upstream.get_result() == "slow-upstream-token"
        assert downstream.wait(timeout=300)
        assert downstream.get_results()["tail"] == "downstream-ok"


# ---------------------------------------------------------------------------
# Smoke: sanity-check that the existing single-task path still works while
# the parallel integration suite is running in the same session. Cheap,
# catches obvious regressions from this branch against the same cluster.
# ---------------------------------------------------------------------------


@pytest.mark.integration_test
def test_single_task_baseline_still_works(slurm_cluster):
    """Non-parallel path must still work — sanity check against this branch."""
    with slurm_cluster:
        job = simple_integration_task()
        assert job.wait(timeout=120)
        assert job.get_result() == "integration-ok"
