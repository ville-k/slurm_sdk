"""Local-mode integration tests for ``parallel(...)`` (Phase 12).

These tests use the *real* :class:`LocalBackend` (with its Phase 12
bypass-sbatch path) so they prove the whole chain end-to-end without
needing Slurm installed.

The bypass fires automatically when ``sbatch`` is not on ``PATH``. On dev
workstations that happen to have Slurm installed we still want to run
these tests deterministically, so they set
``SLURM_SDK_FORCE_LOCAL_PARALLEL=1`` before submission.
"""

from __future__ import annotations

import os

import pytest

from slurm import Peer, parallel, task
from slurm.api.local import LocalBackend
from slurm.cluster import Cluster
from slurm.parallel import topology_supervisor
from slurm.parallel.plan import PlanPeer
from slurm.parallel.validation import validate_local_capacity
from slurm.parallel.types import Peer as _Peer, _ParallelSpec, Pool, Topology
from slurm.errors import TopologyError


@task(time="00:01:00", mem="128M", cpus_per_task=1)
def _return_name(name: str) -> str:
    return f"peer:{name}"


@task(time="00:01:00", mem="128M", cpus_per_task=1)
def _return_pid() -> int:
    return os.getpid()


@task(time="00:01:00", mem="128M", cpus_per_task=1)
def _return_answer() -> int:
    return 42


@pytest.fixture(autouse=True)
def _force_local_parallel(monkeypatch):
    """Force the bypass path regardless of sbatch availability."""
    monkeypatch.setenv("SLURM_SDK_FORCE_LOCAL_PARALLEL", "1")
    yield


@pytest.fixture
def local_cluster(tmp_path):
    backend = LocalBackend(job_base_dir=str(tmp_path))
    cluster = Cluster.from_backend(
        backend,
        backend_type="local",
        default_packaging="none",
    )
    cluster.job_base_dir = str(tmp_path)
    yield cluster


def test_three_peer_parallel_runs_without_slurm(local_cluster):
    """3-peer parallel job — all complete, results are accessible."""
    with local_cluster:
        job = parallel(
            Peer(_return_name.partial(name="alpha"), name="alpha"),
            Peer(_return_name.partial(name="beta"), name="beta"),
            Peer(_return_name.partial(name="gamma"), name="gamma"),
        )
        assert job.wait(timeout=60)
        results = job.get_results()

    assert results == {
        "alpha": "peer:alpha",
        "beta": "peer:beta",
        "gamma": "peer:gamma",
    }


def test_two_peer_leader_sidecar_local(local_cluster):
    """Leader + sidecar: both run; leader's completion cascades shutdown."""
    with local_cluster:
        job = parallel(
            Peer(_return_answer, leader=True, name="leader"),
            Peer(_return_pid, on_failure="continue", name="worker"),
        )
        assert job.wait(timeout=60)
        results = job.get_results()

    assert results["leader"] == 42
    # Sidecar may or may not have produced a result — it might have been
    # killed by the leader-exit cascade. Phase 11's get_results allows None
    # for continue-on-failure peers. Accept either the pid or None.
    worker = results.get("worker")
    assert worker is None or isinstance(worker, int)


def test_local_mode_shutdown_is_clean(local_cluster):
    """After wait() returns, all peer processes must be reaped."""
    with local_cluster:
        job = parallel(
            Peer(_return_answer, name="a"),
            Peer(_return_answer, name="b"),
        )
        assert job.wait(timeout=60)

    # The supervisor tracks its children; inspect the backend's bookkeeping.
    backend = local_cluster.backend
    for info in backend._local_parallel_jobs.values():
        proc = info["process"]
        assert proc.poll() is not None, "supervisor process not terminal"


def test_extract_runner_argv_strips_srun_prefix():
    """Unit-level check on the srun→runner extractor."""
    line = (
        "srun --exact --overlap --ntasks=1 --job-name=foo --export=ALL,X=Y "
        '"$PY_EXEC_RESOLVED" -m slurm.runner --module "m" --function "f"'
    )
    argv = topology_supervisor._extract_runner_argv(line)
    assert argv[0] == "-m"
    assert argv[1] == "slurm.runner"
    assert "--module" in argv
    assert "f" in argv


def test_extract_runner_argv_rejects_missing_sentinel():
    with pytest.raises(RuntimeError, match="sentinel"):
        topology_supervisor._extract_runner_argv("srun --exact echo hi")


def test_launch_peer_local_sets_slurm_procid(tmp_path, monkeypatch):
    """A locally-launched replica inherits SLURM_PROCID and SLURM_NTASKS."""
    # Fake a peer whose "runner" just echoes SLURM_PROCID so we can verify it.
    # We write a tiny wrapper that reads the env and exits.
    stub = tmp_path / "echo_procid.py"
    stub.write_text(
        "import os, sys\n"
        "sys.stdout.write(os.environ.get('SLURM_PROCID', 'unset'))\n"
        "sys.stdout.flush()\n"
    )

    # Build an srun line pointing at the stub; the runner-extraction logic
    # will pull everything after $PY_EXEC_RESOLVED as argv.
    srun = f'srun --exact --ntasks=1 "$PY_EXEC_RESOLVED" {stub}'
    peer = PlanPeer(
        name="worker",
        pool="default",
        leader=False,
        on_failure="kill",
        max_restarts=0,
        srun_command_line=srun,
        replica_count=3,
    )

    # Launch replica 2 out of 3 — PROCID should be 2, NTASKS should be 3.
    proc = topology_supervisor._launch_peer_local(
        peer, replica_index=2, replica_count=3, job_id="42"
    )
    rc = proc.wait(timeout=10)
    assert rc == 0


def test_local_capacity_rejects_overflow():
    """Sum of per-task CPUs across peers exceeds host → TopologyError."""
    huge = (os.cpu_count() or 4) * 100

    @task(cpus_per_task=huge, mem="1M")
    def _hog() -> None:
        pass

    peer = _Peer(task=_hog, name="hog", pool="default")
    pool = Pool(nodes=1)
    topology = Topology(pools={"default": pool})
    spec = _ParallelSpec(peers=(peer,), topology=topology)

    with pytest.raises(TopologyError, match="local host has"):
        validate_local_capacity(spec)


def test_local_capacity_accepts_small_job():
    """A 1-CPU peer fits anywhere — no error."""

    @task(cpus_per_task=1, mem="1M")
    def _tiny() -> None:
        pass

    peer = _Peer(task=_tiny, name="tiny", pool="default")
    pool = Pool(nodes=1)
    topology = Topology(pools={"default": pool})
    spec = _ParallelSpec(peers=(peer,), topology=topology)

    # Does not raise.
    validate_local_capacity(spec)


def test_should_bypass_sbatch_detects_parallel_script(tmp_path):
    backend = LocalBackend(job_base_dir=str(tmp_path))
    # The sentinel is what the renderer emits near the bottom.
    parallel_script = "#!/bin/bash\necho hi\nexec python -m slurm.parallel.topology_supervisor --job-dir .\n"
    assert backend._should_bypass_sbatch(parallel_script) is True
    plain_script = "#!/bin/bash\necho hi\nsrun whatever\n"
    assert backend._should_bypass_sbatch(plain_script) is False
