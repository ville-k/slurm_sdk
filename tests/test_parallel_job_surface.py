"""Phase 11 tests for the :class:`ParallelJob` aggregate surface.

Covers the three surfaces that Phase 11 formalises:

- :meth:`ParallelJob.snapshot` returning one ``JobSnapshot`` per singleton
  peer and a ``list[JobSnapshot]`` per replica peer.
- :attr:`ParallelJob.leader_result` raising clear errors for the zero /
  multiple leader edge cases.
- ``peer_outcomes()`` aggregation matching the snapshot layout for mixed
  singleton+replica allocations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from slurm.job import JobSnapshot
from slurm.parallel import Peer, _build_spec
from slurm.parallel.registry import (
    OUTCOME_SUCCESS,
    write_registry,
)
from slurm.parallel.validation import validate_spec
from slurm.parallel_job import ParallelJob, ReplicaGroup, _ParallelPeerHandle
from slurm.task import SlurmTask


class _SnapshotJob:
    """Minimal :class:`slurm.job.Job` stand-in that returns a canned snapshot."""

    def __init__(self, *, state: str = "COMPLETED", result: Any = None) -> None:
        self.id = "42"
        self.target_job_dir = "/tmp"
        self._state = state
        self._result = result

    def snapshot(self, tail_lines: int = 80) -> JobSnapshot:
        return JobSnapshot(
            job_id=self.id,
            state=self._state,
            exit_code="0:0" if self._state == "COMPLETED" else "1:0",
            reason=None,
            stdout_tail="",
            stderr_tail="",
            elapsed_seconds=1.0,
            is_terminal=self._state in {"COMPLETED", "FAILED"},
            is_successful=self._state == "COMPLETED",
        )

    def wait(self, timeout: Optional[float] = None) -> bool:  # noqa: ARG002
        return True

    def get_result(self, timeout: Optional[float] = None) -> Any:  # noqa: ARG002
        return self._result


def _task_named(name: str) -> SlurmTask:
    def _fn() -> None:
        return None

    _fn.__name__ = name
    return SlurmTask(
        func=_fn,
        sbatch_options={"cpus_per_task": 1, "mem": "1G"},
        packaging=None,
    )


def _singleton_spec(*peer_names: str):
    positional = tuple(Peer(_task_named(name)) for name in peer_names)
    spec = _build_spec(
        positional=positional,
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    validate_spec(spec)
    return spec


def _leader_spec():
    """Two singletons with an explicit leader."""
    trainer = _task_named("trainer")
    metrics = _task_named("metrics")
    positional = (
        Peer(trainer, leader=True),
        Peer(metrics, on_failure="continue"),
    )
    spec = _build_spec(
        positional=positional,
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    validate_spec(spec)
    return spec


def _mixed_spec():
    """One singleton + one replica peer to exercise snapshot shape divergence."""
    leader = _task_named("leader")
    worker = _task_named("worker")
    positional = (
        Peer(leader, leader=True),
        Peer.replicas(worker, count=3),
    )
    spec = _build_spec(
        positional=positional,
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    validate_spec(spec)
    return spec


def _multi_leader_spec():
    """Intentionally invalid leader shape built via the non-validating spec
    builder.

    ``validate_spec`` already forbids multiple leaders, so we build the
    ``_ParallelSpec`` directly to exercise
    :attr:`ParallelJob.leader_result` 's runtime guard — the property must
    raise even if a malformed spec ever slips past validation.
    """
    from slurm.parallel.types import _ParallelSpec, Topology, Pool

    first = Peer(_task_named("a"), leader=True)
    second = Peer(_task_named("b"), leader=True)
    return _ParallelSpec(
        peers=(first, second),
        topology=Topology(pools={"default": Pool(nodes=1)}),
    )


# ---------------------------------------------------------------------------
# snapshot()
# ---------------------------------------------------------------------------


def test_snapshot_singleton_peers_returns_jobsnapshot_per_peer() -> None:
    spec = _singleton_spec("a", "b")
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={
            "a": _SnapshotJob(state="COMPLETED"),
            "b": _SnapshotJob(state="COMPLETED"),
        },
        spec=spec,
    )
    snap = job.snapshot()
    assert set(snap.keys()) == {"a", "b"}
    assert isinstance(snap["a"], JobSnapshot)
    assert isinstance(snap["b"], JobSnapshot)
    assert snap["a"].state == "COMPLETED"
    assert snap["a"].is_successful is True


def test_snapshot_replica_peer_returns_list_of_snapshots() -> None:
    spec = _mixed_spec()
    replica_jobs = [_SnapshotJob(state="COMPLETED") for _ in range(3)]
    peer_jobs = {
        "leader": _SnapshotJob(state="COMPLETED"),
        "worker": replica_jobs[0],
    }
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs=peer_jobs,
        spec=spec,
        peer_replica_jobs={"worker": replica_jobs},
    )
    snap = job.snapshot()
    assert set(snap.keys()) == {"leader", "worker"}
    assert isinstance(snap["leader"], JobSnapshot)
    assert isinstance(snap["worker"], list)
    assert len(snap["worker"]) == 3
    for entry in snap["worker"]:
        assert isinstance(entry, JobSnapshot)


def test_parallel_job_peer_handles_preserve_legacy_surface() -> None:
    spec = _mixed_spec()
    leader_job = _SnapshotJob(state="COMPLETED", result="leader")
    replica_jobs = [_SnapshotJob(state="COMPLETED", result=i) for i in range(3)]
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs=None,
        spec=spec,
        peer_handles={
            "leader": _ParallelPeerHandle(
                name="leader",
                representative_job=leader_job,
            ),
            "worker": _ParallelPeerHandle(
                name="worker",
                representative_job=replica_jobs[0],
                replica_jobs=tuple(replica_jobs),
            ),
        },
    )

    assert job.peer_jobs == {"leader": leader_job, "worker": replica_jobs[0]}
    assert list(job) == [leader_job, replica_jobs[0]]
    assert job["leader"] is leader_job
    group = job["worker"]
    assert isinstance(group, ReplicaGroup)
    assert group.replica_jobs == replica_jobs
    assert job["worker", 2] is replica_jobs[2]


def test_snapshot_forwards_tail_lines_to_per_peer_job() -> None:
    captured: Dict[str, int] = {}

    class _TailCapturingJob(_SnapshotJob):
        def snapshot(self, tail_lines: int = 80) -> JobSnapshot:  # type: ignore[override]
            captured[self.id] = tail_lines
            return super().snapshot(tail_lines=tail_lines)

    spec = _singleton_spec("a")
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"a": _TailCapturingJob()},
        spec=spec,
    )
    job.snapshot(tail_lines=5)
    assert captured == {"42": 5}


# ---------------------------------------------------------------------------
# leader_result edge cases
# ---------------------------------------------------------------------------


def test_leader_result_raises_when_no_leader_declared() -> None:
    spec = _singleton_spec("a", "b")
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"a": _SnapshotJob(), "b": _SnapshotJob()},
        spec=spec,
    )
    with pytest.raises(RuntimeError, match="no leader|one leader peer"):
        _ = job.leader_result


def test_leader_result_raises_when_multiple_leaders() -> None:
    spec = _multi_leader_spec()
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"a": _SnapshotJob(), "b": _SnapshotJob()},
        spec=spec,
    )
    with pytest.raises(RuntimeError, match="multiple leader"):
        _ = job.leader_result


def test_leader_result_returns_unique_leader_value() -> None:
    spec = _leader_spec()
    leader_job = _SnapshotJob(state="COMPLETED", result={"loss": 0.1})
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"trainer": leader_job, "metrics": _SnapshotJob()},
        spec=spec,
    )
    assert job.leader_result == {"loss": 0.1}


# ---------------------------------------------------------------------------
# peer_outcomes aggregation alignment with snapshot
# ---------------------------------------------------------------------------


def test_peer_outcomes_keys_align_with_snapshot_for_mixed_shape(tmp_path: Path) -> None:
    spec = _mixed_spec()
    # Write a registry where every peer / replica reports success.
    peers_entries: Dict[str, Any] = {
        "leader": [
            {
                "name": "leader",
                "pool": "default",
                "replica_index": 0,
                "replica_count": 1,
                "hostname": "",
                "hostnames": [],
                "node_label": None,
                "step_id": None,
                "ports": {},
                "metadata": {},
                "state": "success",
                "restart_count": 0,
                "outcome": OUTCOME_SUCCESS,
                "final_exit_code": 0,
                "message": None,
            }
        ],
        "worker": [
            {
                "name": "worker",
                "pool": "default",
                "replica_index": i,
                "replica_count": 3,
                "hostname": "",
                "hostnames": [],
                "node_label": None,
                "step_id": None,
                "ports": {},
                "metadata": {},
                "state": "success",
                "restart_count": 0,
                "outcome": OUTCOME_SUCCESS,
                "final_exit_code": 0,
                "message": None,
            }
            for i in range(3)
        ],
    }
    write_registry(tmp_path / "registry.json", {"peers": peers_entries, "nodes": {}})

    replica_jobs = [_SnapshotJob() for _ in range(3)]
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"leader": _SnapshotJob(), "worker": replica_jobs[0]},
        spec=spec,
        target_job_dir=str(tmp_path),
        peer_replica_jobs={"worker": replica_jobs},
    )

    outcomes = job.peer_outcomes()
    snap = job.snapshot()

    # Singletons: one outcome key per singleton peer; same key surfaces in
    # snapshot.
    assert "leader" in outcomes
    assert "leader" in snap
    # Replicas: outcomes key is ``<name>[i]`` (Phase 4 contract); snapshot
    # is a list indexed by ``i``. Both shapes must cover every replica.
    for i in range(3):
        assert f"worker[{i}]" in outcomes
    assert isinstance(snap["worker"], list) and len(snap["worker"]) == 3


# ---------------------------------------------------------------------------
# after(...) dependency propagation
# ---------------------------------------------------------------------------


def test_parallel_after_kwarg_emits_afterok_in_every_component() -> None:
    """``parallel(after=prior)`` must render ``--dependency=afterok:<id>``
    into every hetjob component's sbatch header."""
    import os as _os
    from typing import Optional as _Opt

    from slurm import parallel as _parallel
    from slurm import Peer as _Peer, task as _task
    from cluster_factory import make_test_cluster as _mkc

    class _Backend:
        def __init__(self, base: str) -> None:
            self.job_base_dir = base
            self.scripts: list[str] = []
            self._c = 0

        def is_remote(self) -> bool:
            return False

        def submit_job(
            self,
            script: str,
            *,
            target_job_dir: str,
            pre_submission_id: str,
            account: _Opt[str] = None,
            partition: _Opt[str] = None,
            array_spec: _Opt[str] = None,
            **kwargs: Any,
        ) -> str:
            self.scripts.append(script)
            _os.makedirs(target_job_dir, exist_ok=True)
            self._c += 1
            return str(5000 + self._c)

        def get_job_status(self, job_id: str) -> Dict[str, Any]:
            return {"JobState": "PENDING"}

        def get_cluster_info(self) -> Dict[str, Any]:
            return {"partitions": []}

        def cancel_job(self, job_id: str) -> bool:
            return True

        def get_queue(self) -> List[Dict[str, Any]]:
            return []

        def close(self) -> None:
            return None

    @_task(cpus_per_task=1, mem="1G")
    def _t() -> int:
        return 1

    from slurm.job import Job

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = _Backend(tmp)
        cluster = _mkc(
            backend=backend,
            backend_type="local",
            job_base_dir=tmp,
            default_packaging="none",
        )
        # Build a synthetic upstream Job — just a Slurm id is required
        # for the dependency string.
        upstream = Job(id="1234", cluster=cluster, target_job_dir=tmp)
        with cluster:
            _parallel(_Peer(_t), after=upstream)
        # The rendered script must mention the afterok dependency.
        (script,) = backend.scripts
        assert "--dependency=afterok:1234" in script


def test_parallel_job_after_resubmits_with_dependency() -> None:
    """``ParallelJob.after(dep)`` cancels the original and re-submits with
    ``--dependency=afterok:<id>`` on the new allocation."""
    import os as _os
    from typing import Optional as _Opt

    from slurm import parallel as _parallel
    from slurm import Peer as _Peer, task as _task
    from cluster_factory import make_test_cluster as _mkc

    class _Backend:
        def __init__(self, base: str) -> None:
            self.job_base_dir = base
            self.scripts: list[str] = []
            self.cancelled: list[str] = []
            self._c = 0

        def is_remote(self) -> bool:
            return False

        def submit_job(
            self,
            script: str,
            *,
            target_job_dir: str,
            pre_submission_id: str,
            account: _Opt[str] = None,
            partition: _Opt[str] = None,
            array_spec: _Opt[str] = None,
            **kwargs: Any,
        ) -> str:
            self.scripts.append(script)
            _os.makedirs(target_job_dir, exist_ok=True)
            self._c += 1
            return str(6000 + self._c)

        def get_job_status(self, job_id: str) -> Dict[str, Any]:
            return {"JobState": "PENDING"}

        def get_cluster_info(self) -> Dict[str, Any]:
            return {"partitions": []}

        def cancel_job(self, job_id: str) -> bool:
            self.cancelled.append(job_id)
            return True

        def get_queue(self) -> List[Dict[str, Any]]:
            return []

        def close(self) -> None:
            return None

    @_task(cpus_per_task=1, mem="1G")
    def _t() -> int:
        return 1

    from slurm.job import Job
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        backend = _Backend(tmp)
        cluster = _mkc(
            backend=backend,
            backend_type="local",
            job_base_dir=tmp,
            default_packaging="none",
        )
        upstream = Job(id="9999", cluster=cluster, target_job_dir=tmp)
        with cluster:
            first = _parallel(_Peer(_t))
            second = first.after(upstream)
        # Exactly two sbatch submissions — the original and the re-run.
        assert len(backend.scripts) == 2
        # The second script carries the dependency, the first doesn't.
        assert "--dependency=afterok:9999" not in backend.scripts[0]
        assert "--dependency=afterok:9999" in backend.scripts[1]
        # And the original allocation was cancelled.
        assert first.job_id in backend.cancelled
        assert second.job_id != first.job_id


def test_parallel_job_after_rejects_non_job_deps() -> None:
    from slurm.parallel_job import ParallelJob
    from slurm.parallel.types import _ParallelSpec, Topology, Pool

    spec = _ParallelSpec(
        peers=(Peer(_task_named("x")),),
        topology=Topology(pools={"default": Pool(nodes=1)}),
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"x": _SnapshotJob()},
        spec=spec,
    )
    with pytest.raises(TypeError, match="expects Job"):
        job.after("not-a-job")  # type: ignore[arg-type]


def test_parallel_job_after_requires_at_least_one_dep() -> None:
    from slurm.parallel_job import ParallelJob
    from slurm.parallel.types import _ParallelSpec, Topology, Pool

    spec = _ParallelSpec(
        peers=(Peer(_task_named("x")),),
        topology=Topology(pools={"default": Pool(nodes=1)}),
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="42",
        peer_jobs={"x": _SnapshotJob()},
        spec=spec,
    )
    with pytest.raises(TypeError, match="at least one"):
        job.after()
