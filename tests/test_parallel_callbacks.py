"""Phase 11 callback integration for :class:`ParallelJob`.

Verifies:

- A successful submission emits exactly one ``SubmitEndContext`` for the
  whole allocation, regardless of peer count.
- The aggregate ``CompletedContext`` fires exactly once when the
  representative peer's status transitions to a terminal state. Per-peer
  Jobs no longer carry their own ``on_completed`` hook so duplicate
  events can't leak out of the existing ``Job.get_status`` machinery.
- Independent ``Job`` submissions still see their own callbacks — the
  Phase 11 fan-in does not disturb the single-job path.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pytest

from slurm import Peer, parallel, task
from slurm.callbacks import BaseCallback, CompletedContext, SubmitEndContext
from cluster_factory import make_test_cluster  # type: ignore


class _RecordingBackend:
    """Backend that records submissions and canned terminal statuses."""

    def __init__(self, job_base_dir: str) -> None:
        self.job_base_dir = job_base_dir
        self._counter = 0
        self.scripts: List[str] = []

    def is_remote(self) -> bool:
        return False

    def submit_job(
        self,
        script: str,
        *,
        target_job_dir: str,
        pre_submission_id: str,
        account: Optional[str] = None,
        partition: Optional[str] = None,
        array_spec: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        self.scripts.append(script)
        os.makedirs(target_job_dir, exist_ok=True)
        with open(
            os.path.join(target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"),
            "w",
        ) as fh:
            fh.write(script)
        self._counter += 1
        return str(8000 + self._counter)

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        # Force a terminal state so downstream completion hooks fire.
        return {"JobState": "COMPLETED", "ExitCode": "0:0"}

    def get_cluster_info(self) -> Dict[str, Any]:
        return {"partitions": []}

    def cancel_job(self, job_id: str) -> bool:
        return True

    def get_queue(self) -> List[Dict[str, Any]]:
        return []

    def close(self) -> None:
        return None


class _CountingCallback(BaseCallback):
    """Callback that tallies how often each lifecycle hook fires."""

    def __init__(self) -> None:
        self.submit_end: List[SubmitEndContext] = []
        self.completed: List[CompletedContext] = []

    def on_end_submit_job_ctx(self, ctx: SubmitEndContext) -> None:
        self.submit_end.append(ctx)

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        self.completed.append(ctx)


@task(cpus_per_task=1, mem="1G")
def _leader_task() -> str:
    return "done"


@task(cpus_per_task=1, mem="512M")
def _helper() -> None:
    return None


@pytest.fixture
def recording_cluster(tmp_path):
    backend = _RecordingBackend(str(tmp_path))
    callback = _CountingCallback()
    cluster = make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
        callbacks=[callback],
    )
    return cluster, callback


def test_parallel_submit_end_fires_exactly_once(recording_cluster) -> None:
    cluster, callback = recording_cluster
    with cluster:
        job = parallel(
            Peer(_leader_task, leader=True),
            Peer(_helper, on_failure="continue"),
        )
    assert len(job) == 2
    assert len(callback.submit_end) == 1
    (ctx,) = callback.submit_end
    # The representative peer's Job is what the context carries — its id
    # matches the allocation's base job id so downstream consumers can
    # index by job id without caring about the hetjob layout.
    assert ctx.job_id == job.job_id


def test_parallel_completed_fires_exactly_once(recording_cluster) -> None:
    cluster, callback = recording_cluster
    with cluster:
        job = parallel(
            Peer(_leader_task, leader=True),
            Peer(_helper, on_failure="continue"),
            Peer(_helper, name="sidekick", on_failure="continue"),
        )

    # Every peer Job shares the same Slurm job id. Driving the status from
    # any single peer must still result in exactly ONE completion event —
    # the Phase 11 contract is per-allocation, not per-peer.
    for peer_job in job.peer_jobs.values():
        peer_job.get_status()

    assert len(callback.completed) == 1
    (ctx,) = callback.completed
    assert ctx.job_id == job.job_id
    assert ctx.job_state == "COMPLETED"


def test_parallel_non_representative_peers_skip_direct_completion(
    recording_cluster,
) -> None:
    """Only the representative (leader / first peer) carries ``on_completed``.

    Driving ``get_status()`` on a non-representative peer must not fire a
    second :class:`CompletedContext`. If Phase 11 regresses and wires
    ``on_completed`` onto every peer Job, this test will fail with two
    events.
    """
    cluster, callback = recording_cluster
    with cluster:
        job = parallel(
            Peer(_leader_task, leader=True),
            Peer(_helper, on_failure="continue"),
        )
    # Hit both peers' status paths — still only one event.
    job["_leader_task"].get_status()
    job["_helper"].get_status()
    assert len(callback.completed) == 1


def test_single_job_submission_still_fires_its_own_callbacks(tmp_path) -> None:
    """Guard against Phase 11 accidentally suppressing the single-Job path."""
    backend = _RecordingBackend(str(tmp_path))
    callback = _CountingCallback()
    cluster = make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
        callbacks=[callback],
    )

    @task(cpus_per_task=1, mem="512M")
    def _solo() -> int:
        return 7

    with cluster:
        submitted = cluster.submit(_solo)()
    submitted.get_status()
    assert len(callback.submit_end) == 1
    assert len(callback.completed) == 1
