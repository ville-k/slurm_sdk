"""End-to-end submission test for parallel(...) via a fake backend.

Verifies that ``parallel()`` inside a Cluster context builds a spec,
renders a script, calls ``backend.submit_job``, and returns a
:class:`ParallelJob` with the right peer handles. We do not execute the
script — that's Phase 12 once local-mode-without-Slurm is wired.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pytest

from slurm import Peer, parallel, task
from slurm.parallel_job import ParallelJob
from cluster_factory import make_test_cluster  # type: ignore


class _RecordingBackend:
    """Minimal backend that captures the submitted script without running it."""

    def __init__(self, job_base_dir: str):
        self.job_base_dir = job_base_dir
        self.submitted_script: Optional[str] = None
        self.submitted_kwargs: Dict[str, Any] = {}
        self._counter = 0

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
        self.submitted_script = script
        self.submitted_kwargs = {
            "target_job_dir": target_job_dir,
            "pre_submission_id": pre_submission_id,
            "account": account,
            "partition": partition,
            "array_spec": array_spec,
            **kwargs,
        }
        # Persist the script to the target job dir so we can assert real IO.
        os.makedirs(target_job_dir, exist_ok=True)
        with open(
            os.path.join(target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"),
            "w",
        ) as fh:
            fh.write(script)
        self._counter += 1
        return str(7000 + self._counter)

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


@task(cpus_per_task=4, mem="8G")
def _train(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, mem="1G")
def _metrics() -> None:
    pass


@pytest.fixture
def cluster(tmp_path):
    backend = _RecordingBackend(str(tmp_path))
    c = make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
    )
    return c


def test_parallel_submits_and_returns_parallel_job(cluster):
    backend = cluster.backend
    with cluster:
        job = parallel(
            _train.partial(cfg={"lr": 0.001}),
            _metrics,
        )

    assert isinstance(job, ParallelJob)
    assert sorted(job.peer_jobs.keys()) == ["_metrics", "_train"]
    assert job.job_id == "7001"

    # All peers share the base job id but have distinct pre_submission_ids so
    # their result files don't collide.
    train_job = job["_train"]
    metrics_job = job["_metrics"]
    assert train_job.id == metrics_job.id == "7001"
    assert train_job.pre_submission_id != metrics_job.pre_submission_id
    assert "_peer__train" in (train_job.pre_submission_id or "")
    assert "_peer__metrics" in (metrics_job.pre_submission_id or "")

    # The rendered script was actually handed to the backend and persisted.
    assert backend.submitted_script is not None
    # Bootstrap + supervisor entrypoints replace the inline shell supervisor.
    assert "slurm.parallel.topology_bootstrap" in backend.submitted_script
    assert "slurm.parallel.topology_supervisor" in backend.submitted_script
    # Per-peer srun commands live inside the base64-encoded plan.json heredoc.
    # Decode and check that each peer is addressed by --step peer:<name>.
    import base64 as _b64
    import re as _re
    from slurm.parallel.plan import Plan

    m = _re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        backend.submitted_script,
        _re.DOTALL,
    )
    assert m is not None
    plan = Plan.from_json(_b64.b64decode(m.group(1).strip()).decode("utf-8"))
    names = sorted(p.name for p in plan.peers)
    assert names == ["_metrics", "_train"]
    for peer in plan.peers:
        assert f"--step peer:{peer.name}" in peer.srun_command_line


def test_parallel_leader_result_routing(cluster):
    # Leader present → a single leader; parallel() still returns a dict on
    # get_results. Phase 11 adds job.leader_result.
    with cluster:
        job = parallel(
            Peer(_train.partial(cfg={"lr": 0.01}), leader=True),
            Peer(_metrics, on_failure="continue"),
        )
    assert len(job) == 2
    assert "_train" in job
    assert "_metrics" in job


def test_parallel_missing_peer_keyerror(cluster):
    with cluster:
        job = parallel(_train.partial(cfg={}))
    with pytest.raises(KeyError, match="no peer"):
        _ = job["does-not-exist"]


def test_parallel_repr_lists_peer_names(cluster):
    with cluster:
        job = parallel(_train.partial(cfg={}), _metrics)
    assert "_train" in repr(job)
    assert "_metrics" in repr(job)
