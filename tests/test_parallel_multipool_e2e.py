"""End-to-end submission test for multi-pool / hetjob ``parallel(...)``.

Phase 6 makes the multi-pool path a first-class citizen. This test drives
``parallel(...)`` with a two-pool :class:`Topology` through a recording
backend and verifies:

- ``backend.submit_job`` is called once (hetjob is one sbatch submission).
- The rendered script carries per-pool ``#SBATCH`` headers with a
  ``#SBATCH hetjob`` divider between them.
- The plan carries per-peer ``component_index`` mapped from pool order.
- Every peer's :class:`Job` gets a distinct ``pre_submission_id`` so result
  file names don't collide across pools.

The script is not executed — that's Phase 12. We're verifying the
submission contract only.
"""

from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, List, Optional

import pytest

from slurm import Peer, Pool, Topology, parallel, task
from slurm.parallel.plan import Plan
from slurm.parallel_job import ParallelJob

from cluster_factory import make_test_cluster  # type: ignore


class _RecordingBackend:
    """Backend that captures scripts without running them."""

    def __init__(self, job_base_dir: str):
        self.job_base_dir = job_base_dir
        self.submitted_script: Optional[str] = None
        self.submitted_kwargs: Dict[str, Any] = {}
        self.submit_call_count = 0

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
        self.submit_call_count += 1
        self.submitted_script = script
        self.submitted_kwargs = {
            "target_job_dir": target_job_dir,
            "pre_submission_id": pre_submission_id,
            "account": account,
            "partition": partition,
            "array_spec": array_spec,
            **kwargs,
        }
        os.makedirs(target_job_dir, exist_ok=True)
        script_path = os.path.join(
            target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"
        )
        with open(script_path, "w") as fh:
            fh.write(script)
        return "9001"

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


@task(cpus_per_task=4, mem="16G", gpus_per_task=1)
def _learner(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=8, mem="16G")
def _sim_worker(env_id: int) -> int:
    return env_id


@task(cpus_per_task=2, mem="4G")
def _metrics() -> None:
    pass


@pytest.fixture
def cluster(tmp_path):
    backend = _RecordingBackend(str(tmp_path))
    return make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
    )


def _extract_plan(script: str) -> Plan:
    match = re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        script,
        re.DOTALL,
    )
    assert match is not None, "plan.json heredoc missing"
    return Plan.from_json(base64.b64decode(match.group(1).strip()).decode("utf-8"))


def test_two_pool_hetjob_submits_single_sbatch(cluster):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
            "cpu": Pool(nodes=2, cpus_per_node=8),
        },
        default_pool="gpu",
    )
    with cluster:
        job = parallel(
            Peer(_learner.partial(cfg={"lr": 1e-3}), pool="gpu", leader=True),
            Peer(_metrics, pool="cpu", on_failure="continue"),
            topology=topo,
        )

    assert isinstance(job, ParallelJob)
    # Hetjob is ONE sbatch submission — the multiple components live in a
    # single script.
    assert cluster.backend.submit_call_count == 1
    # Peers routed to their pools with matching component indices.
    plan = _extract_plan(cluster.backend.submitted_script)
    assert plan.pool_names == ["gpu", "cpu"]
    assert plan.peer_by_name("_learner").component_index == 0
    assert plan.peer_by_name("_metrics").component_index == 1


def test_two_pool_hetjob_peers_have_distinct_pre_submission_ids(cluster):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
            "cpu": Pool(nodes=1, cpus_per_node=4),
        },
        default_pool="gpu",
    )
    with cluster:
        job = parallel(
            Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
            Peer(_metrics, pool="cpu", on_failure="continue"),
            topology=topo,
        )

    learner_job = job["_learner"]
    metrics_job = job["_metrics"]
    # Same base Slurm job id (one sbatch submission) but distinct result
    # filenames per peer.
    assert learner_job.id == metrics_job.id == "9001"
    assert learner_job.pre_submission_id != metrics_job.pre_submission_id


def test_two_pool_hetjob_script_has_hetjob_divider_and_het_group(cluster):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
            "cpu": Pool(nodes=1, cpus_per_node=4),
        },
        default_pool="gpu",
    )
    with cluster:
        parallel(
            Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
            Peer(_metrics, pool="cpu", on_failure="continue"),
            topology=topo,
        )

    script = cluster.backend.submitted_script
    assert script is not None
    assert script.count("#SBATCH hetjob") == 1
    plan = _extract_plan(script)
    # One --het-group per peer, matching component index.
    assert "--het-group=0" in plan.peer_by_name("_learner").srun_command_line
    assert "--het-group=1" in plan.peer_by_name("_metrics").srun_command_line


def test_two_pool_hetjob_preserves_pool_declaration_order(cluster):
    # Declaration order is cpu, gpu — plan.pool_names must reflect this
    # (so component indices line up with the pool's position in the dict).
    topo = Topology(
        pools={
            "cpu": Pool(nodes=1, cpus_per_node=4),
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
        },
        default_pool="cpu",
    )
    with cluster:
        parallel(
            Peer(_metrics, pool="cpu", on_failure="continue"),
            Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
            topology=topo,
        )

    plan = _extract_plan(cluster.backend.submitted_script)
    assert plan.pool_names == ["cpu", "gpu"]
    # The CPU pool is component 0 because it is declared first.
    assert plan.peer_by_name("_metrics").component_index == 0
    assert plan.peer_by_name("_learner").component_index == 1
