"""Phase 11 integration: ``parallel(...)`` invoked from inside ``@workflow``.

When a workflow body executes on the cluster (in a runner process),
``parallel(...)`` must resolve the active cluster via the workflow
context. These tests exercise the client-side path — we set an
:class:`WorkflowContext` as the active context, call the workflow's
``unwrapped`` body directly, and assert that the nested ``parallel(...)``
submission goes through the cluster's fake backend.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from slurm import Peer, parallel, task
from slurm.decorators import workflow
from slurm.context import (
    _clear_active_context,
    _reset_active_context,
    _set_active_context,
)
from slurm.parallel_job import ParallelJob
from slurm.workflow import WorkflowContext
from cluster_factory import make_test_cluster  # type: ignore


class _RecordingBackend:
    """Fake backend that counts submissions — shared with the e2e test suite."""

    def __init__(self, job_base_dir: str) -> None:
        self.job_base_dir = job_base_dir
        self.submissions: List[Dict[str, Any]] = []
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
        os.makedirs(target_job_dir, exist_ok=True)
        with open(
            os.path.join(target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"),
            "w",
        ) as fh:
            fh.write(script)
        self.submissions.append(
            {
                "script": script,
                "target_job_dir": target_job_dir,
                "pre_submission_id": pre_submission_id,
                "account": account,
                "partition": partition,
                "array_spec": array_spec,
            }
        )
        self._counter += 1
        return str(9000 + self._counter)

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


@task(cpus_per_task=1, mem="1G")
def _inner_leader() -> str:
    return "ok"


@task(cpus_per_task=1, mem="512M")
def _inner_helper() -> None:
    return None


@workflow
def _parallel_in_workflow(ctx: WorkflowContext) -> Dict[str, str]:
    """Workflow body that nests a ``parallel(...)`` submission.

    Returns a dict with the ParallelJob's base job id so the caller can
    confirm the submission reached the cluster's fake backend.
    """
    job = parallel(
        Peer(_inner_leader, leader=True),
        Peer(_inner_helper, on_failure="continue"),
    )
    assert isinstance(job, ParallelJob)
    return {"parallel_job_id": job.job_id, "peers": sorted(job.peer_jobs.keys())}


@pytest.fixture
def workflow_env(tmp_path: Path):
    _clear_active_context()
    backend = _RecordingBackend(str(tmp_path))
    cluster = make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
    )
    workflow_dir = tmp_path / "workflow"
    shared_dir = workflow_dir / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)
    ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_workflow",
        workflow_job_dir=workflow_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )
    token = _set_active_context(ctx)
    try:
        yield cluster, ctx, backend
    finally:
        _reset_active_context(token)
        _clear_active_context()


def test_parallel_inside_workflow_submits_via_active_cluster(workflow_env) -> None:
    cluster, ctx, backend = workflow_env
    # Calling the workflow body directly (unwrapped) simulates what the
    # runner does once the workflow is scheduled: the WorkflowContext is
    # the active cluster context, so parallel() resolves to the same
    # cluster as any regular @task submission would.
    result = _parallel_in_workflow.unwrapped(ctx=ctx)
    assert result["peers"] == ["_inner_helper", "_inner_leader"]
    assert result["parallel_job_id"].startswith("9")
    # Exactly one submission reached the backend.
    assert len(backend.submissions) == 1
    # And it's clearly a parallel script — the supervisor entry is the
    # load-bearing token for Phase 3+.
    assert "slurm.parallel.topology_supervisor" in backend.submissions[0]["script"]


def test_parallel_inside_workflow_nests_job_directory_under_workflow(
    workflow_env,
) -> None:
    """The parallel submission's target_job_dir should nest under the
    workflow's job dir — :func:`setup_job_directory` routes through
    ``WorkflowContext.workflow_job_dir`` when an active WorkflowContext
    exists."""
    cluster, ctx, backend = workflow_env
    _parallel_in_workflow.unwrapped(ctx=ctx)
    (submission,) = backend.submissions
    # The submission's target dir must live under the workflow's tasks/
    # subtree, not the cluster's base dir.
    assert str(ctx.workflow_job_dir) in submission["target_job_dir"]
    assert "/tasks/" in submission["target_job_dir"]
