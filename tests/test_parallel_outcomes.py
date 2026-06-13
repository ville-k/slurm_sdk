"""End-to-end outcome reporting via ``ParallelJob.peer_outcomes`` and
``ParallelJob.get_results``.

These tests construct a :class:`ParallelJob` around fake per-peer Job-like
stubs and a pre-populated ``registry.json``. We exercise every outcome
status from the supervisor and verify that ``get_results`` raises
:class:`CompositeJobError` iff any peer is fatal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from slurm.errors import CompositeJobError
from slurm.parallel import _build_spec
from slurm.parallel.registry import (
    OUTCOME_CONTINUE_ON_FAILURE,
    OUTCOME_FATAL,
    OUTCOME_NOT_STARTED,
    OUTCOME_SHUTDOWN_BY_LEADER,
    OUTCOME_SUCCESS,
    write_registry,
)
from slurm.parallel.validation import validate_spec
from slurm.parallel_job import ParallelJob, PeerOutcome
from slurm.task import SlurmTask


class _FakeJob:
    """Stand-in for :class:`slurm.job.Job` — returns a canned result."""

    def __init__(self, result: Any, raises: bool = False):
        self._result = result
        self._raises = raises

    def wait(self, timeout: Optional[float] = None) -> bool:  # noqa: ARG002
        return True

    def get_result(self, timeout: Optional[float] = None) -> Any:  # noqa: ARG002
        if self._raises:
            raise RuntimeError("download failed")
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


def _spec_with_peers(
    *peer_names: str, failure_policies: Optional[Dict[str, str]] = None
):
    from slurm.parallel import Peer

    failure_policies = failure_policies or {}
    positional = tuple(
        Peer(_task_named(name), on_failure=failure_policies.get(name, "kill"))
        for name in peer_names
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


def _write_registry_with_outcomes(
    tmp_path: Path, outcomes: Dict[str, Dict[str, Any]]
) -> Path:
    """Write a synthetic registry where each peer has a pre-set outcome."""
    peers = {}
    for name, data in outcomes.items():
        peers[name] = [
            {
                "name": name,
                "pool": "default",
                "replica_index": 0,
                "replica_count": 1,
                "hostname": "",
                "hostnames": [],
                "node_label": None,
                "step_id": None,
                "ports": {},
                "metadata": {},
                "state": data.get("state", "success"),
                "restart_count": data.get("restart_count", 0),
                "outcome": data.get("outcome"),
                "final_exit_code": data.get("final_exit_code"),
                "message": data.get("message"),
            }
        ]
    reg_path = tmp_path / "registry.json"
    write_registry(reg_path, {"peers": peers, "nodes": {}})
    return reg_path


def test_peer_outcomes_returns_entry_per_peer(tmp_path: Path):
    spec = _spec_with_peers("a", "b", "c")
    _write_registry_with_outcomes(
        tmp_path,
        {
            "a": {"outcome": OUTCOME_SUCCESS, "final_exit_code": 0},
            "b": {
                "outcome": OUTCOME_SHUTDOWN_BY_LEADER,
                "final_exit_code": 143,
            },
            "c": {
                "outcome": OUTCOME_CONTINUE_ON_FAILURE,
                "final_exit_code": 5,
                "message": "tolerated",
            },
        },
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={
            "a": _FakeJob("A"),
            "b": _FakeJob("B"),
            "c": _FakeJob(None, raises=True),
        },
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    outcomes = job.peer_outcomes()
    assert outcomes["a"] == PeerOutcome(status="success", exit_code=0, restart_count=0)
    assert outcomes["b"] == PeerOutcome(
        status="shutdown_by_leader", exit_code=143, restart_count=0
    )
    assert outcomes["c"] == PeerOutcome(
        status="continue_on_failure",
        exit_code=5,
        restart_count=0,
        message="tolerated",
    )


def test_get_results_returns_dict_when_no_fatal_peers(tmp_path: Path):
    spec = _spec_with_peers("ok", "tolerated", "terminated")
    _write_registry_with_outcomes(
        tmp_path,
        {
            "ok": {"outcome": OUTCOME_SUCCESS, "final_exit_code": 0},
            "tolerated": {
                "outcome": OUTCOME_CONTINUE_ON_FAILURE,
                "final_exit_code": 3,
            },
            "terminated": {
                "outcome": OUTCOME_SHUTDOWN_BY_LEADER,
                "final_exit_code": 143,
            },
        },
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={
            "ok": _FakeJob("result-ok"),
            "tolerated": _FakeJob(None, raises=True),
            "terminated": _FakeJob(None, raises=True),
        },
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    results = job.get_results()
    assert results == {
        "ok": "result-ok",
        "tolerated": None,
        "terminated": None,
    }


def test_get_results_raises_composite_error_when_any_peer_fatal(tmp_path: Path):
    spec = _spec_with_peers("good", "dead")
    _write_registry_with_outcomes(
        tmp_path,
        {
            "good": {"outcome": OUTCOME_SUCCESS, "final_exit_code": 0},
            "dead": {
                "outcome": OUTCOME_FATAL,
                "final_exit_code": 42,
                "message": "on_failure=kill aborted the group",
            },
        },
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={
            "good": _FakeJob("OK"),
            "dead": _FakeJob(None, raises=True),
        },
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    with pytest.raises(CompositeJobError) as info:
        job.get_results()
    assert len(info.value.failures) == 1
    failure = info.value.failures[0]
    assert failure.peer_name == "dead"
    assert failure.exit_code == 42


def test_not_started_counts_as_fatal(tmp_path: Path):
    spec = _spec_with_peers("never_ran", "live")
    _write_registry_with_outcomes(
        tmp_path,
        {
            "never_ran": {
                "outcome": OUTCOME_NOT_STARTED,
                "final_exit_code": None,
            },
            "live": {"outcome": OUTCOME_SUCCESS, "final_exit_code": 0},
        },
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={
            "never_ran": _FakeJob(None, raises=True),
            "live": _FakeJob("hi"),
        },
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    with pytest.raises(CompositeJobError):
        job.get_results()


def test_peer_outcomes_defaults_to_not_started_when_registry_missing(tmp_path: Path):
    # No registry.json at all → every peer reports not_started.
    spec = _spec_with_peers("alone")
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={"alone": _FakeJob("x")},
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    outcomes = job.peer_outcomes()
    assert outcomes["alone"].status == OUTCOME_NOT_STARTED


def test_get_results_falls_back_to_phase3_behavior_without_registry(tmp_path: Path):
    # No registry → legacy path: continue-policy tolerates exceptions,
    # kill-policy propagates them. (Back-compat with tests that mocked
    # the backend but didn't wire in a supervisor.)
    spec = _spec_with_peers(
        "ok",
        "flaky",
        failure_policies={"flaky": "continue"},
    )
    job = ParallelJob(
        cluster=None,  # type: ignore[arg-type]
        job_id="1",
        peer_jobs={
            "ok": _FakeJob("value"),
            "flaky": _FakeJob(None, raises=True),
        },
        spec=spec,
        target_job_dir=str(tmp_path),
    )
    results = job.get_results()
    assert results == {"ok": "value", "flaky": None}
