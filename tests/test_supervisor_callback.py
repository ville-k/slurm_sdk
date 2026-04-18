"""Supervisor callback-policy tests.

The supervisor imports user callbacks by ``module:qualname`` at startup
(fail-fast on unresolvable names) and invokes them with a ``JobSnapshot``
when a peer fails. The callback's return value (``"kill"`` / ``"continue"``)
dispatches the same shutdown / tolerate paths the built-in policies use.
"""

from __future__ import annotations

import subprocess  # nosec B404 - synthetic peers
from pathlib import Path
from typing import List

import pytest

from slurm.job import JobSnapshot
from slurm.parallel import topology_supervisor
from slurm.parallel.plan import Plan, PlanPeer
from slurm.parallel.registry import (
    OUTCOME_CONTINUE_ON_FAILURE,
    OUTCOME_FATAL,
    read_registry,
    write_registry,
)


# Module-level callbacks — supervisor resolves them by ``module:qualname``
# so they must live where ``importlib.import_module`` can find them.

_callback_invocations: List[JobSnapshot] = []


def callback_returns_continue(snapshot: JobSnapshot) -> str:
    _callback_invocations.append(snapshot)
    return "continue"


def callback_returns_kill(snapshot: JobSnapshot) -> str:
    _callback_invocations.append(snapshot)
    return "kill"


def callback_raises(_snapshot: JobSnapshot) -> str:
    raise RuntimeError("boom")


def callback_returns_garbage(_snapshot: JobSnapshot) -> str:
    return "not-a-valid-disposition"  # type: ignore[return-value]


_MODULE = __name__


def _peer(
    name: str,
    cmd: str,
    *,
    leader: bool = False,
    on_failure: str = "kill",
    callback: "str | None" = None,
) -> PlanPeer:
    return PlanPeer(
        name=name,
        pool="default",
        leader=leader,
        on_failure=on_failure,
        max_restarts=0,
        srun_command_line=cmd,
        callback=callback,
    )


def _plan(*peers: PlanPeer, grace: int = 1) -> Plan:
    return Plan(
        peers=list(peers),
        grace_period_seconds=grace,
        pool_names=["default"],
        pre_submission_id="test",
    )


def _seed_registry(tmp_path: Path, *peer_names: str) -> Path:
    peers = {
        name: [
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
                "state": "pending",
                "restart_count": 0,
                "outcome": None,
                "final_exit_code": None,
                "message": None,
            }
        ]
        for name in peer_names
    }
    reg_path = tmp_path / "registry.json"
    write_registry(reg_path, {"peers": peers, "nodes": {}})
    return reg_path


def _shell_launch(peer: PlanPeer) -> subprocess.Popen:
    return subprocess.Popen(  # nosec B603 - test-only
        ["/bin/sh", "-c", peer.srun_command_line]
    )


@pytest.fixture(autouse=True)
def _reset():
    topology_supervisor._external_shutdown = False
    _callback_invocations.clear()
    yield
    topology_supervisor._external_shutdown = False
    _callback_invocations.clear()


def test_callback_continue_tolerates_failure(tmp_path: Path):
    reg_path = _seed_registry(tmp_path, "flaky", "good")
    plan = _plan(
        _peer(
            "flaky",
            "exit 19",
            on_failure="callback",
            callback=f"{_MODULE}:callback_returns_continue",
        ),
        _peer("good", "sleep 0.1; exit 0"),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 0
    entry = read_registry(reg_path)["peers"]["flaky"][0]
    assert entry["outcome"] == OUTCOME_CONTINUE_ON_FAILURE
    assert entry["final_exit_code"] == 19
    # Callback was actually invoked with a JobSnapshot.
    assert len(_callback_invocations) == 1
    assert isinstance(_callback_invocations[0], JobSnapshot)
    assert _callback_invocations[0].exit_code == "19:0"


def test_callback_kill_aborts_group(tmp_path: Path):
    reg_path = _seed_registry(tmp_path, "fatal", "sibling")
    plan = _plan(
        _peer(
            "fatal",
            "exit 23",
            on_failure="callback",
            callback=f"{_MODULE}:callback_returns_kill",
        ),
        _peer("sibling", "sleep 10", on_failure="continue"),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 23
    registry = read_registry(reg_path)
    assert registry["peers"]["fatal"][0]["outcome"] == OUTCOME_FATAL
    # Sibling got SIGTERMed in the cascade.
    assert registry["peers"]["sibling"][0]["outcome"] == "shutdown_by_leader"


def test_callback_raising_is_treated_as_kill(tmp_path: Path):
    # A callback that raises must not silently tolerate the failure —
    # defaulting to "kill" keeps surprising behavior loud instead of quiet.
    reg_path = _seed_registry(tmp_path, "boom")
    plan = _plan(
        _peer(
            "boom",
            "exit 5",
            on_failure="callback",
            callback=f"{_MODULE}:callback_raises",
        ),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 5
    entry = read_registry(reg_path)["peers"]["boom"][0]
    assert entry["outcome"] == OUTCOME_FATAL


def test_callback_invalid_return_is_treated_as_kill(tmp_path: Path):
    reg_path = _seed_registry(tmp_path, "weird")
    plan = _plan(
        _peer(
            "weird",
            "exit 2",
            on_failure="callback",
            callback=f"{_MODULE}:callback_returns_garbage",
        ),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 2
    entry = read_registry(reg_path)["peers"]["weird"][0]
    assert entry["outcome"] == OUTCOME_FATAL


def test_preload_callbacks_fails_fast_on_unresolvable():
    plan = _plan(
        _peer(
            "broken",
            "exit 0",
            on_failure="callback",
            callback="does.not.exist:whatever",
        ),
    )
    with pytest.raises((ImportError, ModuleNotFoundError)):
        topology_supervisor._preload_callbacks(plan)
