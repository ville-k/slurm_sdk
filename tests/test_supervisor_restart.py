"""Supervisor restart-policy tests.

Synthetic peers fail a known number of times by decrementing a counter file,
then succeed. The supervisor's restart loop should keep re-launching a peer
with ``on_failure="restart"`` until either (a) it succeeds — outcome
``"restarted"`` with ``restart_count`` matching the retry count — or (b) the
``max_restarts`` budget is exhausted and the group aborts with outcome
``"fatal"``. Tests assert both the return code and the registry-recorded
outcome so downstream ``peer_outcomes()`` users get deterministic data.
"""

from __future__ import annotations

import subprocess  # nosec B404 - synthetic peers
from pathlib import Path

import pytest

from slurm.parallel import topology_supervisor
from slurm.parallel.plan import Plan, PlanPeer
from slurm.parallel.registry import (
    OUTCOME_FATAL,
    OUTCOME_RESTARTED,
    OUTCOME_SUCCESS,
    read_registry,
    write_registry,
)


def _peer(
    name: str,
    cmd: str,
    *,
    leader: bool = False,
    on_failure: str = "kill",
    max_restarts: int = 0,
    callback: "str | None" = None,
) -> PlanPeer:
    return PlanPeer(
        name=name,
        pool="default",
        leader=leader,
        on_failure=on_failure,
        max_restarts=max_restarts,
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
    """Write the registry skeleton the supervisor expects when recording outcomes."""
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


def _counter_script(counter_file: Path, fail_count: int, exit_code: int = 7) -> str:
    """Build a shell snippet that fails N times then succeeds.

    Each invocation reads ``counter_file``, exits non-zero if the count is
    below ``fail_count``, increments it, and exits 0 only when
    ``counter >= fail_count``.
    """
    path = str(counter_file)
    return (
        f'count=$(cat "{path}" 2>/dev/null || echo 0); '
        f"count=$((count + 1)); "
        f'echo "$count" > "{path}"; '
        f'if [ "$count" -le "{fail_count}" ]; then exit {exit_code}; fi; '
        "exit 0"
    )


def _shell_launch(peer: PlanPeer) -> subprocess.Popen:
    return subprocess.Popen(  # nosec B603 - test-authored command
        ["/bin/sh", "-c", peer.srun_command_line]
    )


def _seed_replica_registry(tmp_path: Path, peer_name: str, replica_count: int) -> Path:
    peers = {
        peer_name: [
            {
                "name": peer_name,
                "pool": "default",
                "replica_index": i,
                "replica_count": replica_count,
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
            for i in range(replica_count)
        ]
    }
    reg_path = tmp_path / "registry.json"
    write_registry(reg_path, {"peers": peers, "nodes": {}})
    return reg_path


@pytest.fixture(autouse=True)
def _reset_flag():
    topology_supervisor._external_shutdown = False
    yield
    topology_supervisor._external_shutdown = False


def test_restart_succeeds_within_budget(tmp_path: Path):
    # Peer fails twice, succeeds on the third attempt. Budget of 5 leaves
    # headroom so we're sure the success path — not budget exhaustion — is
    # what terminates the loop.
    counter = tmp_path / "retries.txt"
    reg_path = _seed_registry(tmp_path, "learner")
    plan = _plan(
        _peer(
            "learner",
            _counter_script(counter, fail_count=2, exit_code=7),
            on_failure="restart",
            max_restarts=5,
        ),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 0
    entry = read_registry(reg_path)["peers"]["learner"][0]
    assert entry["outcome"] == OUTCOME_RESTARTED
    assert entry["restart_count"] == 2
    assert entry["final_exit_code"] == 0


def test_restart_budget_exhaustion_is_fatal(tmp_path: Path):
    # Peer fails every time. Budget of 2 → 3 attempts total (initial + 2
    # restarts), all fail → fatal outcome.
    counter = tmp_path / "retries.txt"
    reg_path = _seed_registry(tmp_path, "flaky")
    plan = _plan(
        _peer(
            "flaky",
            _counter_script(counter, fail_count=99, exit_code=13),
            on_failure="restart",
            max_restarts=2,
        ),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 13
    entry = read_registry(reg_path)["peers"]["flaky"][0]
    assert entry["outcome"] == OUTCOME_FATAL
    assert entry["restart_count"] == 2
    assert entry["final_exit_code"] == 13


def test_restart_on_immediate_success_no_retries(tmp_path: Path):
    # Peer succeeds on first try → outcome is "success" (not "restarted").
    reg_path = _seed_registry(tmp_path, "clean")
    plan = _plan(
        _peer("clean", "exit 0", on_failure="restart", max_restarts=3),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 0
    entry = read_registry(reg_path)["peers"]["clean"][0]
    assert entry["outcome"] == OUTCOME_SUCCESS
    assert entry["restart_count"] == 0


def test_leader_restart_then_success_shuts_down_group(tmp_path: Path):
    # Leader with restart policy fails once, succeeds on retry. When it
    # finally succeeds, the group shuts down normally and the leader's
    # success is recorded as "restarted".
    counter = tmp_path / "leader_retries.txt"
    reg_path = _seed_registry(tmp_path, "leader", "helper")
    plan = _plan(
        _peer(
            "leader",
            _counter_script(counter, fail_count=1, exit_code=9),
            leader=True,
            on_failure="restart",
            max_restarts=3,
        ),
        _peer("helper", "sleep 10", on_failure="continue"),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 0
    registry = read_registry(reg_path)
    leader_entry = registry["peers"]["leader"][0]
    assert leader_entry["outcome"] == OUTCOME_RESTARTED
    assert leader_entry["restart_count"] == 1
    helper_entry = registry["peers"]["helper"][0]
    # helper was sleeping when leader won — shut down by leader.
    assert helper_entry["outcome"] == "shutdown_by_leader"


def test_restart_kill_fallback_aborts_siblings(tmp_path: Path):
    # Peer exhausts its budget (max=0 → no restarts) and falls through to
    # kill, which takes down a sibling that was running normally.
    reg_path = _seed_registry(tmp_path, "doomed", "sibling")
    plan = _plan(
        _peer("doomed", "exit 4", on_failure="restart", max_restarts=0),
        _peer("sibling", "sleep 10", on_failure="continue"),
    )
    rc = topology_supervisor.run_supervisor(
        plan, launch=_shell_launch, registry_path=reg_path
    )
    assert rc == 4
    registry = read_registry(reg_path)
    assert registry["peers"]["doomed"][0]["outcome"] == OUTCOME_FATAL
    assert registry["peers"]["sibling"][0]["outcome"] == "shutdown_by_leader"


def test_local_mode_replica_restart_relaunches_all_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restarting a local-mode replica set re-spawns *every* replica.

    Slurm treats ``srun --ntasks=N`` as one atomic step — any non-zero
    task triggers restart of the whole step. Local mode now mirrors
    that: when one replica exits non-zero and ``on_failure="restart"``
    fires, the supervisor terminates the live siblings (SIGTERM + grace
    + SIGKILL) and launches a fresh set of N replicas.

    The test proves this by counting how many times each replica index
    was spawned. Before the fix, only replica 0 restarted, so indices 1
    and 2 launched exactly once; after the fix, every index launches
    twice (initial + one restart).
    """
    peer_name = "workers"
    replica_count = 3
    # One launch-counter file per replica index. Each subprocess appends
    # "1" when it starts; the test reads the final contents to count.
    launch_logs = {i: tmp_path / f"launches_{i}.log" for i in range(replica_count)}
    for path in launch_logs.values():
        path.write_text("")

    reg_path = _seed_replica_registry(tmp_path, peer_name, replica_count)

    def _fake_local_launch(peer, **kwargs):
        replica_index = kwargs["replica_index"]
        log = launch_logs[replica_index]
        # Record every launch. The first launch of replica 1 exits non-zero
        # so the restart path fires; every other launch exits 0 so we stop.
        if replica_index == 1 and "1" not in log.read_text():
            script = (
                f'echo 1 >> "{log}"; '
                # Give siblings a moment to be running so the supervisor
                # actually has live processes to terminate — exercising
                # the whole SIGTERM / SIGKILL cascade on them.
                "sleep 0.2; "
                "exit 17"
            )
        else:
            script = f'echo 1 >> "{log}"; sleep 0.05; exit 0'
        return subprocess.Popen(  # nosec B603 - test-authored command
            ["/bin/sh", "-c", script], start_new_session=True
        )

    monkeypatch.setattr(topology_supervisor, "_launch_peer_local", _fake_local_launch)

    plan = Plan(
        peers=[
            PlanPeer(
                name=peer_name,
                pool="default",
                leader=False,
                on_failure="restart",
                max_restarts=3,
                srun_command_line=f'"$PY_EXEC_RESOLVED" -m slurm.runner --step peer:{peer_name}:by-taskid',
                replica_count=replica_count,
            )
        ],
        grace_period_seconds=2,
        pool_names=["default"],
        pre_submission_id="restart-parity",
    )

    rc = topology_supervisor.run_supervisor(
        plan, registry_path=reg_path, local_mode=True
    )
    assert rc == 0

    # Every replica ran exactly twice — once in the initial launch, once
    # after the whole-step restart. That's the parity-with-Slurm signal.
    launches_per_replica = {
        i: len([line for line in log.read_text().splitlines() if line.strip()])
        for i, log in launch_logs.items()
    }
    assert launches_per_replica == {0: 2, 1: 2, 2: 2}, (
        f"whole-step restart did not re-spawn every sibling: {launches_per_replica!r}"
    )

    entry = read_registry(reg_path)["peers"][peer_name][0]
    assert entry["restart_count"] == 1
    assert entry["outcome"] == OUTCOME_RESTARTED
