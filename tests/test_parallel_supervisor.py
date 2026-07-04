"""Supervisor lifecycle tests — kill / continue / leader exit / grace window.

These avoid Slurm entirely: the :func:`run_supervisor` ``launch`` hook lets
us spawn plain ``/bin/sh -c`` subprocesses that run for a fixed duration,
exit with a specific code, or ignore SIGTERM. The supervisor's policy
logic and grace-window enforcement are exercised without needing ``srun``
or a real cluster.
"""

from __future__ import annotations

import subprocess  # nosec B404 - required to construct synthetic peers
import time
from typing import List

import pytest

from slurm.parallel import topology_supervisor
from slurm.parallel.plan import Plan, PlanPeer


def _peer(
    name: str,
    cmd: str,
    *,
    leader: bool = False,
    on_failure: str = "kill",
) -> PlanPeer:
    return PlanPeer(
        name=name,
        pool="default",
        leader=leader,
        on_failure=on_failure,
        srun_command_line=cmd,
    )


def _plan(*peers: PlanPeer, grace: int = 2) -> Plan:
    return Plan(
        peers=list(peers),
        grace_period_seconds=grace,
        pool_names=["default"],
        pre_submission_id="test",
    )


def _shell_launch(peer: PlanPeer) -> subprocess.Popen:
    """Launch synthetic peer: run the srun_command_line as a /bin/sh snippet.

    This short-circuits the bash -c + srun + runner chain so tests stay
    hermetic. The supervisor's lifecycle logic doesn't care what the child
    process actually is — only when it exits and with what code.
    """
    return subprocess.Popen(  # nosec B603 - test-only, test-authored command
        ["/bin/sh", "-c", peer.srun_command_line]
    )


@pytest.fixture(autouse=True)
def _reset_external_shutdown():
    """Ensure module-level shutdown flag doesn't bleed between tests."""
    topology_supervisor._external_shutdown = False
    yield
    topology_supervisor._external_shutdown = False


def test_all_peers_succeed_returns_zero():
    plan = _plan(
        _peer("a", "sleep 0.05; exit 0"),
        _peer("b", "sleep 0.05; exit 0"),
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 0


def test_kill_policy_triggers_shutdown_with_failing_exit_code():
    # "a" fails with kill policy; "b" would run for 5s but must be terminated.
    plan = _plan(
        _peer("a", "sleep 0.05; exit 17"),
        _peer("b", "sleep 5"),
        grace=1,
    )
    start = time.time()
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    elapsed = time.time() - start
    assert rc == 17
    # Should be much less than 5s — the grace window + signal cascade kills
    # the long-running peer.
    assert elapsed < 3.0, f"shutdown too slow: {elapsed:.2f}s"


def test_continue_policy_tolerates_failure():
    # "bad" fails with continue policy — supervisor waits for "good" to
    # finish normally, then exits 0.
    plan = _plan(
        _peer("bad", "exit 5", on_failure="continue"),
        _peer("good", "sleep 0.1; exit 0"),
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 0


def test_local_replica_success_does_not_overwrite_sibling_fatal(
    tmp_path, monkeypatch
):
    """Regression: a clean replica exit must not mask a sibling's fatal one.

    Local mode runs each replica as its own Popen. Before the fix, the
    success branch blanket-wrote OUTCOME_SUCCESS across every replica
    entry, so replica 1 finishing cleanly after replica 0 died rewrote the
    registry to claim the whole set succeeded (while the supervisor still
    exited non-zero).
    """
    from slurm.parallel.registry import read_registry, write_registry

    registry_path = tmp_path / "registry.json"
    entries = [
        {
            "name": "workers",
            "pool": "default",
            "replica_index": i,
            "replica_count": 2,
            "metadata": {},
            "state": "pending",
        }
        for i in range(2)
    ]
    write_registry(registry_path, {"peers": {"workers": entries}})

    # Replica 0 dies immediately (kill policy → group fatal). Replica 1
    # ignores the shutdown SIGTERM and finishes cleanly afterwards.
    replica_cmds = {0: "exit 3", 1: "trap '' TERM; sleep 0.4; exit 0"}

    def _fake_local_launch(peer, *, replica_index, **_kwargs):
        # start_new_session=True mirrors the real local launcher: shutdown
        # signals the child's process group, which must not be pytest's.
        return subprocess.Popen(  # nosec B603 - test-only synthetic peer
            ["/bin/sh", "-c", replica_cmds[replica_index]],
            start_new_session=True,
        )

    monkeypatch.setattr(
        topology_supervisor, "_launch_peer_local", _fake_local_launch
    )

    plan = _plan(
        PlanPeer(
            name="workers",
            pool="default",
            leader=False,
            on_failure="kill",
            replica_count=2,
            srun_command_line="unused-in-local-mode",
        ),
        grace=2,
    )
    rc = topology_supervisor.run_supervisor(
        plan, registry_path=registry_path, local_mode=True
    )
    assert rc == 3

    peers = read_registry(registry_path)["peers"]["workers"]
    by_index = {int(e["replica_index"]): e for e in peers}
    # Per-replica truth: the crashed replica stays fatal ...
    assert by_index[0]["outcome"] == "fatal"
    assert by_index[0]["state"] == "failed"
    # ... and the clean replica records its own success without
    # blanketing the sibling.
    assert by_index[1]["outcome"] == "success"
    assert by_index[1]["state"] == "success"


def test_leader_success_triggers_sibling_shutdown():
    # Leader exits cleanly → supervisor returns leader's exit code (0) and
    # signals siblings. Siblings that would have run long get terminated.
    plan = _plan(
        _peer("leader", "sleep 0.1; exit 0", leader=True),
        _peer("helper", "sleep 5", on_failure="continue"),
        grace=1,
    )
    start = time.time()
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    elapsed = time.time() - start
    assert rc == 0
    assert elapsed < 3.0


def test_leader_failure_propagates_exit_code():
    plan = _plan(
        _peer("leader", "sleep 0.05; exit 42", leader=True),
        _peer("helper", "sleep 5", on_failure="continue"),
        grace=1,
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 42


def test_grace_window_hard_kills_stragglers():
    # Child traps SIGTERM and ignores it; the grace deadline forces SIGKILL.
    # Since we're on POSIX with /bin/sh, `trap '' TERM` swallows TERM but
    # KILL cannot be trapped.
    ignore_term = "trap '' TERM; sleep 10 & wait"
    plan = _plan(
        _peer("leader", "exit 0", leader=True),
        _peer("stubborn", ignore_term, on_failure="continue"),
        grace=1,
    )
    start = time.time()
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    elapsed = time.time() - start
    assert rc == 0
    # 1s grace + some overhead; well under the 10s sleep.
    assert elapsed < 5.0, f"hard-kill slow: {elapsed:.2f}s"


def test_no_leader_all_success_returns_zero():
    plan = _plan(
        _peer("a", "exit 0"),
        _peer("b", "exit 0"),
        _peer("c", "exit 0"),
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 0


def test_first_fatal_exit_code_wins():
    # Two peers both fail kill; supervisor records the first one's exit and
    # immediately signals the rest. Grace is short so test finishes fast.
    plan = _plan(
        _peer("a", "sleep 0.05; exit 11"),
        _peer("b", "sleep 5; exit 99"),
        grace=1,
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 11


def test_signal_slurm_job_is_noop_without_scancel(monkeypatch):
    # If scancel isn't installed the supervisor must not raise — tests run
    # in contexts (CI, developer laptops) where scancel is often absent.
    import subprocess as _subprocess

    def _fake_run(*args, **kwargs):
        raise FileNotFoundError("no scancel")

    monkeypatch.setattr(_subprocess, "run", _fake_run)
    topology_supervisor._signal_slurm_job("12345", "TERM")
    topology_supervisor._hard_cancel_slurm_job("12345")


def test_signal_slurm_job_noop_without_job_id():
    # job_id=None should short-circuit before any subprocess call.
    topology_supervisor._signal_slurm_job(None, "TERM")
    topology_supervisor._hard_cancel_slurm_job(None)


def test_signal_slurm_job_targets_base_id(monkeypatch):
    """scancel --signal targets the single allocation job id."""
    import subprocess as _subprocess

    captured: dict = {}

    def _fake_run(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)

        class _R:
            returncode = 0

        return _R()

    monkeypatch.setattr(_subprocess, "run", _fake_run)
    topology_supervisor._signal_slurm_job("7000", "TERM")
    assert captured["cmd"] == ["scancel", "--signal=TERM", "7000"]


def test_hard_cancel_targets_base_id(monkeypatch):
    import subprocess as _subprocess

    captured: dict = {}

    def _fake_run(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)

        class _R:
            returncode = 0

        return _R()

    monkeypatch.setattr(_subprocess, "run", _fake_run)
    topology_supervisor._hard_cancel_slurm_job("7000")
    assert captured["cmd"] == ["scancel", "7000"]


def test_main_roundtrips_plan(tmp_path, monkeypatch):
    from slurm.parallel.plan import write_plan

    plan = _plan(_peer("solo", "exit 3"))
    write_plan(tmp_path / "plan.json", plan)

    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    # Use the CLI entry point — exit code reflects the peer's.
    rc = topology_supervisor.main(["--job-dir", str(tmp_path)])
    assert rc == 3


def _keep_imports_live() -> List[int]:
    # Ensure List import survives lint; used above implicitly via typing.
    return []
