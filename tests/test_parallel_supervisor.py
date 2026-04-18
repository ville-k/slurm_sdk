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
        max_restarts=0,
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


def test_restart_on_success_is_treated_as_success():
    # A peer with on_failure="restart" that exits 0 on its first attempt
    # doesn't actually restart — just succeeds. Phase 4 adds this policy;
    # the "restart budget exhausted" path lives in its own test file.
    plan = _plan(
        _peer("learner", "exit 0", on_failure="restart"),
    )
    rc = topology_supervisor.run_supervisor(plan, launch=_shell_launch)
    assert rc == 0


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


def test_component_job_ids_single_component_returns_base_id():
    assert topology_supervisor._component_job_ids("12345", 1) == ["12345"]


def test_component_job_ids_multi_component_adds_suffixes():
    # Hetjob: base + <base>+1 + <base>+2 ... literal '+N', not arithmetic.
    ids = topology_supervisor._component_job_ids("12345", 3)
    assert ids == ["12345", "12345+1", "12345+2"]


def test_component_job_ids_returns_empty_without_job_id():
    assert topology_supervisor._component_job_ids(None, 3) == []


def test_signal_slurm_job_cascades_over_components(monkeypatch):
    """Multi-component scancel must pass every component id in one call."""
    import subprocess as _subprocess

    captured: dict = {}

    def _fake_run(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)

        class _R:
            returncode = 0

        return _R()

    monkeypatch.setattr(_subprocess, "run", _fake_run)
    topology_supervisor._signal_slurm_job("7000", "TERM", component_count=3)
    assert captured["cmd"] == [
        "scancel",
        "--signal=TERM",
        "7000",
        "7000+1",
        "7000+2",
    ]


def test_hard_cancel_cascades_over_components(monkeypatch):
    import subprocess as _subprocess

    captured: dict = {}

    def _fake_run(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)

        class _R:
            returncode = 0

        return _R()

    monkeypatch.setattr(_subprocess, "run", _fake_run)
    topology_supervisor._hard_cancel_slurm_job("7000", component_count=2)
    assert captured["cmd"] == ["scancel", "7000", "7000+1"]


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
