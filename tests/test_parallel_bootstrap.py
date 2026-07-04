"""Tests for :mod:`slurm.parallel.topology_bootstrap`.

The bootstrap has two interesting pieces in Phase 3: (a) hostname
resolution from SLURM env vars with a scontrol→in-process fallback, and
(b) building a registry skeleton that downstream surfaces can read.
"""

from __future__ import annotations

from pathlib import Path

from slurm.parallel import topology_bootstrap
from slurm.parallel.plan import Plan, PlanPeer, write_plan
from slurm.parallel.registry import read_registry


def _sample_plan() -> Plan:
    return Plan(
        peers=[
            PlanPeer(
                name="learner",
                pool="default",
                leader=True,
                on_failure="kill",
                srun_command_line="echo learner",
            ),
            PlanPeer(
                name="metrics",
                pool="default",
                leader=False,
                on_failure="continue",
                srun_command_line="echo metrics",
            ),
        ],
        grace_period_seconds=5,
        pool_names=["default"],
        pre_submission_id="test123",
    )


def test_build_registry_skeleton_marks_peers_pending():
    plan = _sample_plan()
    skeleton = topology_bootstrap.build_registry_skeleton(plan, ("gpu-01", "gpu-02"))

    assert set(skeleton["peers"].keys()) == {"learner", "metrics"}
    for peer_name in ("learner", "metrics"):
        entries = skeleton["peers"][peer_name]
        assert len(entries) == 1
        entry = entries[0]
        # Every peer carries an empty hostname until its runner publishes
        # the actual value at startup — see ``update_peer_hostinfo``. The
        # pool's hostnames are still recorded for reference.
        assert entry["state"] == "pending"
        assert entry["pool"] == "default"
        assert entry["hostname"] == ""
        assert entry["hostnames"] == ["gpu-01", "gpu-02"]


def test_build_registry_skeleton_handles_empty_nodelist():
    # No SLURM env = no hostnames. Peers still get entries so the supervisor
    # can proceed; hostnames are filled in at runtime once they are known.
    plan = _sample_plan()
    skeleton = topology_bootstrap.build_registry_skeleton(plan, ())

    assert skeleton["peers"]["learner"][0]["hostname"] == ""
    assert skeleton["peers"]["learner"][0]["hostnames"] == []


def test_resolve_hostnames_falls_back_when_scontrol_missing(monkeypatch):
    # Force the scontrol subprocess call to FileNotFoundError so the
    # in-process fallback runs. The fallback handles simple brackets.
    import subprocess

    def _fake_run(*args, **kwargs):
        raise FileNotFoundError("scontrol missing")

    monkeypatch.setattr(subprocess, "run", _fake_run)
    hostnames = topology_bootstrap._resolve_hostnames("gpu-[01-02]")
    # The fallback expands bracket ranges.
    assert list(hostnames) == ["gpu-01", "gpu-02"]


def test_resolve_hostnames_empty_string():
    assert topology_bootstrap._resolve_hostnames("") == ()


def test_main_writes_registry(tmp_path, monkeypatch):
    job_dir = tmp_path
    plan = _sample_plan()
    write_plan(job_dir / "plan.json", plan)

    # Stub out scontrol so the test doesn't depend on the host.
    import subprocess

    def _fake_run(*args, **kwargs):
        raise FileNotFoundError("no scontrol")

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setenv("SLURM_JOB_NODELIST", "test-host-[01-02]")

    rc = topology_bootstrap.main(["--job-dir", str(job_dir)])
    assert rc == 0

    registry_path = job_dir / "registry.json"
    assert registry_path.exists()
    registry = read_registry(registry_path)
    assert set(registry["peers"].keys()) == {"learner", "metrics"}


def test_main_returns_error_when_plan_missing(tmp_path):
    rc = topology_bootstrap.main(["--job-dir", str(tmp_path)])
    assert rc == 1
    assert not (tmp_path / "registry.json").exists()


def _unused_path_guard() -> Path:
    return Path("/")
