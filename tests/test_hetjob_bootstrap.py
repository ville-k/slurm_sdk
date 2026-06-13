"""Bootstrap tests for hetjob / multi-component allocations (Phase 6).

Slurm exposes per-component nodelists via
``SLURM_JOB_NODELIST_HET_GROUP_<N>``. The bootstrap reads each env var,
expands it to hostnames, and seeds the registry skeleton so every peer
reports hostnames from the correct pool.

The tests stub out :func:`subprocess.run` so the in-process nodelist
expander (same one the runtime uses) is exercised instead of real
``scontrol``. This keeps the tests hermetic on hosts without Slurm.
"""

from __future__ import annotations

from typing import Optional

from slurm.parallel import topology_bootstrap
from slurm.parallel.plan import Plan, PlanComponent, PlanPeer, write_plan
from slurm.parallel.registry import read_registry


def _plan_with_components() -> Plan:
    return Plan(
        peers=[
            PlanPeer(
                name="learner",
                pool="gpu",
                leader=True,
                on_failure="kill",
                srun_command_line="echo learner",
                component_index=0,
            ),
            PlanPeer(
                name="sim",
                pool="cpu",
                leader=False,
                on_failure="continue",
                srun_command_line="echo sim",
                replica_count=4,
                component_index=1,
            ),
            PlanPeer(
                name="collector",
                pool="aux",
                leader=False,
                on_failure="continue",
                srun_command_line="echo collector",
                component_index=2,
            ),
        ],
        grace_period_seconds=5,
        pool_names=["gpu", "cpu", "aux"],
        pre_submission_id="hetjob-bootstrap",
        components=[
            PlanComponent(index=0, pool="gpu", nodes=1),
            PlanComponent(index=1, pool="cpu", nodes=4),
            PlanComponent(index=2, pool="aux", nodes=1),
        ],
    )


def _install_no_scontrol(monkeypatch) -> None:
    """Force the in-process nodelist expander to run for every ``scontrol`` call."""
    import subprocess

    def _fake_run(*args, **kwargs):
        raise FileNotFoundError("no scontrol")

    monkeypatch.setattr(subprocess, "run", _fake_run)


def test_resolve_component_hostnames_reads_per_component_env(monkeypatch):
    _install_no_scontrol(monkeypatch)
    plan = _plan_with_components()

    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_0", "gpu-01")
    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_1", "cpu-[01-04]")
    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_2", "aux-01")
    # Leave the flat SLURM_JOB_NODELIST unset so the bootstrap must stitch
    # components together.
    monkeypatch.delenv("SLURM_JOB_NODELIST", raising=False)
    monkeypatch.delenv("SLURM_NODELIST", raising=False)

    flat, per_comp = topology_bootstrap._resolve_component_hostnames(plan)

    assert per_comp is not None
    assert list(per_comp[0]) == ["gpu-01"]
    assert list(per_comp[1]) == ["cpu-01", "cpu-02", "cpu-03", "cpu-04"]
    assert list(per_comp[2]) == ["aux-01"]
    # Flat hostnames are concatenated in component order when the flat env
    # var is absent.
    assert list(flat) == ["gpu-01", "cpu-01", "cpu-02", "cpu-03", "cpu-04", "aux-01"]


def test_resolve_component_hostnames_single_pool_falls_back(monkeypatch):
    """Single-pool (no HET env) returns per_component=None."""
    _install_no_scontrol(monkeypatch)
    plan = Plan(
        peers=[
            PlanPeer(
                name="solo",
                pool="default",
                leader=True,
                on_failure="kill",
                srun_command_line="echo solo",
            ),
        ],
        grace_period_seconds=5,
        pool_names=["default"],
    )
    monkeypatch.setenv("SLURM_JOB_NODELIST", "node-[01-02]")
    for key in ("SLURM_JOB_NODELIST_HET_GROUP_0", "SLURM_JOB_NODELIST_HET_GROUP_1"):
        monkeypatch.delenv(key, raising=False)

    flat, per_comp = topology_bootstrap._resolve_component_hostnames(plan)

    assert list(flat) == ["node-01", "node-02"]
    assert per_comp is None


def test_build_registry_skeleton_seeds_per_component_hostnames():
    plan = _plan_with_components()
    per_component = {
        0: ("gpu-01",),
        1: ("cpu-01", "cpu-02", "cpu-03", "cpu-04"),
        2: ("aux-01",),
    }
    skeleton = topology_bootstrap.build_registry_skeleton(
        plan,
        hostnames=("gpu-01", "cpu-01", "cpu-02", "cpu-03", "cpu-04", "aux-01"),
        per_component_hostnames=per_component,
    )

    # Each peer knows the full hostname slice of its component (for
    # ordinal lookups); its own ``hostname`` stays empty until the
    # runner publishes the actual value — bootstrap no longer round-robins.
    assert skeleton["peers"]["learner"][0]["hostnames"] == ["gpu-01"]
    assert skeleton["peers"]["learner"][0]["hostname"] == ""
    assert skeleton["peers"]["learner"][0]["component_index"] == 0

    sim_entries = skeleton["peers"]["sim"]
    assert len(sim_entries) == 4
    assert all(
        e["hostnames"] == ["cpu-01", "cpu-02", "cpu-03", "cpu-04"] for e in sim_entries
    )
    assert all(e["hostname"] == "" for e in sim_entries)
    assert all(e["component_index"] == 1 for e in sim_entries)

    assert skeleton["peers"]["collector"][0]["hostnames"] == ["aux-01"]
    assert skeleton["peers"]["collector"][0]["hostname"] == ""
    assert skeleton["peers"]["collector"][0]["component_index"] == 2


def test_build_registry_skeleton_unpinned_replicas_stay_pending():
    """Replica count > component hostnames — every replica stays empty.

    Bootstrap no longer fabricates a round-robin placement; each
    replica's runner publishes the real hostname at startup.
    """
    plan = Plan(
        peers=[
            PlanPeer(
                name="worker",
                pool="cpu",
                leader=False,
                on_failure="kill",
                srun_command_line="echo worker",
                replica_count=5,
                component_index=0,
            ),
        ],
        grace_period_seconds=5,
        pool_names=["cpu"],
        components=[PlanComponent(index=0, pool="cpu", nodes=2)],
    )
    skeleton = topology_bootstrap.build_registry_skeleton(
        plan,
        hostnames=("cpu-01", "cpu-02"),
        per_component_hostnames={0: ("cpu-01", "cpu-02")},
    )
    hosts = [e["hostname"] for e in skeleton["peers"]["worker"]]
    assert hosts == ["", "", "", "", ""]
    # The full component hostname list is still recorded for reference.
    assert all(
        e["hostnames"] == ["cpu-01", "cpu-02"] for e in skeleton["peers"]["worker"]
    )


def test_bootstrap_main_writes_hetjob_registry(tmp_path, monkeypatch):
    """End-to-end bootstrap main() on a canned hetjob environment."""
    _install_no_scontrol(monkeypatch)
    plan = _plan_with_components()
    write_plan(tmp_path / "plan.json", plan)

    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_0", "gpu-01")
    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_1", "cpu-[01-02]")
    monkeypatch.setenv("SLURM_JOB_NODELIST_HET_GROUP_2", "aux-01")
    monkeypatch.delenv("SLURM_JOB_NODELIST", raising=False)

    rc = topology_bootstrap.main(["--job-dir", str(tmp_path)])
    assert rc == 0

    registry = read_registry(tmp_path / "registry.json")
    # Learner sees only the GPU hostnames (slice), but its own hostname
    # is empty until the runner publishes.
    assert registry["peers"]["learner"][0]["hostnames"] == ["gpu-01"]
    assert registry["peers"]["learner"][0]["hostname"] == ""
    # Sim peer (2 replicas) has the full component's hostname list but
    # no per-replica pin.
    for entry in registry["peers"]["sim"]:
        assert entry["hostnames"] == ["cpu-01", "cpu-02"]
        assert entry["hostname"] == ""
    # Collector in aux — unpinned, pending.
    assert registry["peers"]["collector"][0]["hostnames"] == ["aux-01"]
    assert registry["peers"]["collector"][0]["hostname"] == ""


def _unused() -> Optional[int]:
    return None
