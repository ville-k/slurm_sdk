"""Tests for ``ctx.shared_dir`` — the per-allocation shared directory.

Exercises:

- Bootstrap creates ``$JOB_DIR/shared/`` before any peer launches.
- ``build_job_context()`` picks up ``SLURM_SDK_SHARED_DIR`` from the env.
- Supervisor exports ``SLURM_SDK_SHARED_DIR`` into each peer's Popen env.
- Cross-peer reads via the directory work.
"""

from __future__ import annotations

import json
from pathlib import Path

from slurm.parallel import topology_bootstrap, topology_supervisor
from slurm.parallel.plan import Plan, PlanPeer, write_plan
from slurm.parallel.topology_supervisor import _launch_peer
from slurm.runtime import build_job_context


def _write_plan(job_dir: Path) -> None:
    plan = Plan(
        peers=[
            PlanPeer(
                name="peer1",
                pool="default",
                leader=False,
                on_failure="kill",
                srun_command_line="srun true",
            )
        ],
        grace_period_seconds=5,
        pool_names=["default"],
    )
    write_plan(job_dir / "plan.json", plan)


def test_bootstrap_creates_shared_dir(tmp_path):
    _write_plan(tmp_path)
    # Run bootstrap in-process — it reads plan.json and writes registry.json.
    rc = topology_bootstrap.main(["--job-dir", str(tmp_path)])
    assert rc == 0
    assert (tmp_path / "shared").is_dir()


def test_build_job_context_picks_up_shared_dir_env(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    env = {
        "SLURM_JOB_ID": "42",
        "SLURM_SDK_SHARED_DIR": str(shared),
    }
    ctx = build_job_context(env=env)
    assert ctx.shared_dir == shared


def test_ctx_shared_dir_is_none_outside_parallel():
    env = {"SLURM_JOB_ID": "42"}
    ctx = build_job_context(env=env)
    assert ctx.shared_dir is None


def test_supervisor_exports_shared_dir_to_peer_env(tmp_path, monkeypatch):
    """``_launch_peer`` sets ``SLURM_SDK_SHARED_DIR`` on the peer's Popen env."""
    shared = tmp_path / "shared"
    shared.mkdir()
    registry = tmp_path / "registry.json"
    registry.write_text('{"peers": {}, "nodes": {}}')

    captured_env: dict = {}

    class FakePopen:
        def __init__(self, cmd, env=None, **kwargs):  # noqa: ARG002
            captured_env.update(env or {})
            self.pid = 1
            self.returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(topology_supervisor.subprocess, "Popen", FakePopen)

    peer = PlanPeer(
        name="p",
        pool="default",
        leader=False,
        on_failure="kill",
        srun_command_line="srun echo hi",
    )
    _launch_peer(peer, registry_path=registry, shared_dir=shared)
    assert captured_env.get("SLURM_SDK_SHARED_DIR") == str(shared)


def test_cross_peer_reads_work_through_shared_dir(tmp_path):
    """Two peer contexts sharing the same ``shared_dir`` can read each other's files."""
    shared = tmp_path / "shared"
    shared.mkdir()

    env_a = {
        "SLURM_JOB_ID": "42",
        "SLURM_SDK_PEER_NAME": "a",
        "SLURM_SDK_SHARED_DIR": str(shared),
    }
    env_b = {
        "SLURM_JOB_ID": "42",
        "SLURM_SDK_PEER_NAME": "b",
        "SLURM_SDK_SHARED_DIR": str(shared),
    }
    ctx_a = build_job_context(env=env_a)
    ctx_b = build_job_context(env=env_b)

    assert ctx_a.shared_dir == ctx_b.shared_dir

    # a writes, b reads.
    payload = {"hello": "from_a"}
    (ctx_a.shared_dir / "handshake.json").write_text(json.dumps(payload))
    assert json.loads((ctx_b.shared_dir / "handshake.json").read_text()) == payload
