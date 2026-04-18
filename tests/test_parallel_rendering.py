"""Structural tests for the parallel-job renderer.

These check that the rendered script contains the expected shape — one
sbatch header, shared packaging setup, a base64-encoded supervisor plan,
and the bootstrap+supervisor invocation — without pinning every character
(which would make every whitespace tweak a test change).

The per-peer ``srun`` commands live inside the plan.json heredoc, so tests
that need to inspect them decode the heredoc and load it as a :class:`Plan`.
"""

from __future__ import annotations

import base64
import re

from slurm import task
from slurm.callbacks import BaseCallback
from slurm.packaging.base import PackagingStrategy
from slurm.parallel import _build_spec
from slurm.parallel.plan import Plan
from slurm.parallel.rendering import render_parallel_script
from slurm.parallel.validation import validate_spec


class _DummyStrategy(PackagingStrategy):
    """Minimal packaging strategy: echoes setup/cleanup lines, no wrapping."""

    def prepare(self, task, cluster):
        return {"status": "ok"}

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        return ["echo setup"]

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None):
        return ["echo cleanup"]


@task(cpus_per_task=4, mem="8G")
def _train(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, mem="1G")
def _metrics() -> None:
    pass


@task(cpus_per_task=2, mem="2G")
def _atmo(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=2, mem="2G")
def _ocean(cfg: dict) -> dict:
    return cfg


def _make_spec(*peers, **kwargs):
    spec = _build_spec(
        positional=tuple(peers),
        named={},
        topology=kwargs.pop("topology", None),
        time=kwargs.pop("time", None),
        account=kwargs.pop("account", None),
        qos=kwargs.pop("qos", None),
        reservation=kwargs.pop("reservation", None),
        network=kwargs.pop("network", None),
        grace_period_seconds=kwargs.pop("grace_period_seconds", 10),
    )
    validate_spec(spec)
    return spec


def _render(spec, tmp_path):
    return render_parallel_script(
        spec=spec,
        packaging_strategy=_DummyStrategy({}),
        target_job_dir=str(tmp_path),
        pre_submission_id="abc123",
        cluster=None,
        task_defaults={},
        sbatch_overrides={},
        callbacks=[BaseCallback()],
    )


_PLAN_HEREDOC_RE = re.compile(
    r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
    re.DOTALL,
)


def _extract_plan(script: str) -> Plan:
    match = _PLAN_HEREDOC_RE.search(script)
    assert match is not None, "plan.json heredoc not found in rendered script"
    encoded = match.group(1).strip()
    decoded = base64.b64decode(encoded).decode("utf-8")
    return Plan.from_json(decoded)


def test_two_peers_render_distinct_srun_commands(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={"lr": 0.001}), leader=True),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    # Single sbatch header.
    assert script.count("#!/bin/bash") == 1
    assert "#SBATCH --job-name=" in script

    # Distinct per-peer args/kwargs pickle targets (still emitted as heredocs
    # so the supervisor's srun invocations find them).
    assert 'base64 -d > "peer__train_args.pkl"' in script
    assert 'base64 -d > "peer__train_kwargs.pkl"' in script
    assert 'base64 -d > "peer__metrics_args.pkl"' in script
    assert 'base64 -d > "peer__metrics_kwargs.pkl"' in script

    # Shared packaging setup emitted once.
    assert script.count("echo setup") == 1

    # Bootstrap + supervisor replace the old inline shell supervisor.
    assert "slurm.parallel.topology_bootstrap" in script
    assert "slurm.parallel.topology_supervisor" in script
    assert 'exec "$PY_EXEC_RESOLVED" -m slurm.parallel.topology_supervisor' in script

    # Decode the plan and inspect peer entries.
    plan = _extract_plan(script)
    names = sorted(p.name for p in plan.peers)
    assert names == ["_metrics", "_train"]
    train = plan.peer_by_name("_train")
    metrics = plan.peer_by_name("_metrics")
    assert train.leader is True
    assert train.on_failure == "kill"
    assert metrics.leader is False
    assert metrics.on_failure == "continue"
    assert "--step peer:_train" in train.srun_command_line
    assert "--step peer:_metrics" in metrics.srun_command_line
    # Peer result files follow Job._result_filename convention so get_result()
    # finds them without overriding the derived filename.
    assert "slurm_job_abc123_peer__train_result.pkl" in train.srun_command_line
    assert "slurm_job_abc123_peer__metrics_result.pkl" in metrics.srun_command_line
    # Each srun carries the peer-identity env vars the runner reads.
    assert "SLURM_SDK_PEER_NAME=_train" in train.srun_command_line
    assert "SLURM_SDK_PEER_POOL=default" in train.srun_command_line


def test_all_peers_symmetric_record_kill_policy(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_ocean.partial(cfg={})),
        Peer(_atmo.partial(cfg={})),
    )
    script = _render(spec, tmp_path)

    # No inline wait -n supervisor any more — the Python supervisor owns it.
    assert "wait -n" not in script
    assert "LEADER_EXIT" not in script

    plan = _extract_plan(script)
    for peer in plan.peers:
        assert peer.leader is False
        assert peer.on_failure == "kill"
    names = sorted(p.name for p in plan.peers)
    assert names == ["_atmo", "_ocean"]


def test_on_failure_continue_policy_recorded_in_plan(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_ocean.partial(cfg={})),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    assert plan.peer_by_name("_ocean").on_failure == "kill"
    assert plan.peer_by_name("_metrics").on_failure == "continue"


def test_single_peer_renders_minimal_script(tmp_path):
    from slurm import Peer

    spec = _make_spec(Peer(_train.partial(cfg={"lr": 0.01}), leader=True))
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    assert len(plan.peers) == 1
    assert plan.peers[0].leader is True
    assert "--step peer:_train" in plan.peers[0].srun_command_line


def test_peer_env_vars_exported_on_srun(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={}), leader=True),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    # --export=ALL preserves the batch env (JOB_DIR, PYTHONPATH, etc.) while
    # still adding the peer-identity vars.
    for peer in plan.peers:
        assert f"--export=ALL,SLURM_SDK_PEER_NAME={peer.name}" in peer.srun_command_line


def test_script_includes_runner_module_and_function(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={}), leader=True),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    for peer in plan.peers:
        assert "-m slurm.runner" in peer.srun_command_line
        assert "--module " in peer.srun_command_line
    assert '--function "_train"' in plan.peer_by_name("_train").srun_command_line
    assert '--function "_metrics"' in plan.peer_by_name("_metrics").srun_command_line


def test_plan_metadata_round_trips(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={}), leader=True),
        Peer(_metrics, on_failure="continue"),
        grace_period_seconds=7,
    )
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    assert plan.grace_period_seconds == 7
    assert plan.pool_names == ["default"]
    assert plan.pre_submission_id == "abc123"
