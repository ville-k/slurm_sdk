"""Structural tests for the parallel-job renderer.

These check that the rendered script contains the expected shape — one
sbatch header, per-peer srun invocations tagged with ``--step peer:<name>``,
shared packaging setup, and the temporary shell supervisor — without pinning
every character (which would make every whitespace tweak a test change).
"""

from __future__ import annotations

from slurm import task
from slurm.callbacks import BaseCallback
from slurm.packaging.base import PackagingStrategy
from slurm.parallel import _build_spec
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

    # One srun per peer, each tagged with the peer-step selector.
    assert "--step peer:_train" in script
    assert "--step peer:_metrics" in script

    # Distinct per-peer args/kwargs pickle targets.
    assert 'base64 -d > "peer__train_args.pkl"' in script
    assert 'base64 -d > "peer__train_kwargs.pkl"' in script
    assert 'base64 -d > "peer__metrics_args.pkl"' in script
    assert 'base64 -d > "peer__metrics_kwargs.pkl"' in script

    # Peer result files mirror Job._result_filename convention so get_result()
    # finds them without overriding the derived filename.
    assert "slurm_job_abc123_peer__train_result.pkl" in script
    assert "slurm_job_abc123_peer__metrics_result.pkl" in script

    # Each srun carries the peer-identity env vars the runner reads.
    assert "SLURM_SDK_PEER_NAME=_train" in script
    assert "SLURM_SDK_PEER_POOL=default" in script

    # Shared packaging setup emitted once.
    assert script.count("echo setup") == 1

    # Temporary shell supervisor: trap + scancel + leader wait.
    assert "trap 'scancel --signal=TERM" in script
    assert "LEADER_EXIT=$?" in script


def test_all_peers_symmetric_emit_wait_n_loop(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_ocean.partial(cfg={})),
        Peer(_atmo.partial(cfg={})),
    )
    script = _render(spec, tmp_path)

    # No leader → fail-fast wait -n loop.
    assert "LEADER_EXIT=" not in script
    assert "wait -n" in script
    assert "FINAL_EXIT" in script

    # Both peers still have their own srun commands.
    assert "--step peer:_ocean" in script
    assert "--step peer:_atmo" in script


def test_on_failure_continue_policy_recorded_for_shell_supervisor(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_ocean.partial(cfg={})),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    # The shell supervisor records each peer's failure policy so it can
    # distinguish kill vs continue on exit.
    assert '"continue"' in script
    assert '"kill"' in script


def test_single_peer_renders_minimal_script(tmp_path):
    from slurm import Peer

    spec = _make_spec(Peer(_train.partial(cfg={"lr": 0.01}), leader=True))
    script = _render(spec, tmp_path)

    assert "--step peer:_train" in script
    # Leader + no siblings → LEADER_EXIT path still fires.
    assert "LEADER_EXIT=$?" in script


def test_peer_env_vars_exported_on_srun(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={}), leader=True),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    # --export=ALL preserves the batch env (JOB_DIR, PYTHONPATH, etc.) while
    # still adding the peer-identity vars.
    for peer_name in ("_train", "_metrics"):
        assert f"--export=ALL,SLURM_SDK_PEER_NAME={peer_name}" in script


def test_script_includes_runner_module_and_function(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_train.partial(cfg={}), leader=True),
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)

    assert "-m slurm.runner" in script
    assert "--module " in script
    assert '--function "_train"' in script
    assert '--function "_metrics"' in script
