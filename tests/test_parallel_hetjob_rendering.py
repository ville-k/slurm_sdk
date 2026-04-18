"""Hetjob rendering tests — multi-pool ``#SBATCH`` header + ``--het-group``.

Phase 6 introduces the hetjob path. The rendered batch script now carries
N ``#SBATCH`` header blocks separated by ``#SBATCH hetjob`` dividers (one
per pool in declaration order) and every per-peer ``srun`` line gets
``--het-group=<component_index>`` so it lands in the right component.

These tests lock the shape of the rendered script:

- Single-pool submissions render byte-compatibly with the Phase 2 output
  (no ``hetjob`` divider, no ``--het-group`` flag).
- Multi-pool submissions emit one header block per pool with per-pool
  resource directives, divided by ``#SBATCH hetjob``.
- Each peer's srun line routes to the correct component.

The per-peer srun command lines live inside the base64-encoded plan.json
heredoc, so the tests decode it with the same helper the existing
rendering tests use.
"""

from __future__ import annotations

import base64
import re

from slurm import Peer, Pool, Topology, task
from slurm.callbacks import BaseCallback
from slurm.packaging.base import PackagingStrategy
from slurm.parallel import _build_spec
from slurm.parallel.plan import Plan
from slurm.parallel.rendering import render_parallel_script
from slurm.parallel.validation import validate_spec


class _DummyStrategy(PackagingStrategy):
    """Minimal packaging strategy — no wrapping, deterministic setup lines."""

    def prepare(self, task, cluster):
        return {"status": "ok"}

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        return ["echo setup"]

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None):
        return ["echo cleanup"]


@task(cpus_per_task=4, mem="8G", gpus_per_task=1)
def _learner(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=2, mem="2G")
def _sim(env_id: int) -> int:
    return env_id


@task(cpus_per_task=1, mem="1G")
def _metrics() -> None:
    pass


def _make_spec(*peers, topology=None, **kwargs):
    spec = _build_spec(
        positional=tuple(peers),
        named={},
        topology=topology,
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
        pre_submission_id="hetjob1",
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
    decoded = base64.b64decode(match.group(1).strip()).decode("utf-8")
    return Plan.from_json(decoded)


# ---------------------------------------------------------------------------
# Single-pool: unchanged from Phase 2/5
# ---------------------------------------------------------------------------


def test_single_pool_render_has_no_hetjob_divider(tmp_path):
    spec = _make_spec(
        Peer(_metrics, on_failure="continue"),
    )
    script = _render(spec, tmp_path)
    # No hetjob divider and no --het-group flag when there is just one pool.
    assert "#SBATCH hetjob" not in script
    plan = _extract_plan(script)
    for peer in plan.peers:
        assert "--het-group=" not in peer.srun_command_line
        assert peer.component_index == 0


def test_single_pool_plan_pool_names_unchanged(tmp_path):
    spec = _make_spec(Peer(_metrics, on_failure="continue"))
    script = _render(spec, tmp_path)
    plan = _extract_plan(script)
    # Phase 2's wire shape: one pool_name, "default".
    assert plan.pool_names == ["default"]


# ---------------------------------------------------------------------------
# Two-pool hetjob
# ---------------------------------------------------------------------------


def test_two_pool_hetjob_emits_one_divider(tmp_path):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
            "cpu": Pool(nodes=2, cpus_per_node=8),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={"lr": 1e-3}), pool="gpu", leader=True),
        Peer(_sim.partial(env_id=0), pool="cpu", name="_sim"),
        topology=topo,
    )
    script = _render(spec, tmp_path)

    # Exactly one divider between two components — no trailing divider.
    assert script.count("#SBATCH hetjob") == 1
    # Per-pool directives appear — gpu pool has --gpus-per-node=1, cpu has
    # its own --cpus-per-task=8.
    assert "#SBATCH --gpus-per-node=1" in script
    # The cpu pool's per-task CPU budget flows through cpus_per_node → cpus_per_task.
    assert "#SBATCH --cpus-per-task=8" in script
    # One shebang only.
    assert script.count("#!/bin/bash") == 1


def test_two_pool_hetjob_peers_carry_het_group(tmp_path):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1, cpus_per_node=4),
            "cpu": Pool(nodes=2, cpus_per_node=8),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_sim.partial(env_id=0), pool="cpu", name="_sim"),
        topology=topo,
    )
    script = _render(spec, tmp_path)

    plan = _extract_plan(script)
    learner = plan.peer_by_name("_learner")
    sim = plan.peer_by_name("_sim")

    # Component indices map to pool declaration order.
    assert learner.component_index == 0
    assert sim.component_index == 1
    # Every peer's srun line carries the matching --het-group flag.
    assert "--het-group=0" in learner.srun_command_line
    assert "--het-group=1" in sim.srun_command_line
    # Plan pool_names follows declaration order.
    assert plan.pool_names == ["gpu", "cpu"]


def test_two_pool_hetjob_components_metadata(tmp_path):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=3, cpus_per_node=16),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_metrics, pool="cpu", on_failure="continue"),
        topology=topo,
    )
    script = _render(spec, tmp_path)
    plan = _extract_plan(script)
    components = plan.effective_components()
    assert [c.pool for c in components] == ["gpu", "cpu"]
    assert [c.index for c in components] == [0, 1]
    assert [c.nodes for c in components] == [1, 3]


# ---------------------------------------------------------------------------
# Three-pool hetjob
# ---------------------------------------------------------------------------


def test_three_pool_hetjob_emits_two_dividers(tmp_path):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=2, cpus_per_node=4),
            "aux": Pool(nodes=1, cpus_per_node=2),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_metrics, pool="cpu", on_failure="continue"),
        Peer(_metrics, pool="aux", name="aux_metrics", on_failure="continue"),
        topology=topo,
    )
    script = _render(spec, tmp_path)

    # N pools → N-1 dividers (no trailing divider).
    assert script.count("#SBATCH hetjob") == 2

    plan = _extract_plan(script)
    assert plan.pool_names == ["gpu", "cpu", "aux"]
    # Component indices in declaration order.
    assert plan.peer_by_name("_learner").component_index == 0
    assert plan.peer_by_name("_metrics").component_index == 1
    assert plan.peer_by_name("aux_metrics").component_index == 2
    assert "--het-group=0" in plan.peer_by_name("_learner").srun_command_line
    assert "--het-group=2" in plan.peer_by_name("aux_metrics").srun_command_line


def test_hetjob_only_first_component_has_shebang(tmp_path):
    """``#!/bin/bash`` must only precede the first component's directives."""
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=1, cpus_per_node=4),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_metrics, pool="cpu", on_failure="continue"),
        topology=topo,
    )
    script = _render(spec, tmp_path)
    # Shebang appears once, ahead of the first #SBATCH directive.
    assert script.count("#!/bin/bash") == 1
    first_sbatch = script.find("#SBATCH ")
    first_shebang = script.find("#!/bin/bash")
    assert first_shebang < first_sbatch


def test_hetjob_component_0_has_output_error(tmp_path):
    """Only component 0 needs --output/--error; later components inherit them."""
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=1, cpus_per_node=4),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_metrics, pool="cpu", on_failure="continue"),
        topology=topo,
    )
    script = _render(spec, tmp_path)
    # Output/error paths appear exactly once — on component 0.
    assert script.count("#SBATCH --output=") == 1
    assert script.count("#SBATCH --error=") == 1


def test_hetjob_plan_peer_pool_assignment_honoured(tmp_path):
    topo = Topology(
        pools={
            "gpu": Pool(nodes=1, gpus_per_node=1),
            "cpu": Pool(nodes=1, cpus_per_node=4),
        },
        default_pool="gpu",
    )
    spec = _make_spec(
        Peer(_learner.partial(cfg={}), pool="gpu", leader=True),
        Peer(_metrics, pool="cpu", on_failure="continue"),
        topology=topo,
    )
    script = _render(spec, tmp_path)
    plan = _extract_plan(script)
    learner = plan.peer_by_name("_learner")
    metrics = plan.peer_by_name("_metrics")
    assert learner.pool == "gpu"
    assert metrics.pool == "cpu"
    # --export carries the pool name so the runtime JobContext gets the
    # right pool attribution at peer startup.
    assert "SLURM_SDK_PEER_POOL=gpu" in learner.srun_command_line
    assert "SLURM_SDK_PEER_POOL=cpu" in metrics.srun_command_line
