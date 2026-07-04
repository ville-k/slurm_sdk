"""Phase 10 — per-peer packaging for ``parallel(...)`` submissions.

These tests drive the *rendering* side of per-peer packaging (where
heterogeneity actually shows up — each peer's srun gets its own
wrap_execution_command output) and the *preparation* side in
``_parallel_submission`` (dedup on resolved config, inherit resolution,
validation errors for unresolvable inherits).

The container + wheel strategies are not exercised for real: they make
network calls or shell out to ``docker``. Instead these tests use a
``FakeStrategy`` that records a distinct marker per instance and emits a
predictable ``wrap_execution_command`` output we can grep for.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

import pytest

from slurm import task
from slurm.callbacks import BaseCallback
from slurm.errors import TopologyError
from slurm.packaging.base import PackagingStrategy
from slurm.parallel import _build_spec
from slurm.parallel.rendering import render_parallel_script
from slurm.parallel.validation import validate_spec
from slurm._parallel_submission import (
    _packaging_config_key,
    _prepare_per_peer_packaging,
)


# ---------------------------------------------------------------------------
# Fake strategies — isolate tests from real container / venv work.
# ---------------------------------------------------------------------------


class _FakeStrategy(PackagingStrategy):
    """Records prepare() calls and emits a greppable wrap_execution_command."""

    prepare_calls: List[tuple[str, str]] = []

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.label = (self.config or {}).get("image", "bare")
        self._prepared = False

    def prepare(self, task, cluster):
        type(self).prepare_calls.append(
            (self.label, getattr(task, "__name__", str(task)))
        )
        self._prepared = True
        return {"status": "ok", "image": self.label}

    def generate_setup_commands(
        self,
        task: Union["PackagingStrategy", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        return [f"echo 'SETUP {self.label}'"]

    def generate_cleanup_commands(
        self,
        task: Union["PackagingStrategy", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        return []

    def wrap_execution_command(
        self, command: str, task=None, job_id=None, job_dir=None
    ) -> str:
        return f"wrap[{self.label}] {command}"


class _BareStrategy(PackagingStrategy):
    """Stand-in for ``packaging="none"`` — wrap is a no-op."""

    def prepare(self, task, cluster):
        return {"status": "ok"}

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        return ["echo 'SETUP bare'"]

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None):
        return []


@pytest.fixture(autouse=True)
def _reset_fake_strategy_state():
    _FakeStrategy.prepare_calls = []
    yield


# ---------------------------------------------------------------------------
# Tasks used across the rendering tests. Packaging configs picked so the
# dedup keys are both realistic and easy to assert against.
# ---------------------------------------------------------------------------


@task(cpus_per_task=1, packaging="container:registry.example/alpha:v1")
def _alpha(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, packaging="container:registry.example/beta:v1")
def _beta(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, packaging="container:registry.example/gamma:v1")
def _gamma(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, packaging="container:registry.example/alpha:v1")
def _alpha_clone(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, packaging="none")
def _bare(cfg: dict) -> dict:
    return cfg


@task(cpus_per_task=1, packaging="inherit")
def _inheritor(cfg: dict) -> dict:
    return cfg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_spec(*peers):
    spec = _build_spec(
        positional=tuple(peers),
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    validate_spec(spec)
    return spec


def _render_with_strategies(spec, tmp_path, strategies):
    return render_parallel_script(
        spec=spec,
        packaging_strategy=next(iter(strategies.values())),
        target_job_dir=str(tmp_path),
        pre_submission_id="hetero123",
        cluster=None,
        task_defaults={},
        sbatch_overrides={},
        callbacks=[BaseCallback()],
        peer_packaging_strategies=strategies,
    )


# ---------------------------------------------------------------------------
# Rendering — per-peer wrapping + deduped setup
# ---------------------------------------------------------------------------


def test_three_distinct_images_emit_three_setup_blocks_and_three_wrappers(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_beta.partial(cfg={}), on_failure="continue"),
        Peer(_gamma.partial(cfg={}), on_failure="continue"),
    )

    strategies = {
        "_alpha": _FakeStrategy({"image": "alpha"}),
        "_beta": _FakeStrategy({"image": "beta"}),
        "_gamma": _FakeStrategy({"image": "gamma"}),
    }
    script = _render_with_strategies(spec, tmp_path, strategies)

    # Three distinct setup blocks — one per unique strategy.
    assert script.count("SETUP alpha") == 1
    assert script.count("SETUP beta") == 1
    assert script.count("SETUP gamma") == 1

    # Each peer's srun command is wrapped with its own strategy's label.
    # The wrapped commands live inside the base64 plan heredoc, so grepping
    # the raw script hits them directly (the heredoc is plaintext).
    from slurm.parallel.plan import Plan
    import base64
    import re

    match = re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        script,
        re.DOTALL,
    )
    assert match is not None
    plan = Plan.from_json(base64.b64decode(match.group(1).strip()).decode())

    assert "wrap[alpha]" in plan.peer_by_name("_alpha").srun_command_line
    assert "wrap[beta]" in plan.peer_by_name("_beta").srun_command_line
    assert "wrap[gamma]" in plan.peer_by_name("_gamma").srun_command_line


def test_shared_strategy_emits_one_setup_and_one_wrapper(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_alpha.partial(cfg={}), name="b", on_failure="continue"),
        Peer(_alpha.partial(cfg={}), name="c", on_failure="continue"),
    )

    shared = _FakeStrategy({"image": "alpha"})
    strategies = {name: shared for name in ("_alpha", "b", "c")}
    script = _render_with_strategies(spec, tmp_path, strategies)

    # Exactly one setup block even with three peers.
    assert script.count("SETUP alpha") == 1

    # Per-peer srun commands live inside the base64 plan heredoc. Decode
    # the plan and assert every peer's line carries the wrapper.
    from slurm.parallel.plan import Plan
    import base64
    import re

    match = re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        script,
        re.DOTALL,
    )
    assert match is not None
    plan = Plan.from_json(base64.b64decode(match.group(1).strip()).decode())

    wrapped_count = sum(
        1 for peer in plan.peers if "wrap[alpha]" in peer.srun_command_line
    )
    assert wrapped_count == 3


def test_mixed_container_and_none_keeps_none_peer_bare(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_bare.partial(cfg={}), on_failure="continue"),
    )

    container = _FakeStrategy({"image": "alpha"})
    bare = _BareStrategy({})
    strategies = {"_alpha": container, "_bare": bare}
    script = _render_with_strategies(spec, tmp_path, strategies)

    assert "SETUP alpha" in script
    assert "SETUP bare" in script

    from slurm.parallel.plan import Plan
    import base64
    import re

    match = re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        script,
        re.DOTALL,
    )
    plan = Plan.from_json(base64.b64decode(match.group(1).strip()).decode())

    # Container-wrapped command for the leader, bare srun for the sidecar.
    alpha_cmd = plan.peer_by_name("_alpha").srun_command_line
    bare_cmd = plan.peer_by_name("_bare").srun_command_line
    assert "wrap[alpha]" in alpha_cmd
    assert "wrap[" not in bare_cmd


def test_missing_peer_entry_in_strategies_raises(tmp_path):
    from slurm import Peer

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_beta.partial(cfg={}), on_failure="continue"),
    )
    # Only one of the two peers is mapped — rendering should refuse.
    partial = {"_alpha": _FakeStrategy({"image": "alpha"})}
    with pytest.raises(RuntimeError, match="missing entries"):
        _render_with_strategies(spec, tmp_path, partial)


# ---------------------------------------------------------------------------
# Config-key dedup logic
# ---------------------------------------------------------------------------


def test_packaging_config_key_ignores_auto_tag():
    a = {"type": "container", "image": "foo", "tag": "build-aaaaaaaaaaaa"}
    b = {"type": "container", "image": "foo", "tag": "build-bbbbbbbbbbbb"}
    assert _packaging_config_key(a) == _packaging_config_key(b)


def test_packaging_config_key_differs_by_image():
    a = {"type": "container", "image": "foo"}
    b = {"type": "container", "image": "bar"}
    assert _packaging_config_key(a) != _packaging_config_key(b)


def test_packaging_config_key_none_vs_empty():
    assert _packaging_config_key(None) == _packaging_config_key({})


# ---------------------------------------------------------------------------
# Submission-side preparation — _prepare_per_peer_packaging
# ---------------------------------------------------------------------------


class _RecordingCluster:
    """Stand-in Cluster exposing just enough surface for prepare() tests.

    ``prepare_packaging_strategy`` reaches into ``cluster._prebuilt_dependency_images``
    and fires packaging callbacks via ``_dispatch_callbacks``. We stub both.
    """

    def __init__(self):
        self._prebuilt_dependency_images: Dict[str, str] = {}
        self.default_packaging = None
        self.default_packaging_kwargs: Dict[str, Any] = {}
        self.packaging_defaults = None
        self.callbacks: list = []
        self.backend_type = "fake"

    def _dispatch_callbacks(self, *args, **kwargs):
        pass


def _patch_strategy_factory(monkeypatch, factory):
    """Make ``get_packaging_strategy`` return whatever ``factory(config)`` says."""
    monkeypatch.setattr(
        "slurm._submission.get_packaging_strategy",
        factory,
    )


def test_prepare_dedupes_equivalent_configs(monkeypatch):
    from slurm import Peer

    prepared: List[_FakeStrategy] = []

    def factory(config):
        s = _FakeStrategy(config)
        prepared.append(s)
        return s

    _patch_strategy_factory(monkeypatch, factory)

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_alpha_clone.partial(cfg={}), on_failure="continue"),
    )
    cluster = _RecordingCluster()
    result = _prepare_per_peer_packaging(cluster, spec)

    # Two peers but only one strategy instance — same config key.
    assert len(prepared) == 1
    assert result["_alpha"] is result["_alpha_clone"]


def test_prepare_creates_one_per_unique_config(monkeypatch):
    from slurm import Peer

    prepared: List[_FakeStrategy] = []

    def factory(config):
        s = _FakeStrategy(config)
        prepared.append(s)
        return s

    _patch_strategy_factory(monkeypatch, factory)

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_beta.partial(cfg={}), on_failure="continue"),
        Peer(_gamma.partial(cfg={}), on_failure="continue"),
    )
    cluster = _RecordingCluster()
    result = _prepare_per_peer_packaging(cluster, spec)

    assert len(prepared) == 3
    assert result["_alpha"] is not result["_beta"]
    assert result["_beta"] is not result["_gamma"]


def test_inherit_peer_clones_leader_config(monkeypatch):
    from slurm import Peer

    prepared: List[_FakeStrategy] = []

    def factory(config):
        s = _FakeStrategy(config)
        prepared.append(s)
        return s

    _patch_strategy_factory(monkeypatch, factory)

    spec = _make_spec(
        Peer(_alpha.partial(cfg={}), leader=True),
        Peer(_inheritor.partial(cfg={}), on_failure="continue"),
    )
    cluster = _RecordingCluster()
    result = _prepare_per_peer_packaging(cluster, spec)

    # After inherit resolution, both peers share the leader's config —
    # and therefore the same (deduped) strategy instance.
    assert len(prepared) == 1
    assert result["_alpha"] is result["_inheritor"]


def test_inherit_falls_back_to_first_peer_when_no_leader(monkeypatch):
    from slurm import Peer

    prepared: List[_FakeStrategy] = []

    def factory(config):
        s = _FakeStrategy(config)
        prepared.append(s)
        return s

    _patch_strategy_factory(monkeypatch, factory)

    # No leader; first peer is _alpha (a concrete container) — _inheritor
    # second, so inheritance resolves against _alpha.
    spec = _make_spec(
        Peer(_alpha.partial(cfg={})),
        Peer(_inheritor.partial(cfg={}), on_failure="continue"),
    )
    cluster = _RecordingCluster()
    result = _prepare_per_peer_packaging(cluster, spec)

    assert len(prepared) == 1
    assert result["_alpha"] is result["_inheritor"]


# ---------------------------------------------------------------------------
# Validation — inherit-without-source
# ---------------------------------------------------------------------------


def test_inherit_as_first_peer_without_leader_is_rejected():
    from slurm import Peer

    with pytest.raises(TopologyError) as excinfo:
        _make_spec(
            Peer(_inheritor.partial(cfg={})),
            Peer(_alpha.partial(cfg={})),
        )
    msg = str(excinfo.value)
    assert "inherit" in msg
    assert "_inheritor" in msg


def test_inherit_as_first_peer_with_leader_elsewhere_is_valid():
    from slurm import Peer

    # The inheritor is first but a later peer is leader — source exists.
    spec = _make_spec(
        Peer(_inheritor.partial(cfg={})),
        Peer(_alpha.partial(cfg={}), leader=True),
    )
    assert any(p.resolved_name == "_inheritor" for p in spec.peers)
