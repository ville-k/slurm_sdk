"""Tests for ``SlurmTask.with_sidecars(...)`` sugar + ``BoundLeaderBundle``.

Exercises the desugaring contract:

- ``train.with_sidecars(metrics, tensorboard)(cfg=...)`` is equivalent to
  ``parallel(Peer(train.partial(cfg=...), leader=True),
  Peer(metrics, on_failure="continue"),
  Peer(tensorboard, on_failure="continue"))``.
- ``SlurmTask``, ``BoundTask``, and pre-built ``Peer`` sidecars are all
  accepted.
- A user-supplied ``Peer(..., on_failure="kill")`` retains its explicit
  policy (sugar only fills in defaults).
- ``.with_options(...)`` chains onto the leader before ``.with_sidecars``.
- ``topology=`` in the bundle call raises ``TypeError`` to steer users to
  the explicit ``parallel(...)`` form.

All tests intercept the final ``parallel(...)`` call by monkeypatching the
entry point, so no cluster / backend is required.
"""

from __future__ import annotations

import pytest

from slurm import task
from slurm.parallel.types import Peer
from slurm.task import BoundLeaderBundle, BoundTask, SlurmTask


@task(time="00:10:00", cpus_per_task=4)
def _train(cfg: dict) -> dict:
    return cfg


@task(time="00:05:00")
def _metrics():
    return "metrics"


@task(time="00:05:00")
def _tensorboard():
    return "tb"


def test_with_sidecars_returns_bound_leader_bundle():
    bundle = _train.with_sidecars(_metrics)
    assert isinstance(bundle, BoundLeaderBundle)
    assert bundle.leader is _train
    assert len(bundle.sidecars) == 1
    assert isinstance(bundle.sidecars[0], Peer)
    assert bundle.sidecars[0].on_failure == "continue"


def test_with_sidecars_requires_at_least_one_sidecar():
    with pytest.raises(TypeError, match="at least one sidecar"):
        _train.with_sidecars()


def test_with_sidecars_rejects_non_task_inputs():
    with pytest.raises(TypeError):
        _train.with_sidecars("not-a-task")  # type: ignore[arg-type]


def test_with_sidecars_accepts_slurm_task_bound_task_and_peer():
    bt = _metrics.partial()
    existing_peer = Peer(_tensorboard, on_failure="kill")

    bundle = _train.with_sidecars(_metrics, bt, existing_peer)
    assert len(bundle.sidecars) == 3
    # Bare SlurmTask + BoundTask → on_failure='continue'.
    assert bundle.sidecars[0].on_failure == "continue"
    assert bundle.sidecars[1].on_failure == "continue"
    # User-provided Peer keeps its explicit policy.
    assert bundle.sidecars[2].on_failure == "kill"


def test_bundle_call_desugars_to_parallel(monkeypatch):
    captured: dict = {}

    def fake_parallel(*peers, **kwargs):
        captured["peers"] = peers
        captured["kwargs"] = kwargs
        return "submitted"

    import importlib

    _parallel_mod = importlib.import_module("slurm.parallel")
    monkeypatch.setattr(_parallel_mod, "parallel", fake_parallel)

    bundle = _train.with_sidecars(_metrics, _tensorboard)
    result = bundle(cfg={"lr": 0.001})
    assert result == "submitted"

    peers = captured["peers"]
    assert len(peers) == 3
    # Leader peer is first, has leader=True, and wraps the bound leader task.
    assert peers[0].leader is True
    assert isinstance(peers[0].task, BoundTask)
    assert peers[0].task.kwargs == {"cfg": {"lr": 0.001}}
    # Sidecars retain the default on_failure='continue'.
    assert peers[1].on_failure == "continue"
    assert peers[2].on_failure == "continue"
    # grace_period_seconds is threaded through.
    assert captured["kwargs"]["grace_period_seconds"] == 10


def test_bundle_grace_period_is_threaded_through(monkeypatch):
    captured: dict = {}

    def fake_parallel(*peers, **kwargs):
        captured["kwargs"] = kwargs
        return None

    import importlib

    _parallel_mod = importlib.import_module("slurm.parallel")
    monkeypatch.setattr(_parallel_mod, "parallel", fake_parallel)

    bundle = _train.with_sidecars(_metrics, grace_period_seconds=30)
    bundle(cfg={})
    assert captured["kwargs"]["grace_period_seconds"] == 30


def test_with_options_chains_onto_leader(monkeypatch):
    """``.with_options(...).with_sidecars(...)`` puts options on the leader."""
    captured: dict = {}

    def fake_parallel(*peers, **kwargs):
        captured["peers"] = peers
        return None

    import importlib

    _parallel_mod = importlib.import_module("slurm.parallel")
    monkeypatch.setattr(_parallel_mod, "parallel", fake_parallel)

    leader_with_options = _train.with_options(partition="gpu", gpus=2)
    bundle = leader_with_options.with_sidecars(_metrics)
    bundle(cfg={})

    leader_peer = captured["peers"][0]
    # The leader's underlying SlurmTask carries the overrides.
    inner_task = leader_peer.task.task
    assert isinstance(inner_task, SlurmTask)
    assert inner_task.sbatch_options.get("partition") == "gpu"


def test_bundle_rejects_topology_keyword(monkeypatch):
    def fake_parallel(*peers, **kwargs):
        pytest.fail("parallel() must not be reached when topology= is rejected")

    import importlib

    _parallel_mod = importlib.import_module("slurm.parallel")
    monkeypatch.setattr(_parallel_mod, "parallel", fake_parallel)

    bundle = _train.with_sidecars(_metrics)
    with pytest.raises(TypeError, match="topology"):
        bundle(cfg={}, topology="something")


def test_user_peer_with_explicit_policy_is_respected(monkeypatch):
    captured: dict = {}

    def fake_parallel(*peers, **kwargs):
        captured["peers"] = peers
        return None

    import importlib

    _parallel_mod = importlib.import_module("slurm.parallel")
    monkeypatch.setattr(_parallel_mod, "parallel", fake_parallel)

    sidecar = Peer(_tensorboard, on_failure="kill")
    bundle = _train.with_sidecars(sidecar)
    bundle(cfg={})

    assert captured["peers"][1] is sidecar
    assert captured["peers"][1].on_failure == "kill"
