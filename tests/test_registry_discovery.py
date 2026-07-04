"""End-to-end peer discovery — announce from one context, observe from another.

Exercises the full Phase 7 runtime API: one peer's context ``announce()``s
metadata + ``ready=True``; another peer's context reads the same peer's
``PeerGroup`` and observes the announcement. No supervisor, no Slurm —
the in-memory contexts are wired directly at the ``JobContext`` layer so
the test focuses on the registry-driven discovery contract.
"""

from __future__ import annotations

from pathlib import Path

from slurm.parallel.registry import STATE_PENDING, write_registry
from slurm.runtime import JobContext, build_job_context


def _entries(name: str) -> list[dict]:
    return [
        {
            "name": name,
            "pool": "default",
            "replica_index": 0,
            "replica_count": 1,
            "hostname": f"{name}-host",
            "hostnames": [f"{name}-host"],
            "metadata": {},
            "state": STATE_PENDING,
        }
    ]


def _seed_registry(path: Path) -> None:
    """Two peers — ``a`` (single replica) and ``b`` (single replica)."""
    write_registry(
        path,
        {
            "peers": {"a": _entries("a"), "b": _entries("b")},
        },
    )


def _ctx_for(name: str, registry_path: Path) -> JobContext:
    return JobContext(
        job_id="42",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
        peer_name=name,
        peer_pool="default",
        replica_index=0,
        replica_count=None,
        _registry_path=registry_path,
    )


def test_peer_a_announces_peer_b_observes(tmp_path):
    """Classic service-discovery handshake — a serves, b discovers."""
    registry_path = tmp_path / "registry.json"
    _seed_registry(registry_path)

    ctx_a = _ctx_for("a", registry_path)
    ctx_b = _ctx_for("b", registry_path)

    # Before anyone announces, b sees a as pending with empty metadata.
    group_a_initial = ctx_b.peers["a"]
    assert group_a_initial.first.state == "pending"
    assert dict(group_a_initial.first.metadata) == {}

    # a announces readiness + a service endpoint.
    ctx_a.announce(ready=True, endpoint="tcp://a-host:50051", foo="bar")

    # b reads fresh — ctx.peers re-reads every access.
    group_a = ctx_b.peers["a"]
    assert group_a.first.state == "ready"
    assert group_a.first.metadata["endpoint"] == "tcp://a-host:50051"
    assert group_a.first.metadata["foo"] == "bar"


def test_ctx_peers_returns_empty_when_no_registry_path(tmp_path):
    """Non-parallel run — no registry path, ctx.peers is empty."""
    ctx = JobContext(
        job_id="1",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
    )
    assert len(ctx.peers) == 0
    assert list(ctx.peers) == []


def test_build_job_context_picks_up_registry_env(tmp_path):
    """The env var ``SLURM_SDK_REGISTRY_PATH`` threads through build_job_context."""
    registry_path = tmp_path / "registry.json"
    _seed_registry(registry_path)

    env = {
        "SLURM_JOB_ID": "7",
        "SLURM_SDK_PEER_NAME": "a",
        "SLURM_SDK_PEER_POOL": "default",
        "SLURM_SDK_REGISTRY_PATH": str(registry_path),
    }
    ctx = build_job_context(env=env)
    assert ctx.peer_name == "a"
    assert ctx._registry_path == registry_path
    assert "a" in ctx.peers
    assert "b" in ctx.peers


def test_ctx_peers_is_read_only_mapping(tmp_path):
    registry_path = tmp_path / "registry.json"
    _seed_registry(registry_path)
    ctx = _ctx_for("a", registry_path)
    peers = ctx.peers
    import pytest

    with pytest.raises(TypeError):
        peers["x"] = None  # type: ignore[index]


def test_ctx_announce_then_self_observe_via_peers(tmp_path):
    """A peer can also see its own announcements through ctx.peers[self]."""
    registry_path = tmp_path / "registry.json"
    _seed_registry(registry_path)
    ctx = _ctx_for("a", registry_path)

    ctx.announce(model_version="r9")
    assert ctx.peers["a"].first.metadata["model_version"] == "r9"
