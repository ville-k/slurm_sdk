"""Tests for the :class:`NodeInfo` / :class:`NodeGroup` surface.

These tests exercise the read-only view API for nodes: label / ordinal
lookup, hostname lookup, per-pool iteration, :attr:`JobContext.nodes`, and
:attr:`JobContext.node`. They build registries directly so no supervisor
or Slurm allocation is needed.
"""

from __future__ import annotations

import pytest

from slurm.parallel.node_info import NodeGroup, NodeInfo
from slurm.parallel.registry import load_node_group, write_registry


def _registry_with_nodes() -> dict:
    """Build a registry with two pools: ``gpu`` (2 nodes) and ``cpu`` (3 nodes)."""
    nodes = {
        "gpu-01": {
            "hostname": "gpu-01",
            "pool": "gpu",
            "ordinal": 0,
            "label": "head",
            "peers": ["learner"],
            "component_index": 0,
        },
        "gpu-02": {
            "hostname": "gpu-02",
            "pool": "gpu",
            "ordinal": 1,
            "label": "ops",
            "peers": ["metrics"],
            "component_index": 0,
        },
        "cpu-01": {
            "hostname": "cpu-01",
            "pool": "cpu",
            "ordinal": 0,
            "label": None,
            "peers": ["sim"],
            "component_index": 1,
        },
        "cpu-02": {
            "hostname": "cpu-02",
            "pool": "cpu",
            "ordinal": 1,
            "label": None,
            "peers": ["sim"],
            "component_index": 1,
        },
        "cpu-03": {
            "hostname": "cpu-03",
            "pool": "cpu",
            "ordinal": 2,
            "label": None,
            "peers": ["sim"],
            "component_index": 1,
        },
    }
    return {"peers": {}, "nodes": nodes}


def test_node_info_from_entry_materializes_all_fields():
    entry = {
        "hostname": "gpu-01",
        "pool": "gpu",
        "ordinal": 0,
        "label": "head",
        "peers": ["learner", "sidecar"],
        "component_index": 1,
    }
    info = NodeInfo.from_entry(entry)
    assert info.hostname == "gpu-01"
    assert info.pool == "gpu"
    assert info.ordinal == 0
    assert info.label == "head"
    assert info.peers == ("learner", "sidecar")
    assert info.component_index == 1


def test_node_group_lookup_by_label(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)
    head = group["head"]
    assert head.hostname == "gpu-01"
    assert head.pool == "gpu"


def test_node_group_ordinal_requires_current_pool(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)  # current_pool not set
    with pytest.raises(IndexError, match="current peer pool"):
        _ = group[0]


def test_node_group_ordinal_within_current_pool(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path, current_pool="cpu")
    # Ordinal 0 in ``cpu`` pool → cpu-01.
    assert group[0].hostname == "cpu-01"
    assert group[2].hostname == "cpu-03"


def test_node_group_ordinal_out_of_range(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path, current_pool="gpu")
    with pytest.raises(IndexError, match="out of range"):
        _ = group[5]


def test_node_group_by_hostname(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)
    assert group.by_hostname("cpu-02").hostname == "cpu-02"
    assert group.by_hostname("missing-host") is None


def test_node_group_derives_peers_from_live_peer_entries(tmp_path):
    path = tmp_path / "registry.json"
    registry = _registry_with_nodes()
    registry["nodes"]["gpu-01"]["peers"] = ["stale-peer"]
    registry["nodes"]["gpu-02"]["peers"] = ["stale-peer"]
    registry["nodes"]["cpu-01"]["peers"] = ["stale-peer"]
    registry["peers"] = {
        "learner": [
            {
                "name": "learner",
                "pool": "gpu",
                "replica_index": 0,
                "replica_count": 1,
                "hostname": "gpu-01",
                "hostnames": ["gpu-01"],
            }
        ],
        "metrics": [
            {
                "name": "metrics",
                "pool": "gpu",
                "replica_index": 0,
                "replica_count": 2,
                "hostname": "gpu-01",
                "hostnames": ["gpu-01"],
            },
            {
                "name": "metrics",
                "pool": "gpu",
                "replica_index": 1,
                "replica_count": 2,
                "hostname": "gpu-01",
                "hostnames": ["gpu-01"],
            },
        ],
        "sim": [
            {
                "name": "sim",
                "pool": "cpu",
                "replica_index": 0,
                "replica_count": 1,
                "hostname": "cpu-03",
                "hostnames": ["cpu-03"],
            }
        ],
    }
    write_registry(path, registry)

    group = load_node_group(path)
    assert group.by_hostname("gpu-01").peers == ("learner", "metrics")
    assert group.by_hostname("gpu-02").peers == ()
    assert group.by_hostname("cpu-01").peers == ()
    assert group.by_hostname("cpu-03").peers == ("sim",)


def test_node_group_in_pool_iterator(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)
    cpu = list(group.in_pool("cpu"))
    assert [n.hostname for n in cpu] == ["cpu-01", "cpu-02", "cpu-03"]


def test_node_group_len_and_iter(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)
    assert len(group) == 5
    hosts = sorted(n.hostname for n in group)
    assert hosts == ["cpu-01", "cpu-02", "cpu-03", "gpu-01", "gpu-02"]


def test_node_group_missing_label_raises_keyerror(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    group = load_node_group(path)
    with pytest.raises(KeyError, match="no-such-label"):
        _ = group["no-such-label"]


def test_node_group_missing_registry_returns_empty(tmp_path):
    group = load_node_group(tmp_path / "does-not-exist.json")
    assert len(group) == 0
    assert list(group) == []


def test_node_group_refresh_reloads_from_disk(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, {"peers": {}, "nodes": {}})
    group = load_node_group(path)
    assert len(group) == 0
    write_registry(path, _registry_with_nodes())
    group.refresh()
    assert len(group) == 5


def test_ctx_nodes_empty_without_registry():
    from slurm.runtime import build_job_context

    # No SLURM_SDK_REGISTRY_PATH in the env → ctx.nodes is a read-only
    # empty NodeGroup instead of crashing.
    ctx = build_job_context(env={})
    nodes = ctx.nodes
    assert isinstance(nodes, NodeGroup)
    assert len(nodes) == 0


def test_ctx_nodes_loads_registry(tmp_path, monkeypatch):
    from slurm.runtime import build_job_context

    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    env = {
        "SLURM_SDK_REGISTRY_PATH": str(path),
        "SLURM_SDK_PEER_POOL": "cpu",
    }
    ctx = build_job_context(env=env)
    nodes = ctx.nodes
    assert len(nodes) == 5
    assert nodes["head"].hostname == "gpu-01"
    # Ordinal lookup resolves within the current peer's pool.
    assert nodes[0].hostname == "cpu-01"


def test_ctx_node_returns_info_for_current_host(tmp_path, monkeypatch):
    from slurm.runtime import build_job_context

    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    monkeypatch.setenv("HOSTNAME", "cpu-02")
    env = {
        "SLURM_SDK_REGISTRY_PATH": str(path),
        "SLURM_SDK_PEER_POOL": "cpu",
        "HOSTNAME": "cpu-02",
    }
    ctx = build_job_context(env=env)
    node = ctx.node
    assert node is not None
    assert node.hostname == "cpu-02"
    assert node.pool == "cpu"


def test_ctx_node_prefers_step_nodelist_over_hostname_env(tmp_path):
    from slurm.runtime import build_job_context

    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    env = {
        "SLURM_SDK_REGISTRY_PATH": str(path),
        "SLURM_SDK_PEER_POOL": "cpu",
        "SLURM_STEP_NODELIST": "cpu-[01-03]",
        "SLURM_NODEID": "1",
        "HOSTNAME": "gpu-01",
    }
    ctx = build_job_context(env=env)
    node = ctx.node
    assert node is not None
    assert node.hostname == "cpu-02"


def test_ctx_node_falls_back_to_socket_hostname(tmp_path, monkeypatch):
    from slurm.runtime import build_job_context

    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    monkeypatch.delenv("HOSTNAME", raising=False)
    import socket

    monkeypatch.setattr(socket, "gethostname", lambda: "cpu-03")
    env = {
        "SLURM_SDK_REGISTRY_PATH": str(path),
        "SLURM_SDK_PEER_POOL": "cpu",
    }
    ctx = build_job_context(env=env)
    node = ctx.node
    assert node is not None
    assert node.hostname == "cpu-03"


def test_ctx_node_none_when_host_not_in_registry(tmp_path, monkeypatch):
    from slurm.runtime import build_job_context

    path = tmp_path / "registry.json"
    write_registry(path, _registry_with_nodes())
    env = {
        "SLURM_SDK_REGISTRY_PATH": str(path),
        "SLURM_SDK_PEER_POOL": "cpu",
        "HOSTNAME": "some-other-host",
    }
    ctx = build_job_context(env=env)
    assert ctx.node is None


def test_ctx_node_none_when_registry_absent():
    from slurm.runtime import build_job_context

    ctx = build_job_context(env={})
    # No registry at all → no NodeInfo either.
    assert ctx.node is None
