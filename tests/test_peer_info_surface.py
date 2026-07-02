"""Tests for the ``PeerInfo`` / ``PeerGroup`` surface.

These tests exercise the read-only view API: indexing, iteration,
``hostnames``, ``ready_only``, ``first``, ``refresh``. They use the
in-process registry helpers directly — no supervisor or Slurm needed.
"""

from __future__ import annotations

import pytest

from slurm.parallel.peer_info import PeerGroup, PeerInfo
from slurm.parallel.registry import (
    STATE_PENDING,
    STATE_READY,
    announce_peer_metadata,
    load_peer_groups,
    update_peer_entry,
    write_registry,
)


def _skeleton_registry(peer_name: str = "worker", replica_count: int = 3) -> dict:
    """Build a minimal registry with one peer and N replicas."""
    entries = []
    for i in range(replica_count):
        entries.append(
            {
                "name": peer_name,
                "pool": "default",
                "replica_index": i,
                "replica_count": replica_count,
                "hostname": f"node-{i}",
                "hostnames": [f"node-{i}"],
                "step_id": None,
                "metadata": {},
                "state": STATE_PENDING,
            }
        )
    return {"peers": {peer_name: entries}, "nodes": {}}


def test_peer_info_from_entry_materializes_all_fields():
    entry = {
        "name": "learner",
        "pool": "gpu",
        "replica_index": 0,
        "replica_count": 1,
        "hostname": "gpu-01",
        "hostnames": ["gpu-01", "gpu-02"],
        "step_id": "12345.0",
        "metadata": {"model_version": "r3"},
        "state": "ready",
    }
    info = PeerInfo.from_entry(entry)
    assert info.name == "learner"
    assert info.pool == "gpu"
    assert info.hostname == "gpu-01"
    assert info.hostnames == ("gpu-01", "gpu-02")
    assert info.step_id == "12345.0"
    assert dict(info.metadata) == {"model_version": "r3"}
    assert info.state == "ready"


def test_peer_info_state_collapses_internal_states_to_failed():
    for raw in ("failed", "shutdown_by_leader", "shutting_down"):
        info = PeerInfo.from_entry({"name": "x", "pool": "p", "state": raw})
        assert info.state == "failed", f"state={raw!r} should map to failed"


def test_peer_info_state_pending_covers_pending_and_running():
    for raw in ("pending", "running"):
        info = PeerInfo.from_entry({"name": "x", "pool": "p", "state": raw})
        assert info.state == "pending"


def test_peer_info_mappings_are_read_only():
    info = PeerInfo.from_entry({"name": "x", "pool": "p", "metadata": {"k": "v"}})
    with pytest.raises(TypeError):
        info.metadata["k2"] = "v2"  # type: ignore[index]


def test_peer_group_getitem_and_len(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=3))
    groups = load_peer_groups(path)
    group = groups["worker"]
    assert len(group) == 3
    assert group[0].replica_index == 0
    assert group[2].replica_index == 2
    assert [info.hostname for info in group] == ["node-0", "node-1", "node-2"]


def test_peer_group_first_property(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=2))
    group = load_peer_groups(path)["worker"]
    assert group.first.replica_index == 0


def test_peer_group_first_raises_on_empty():
    # Synthetic empty group — service discovery never encounters this in
    # practice but the property should still fail loudly.
    group = PeerGroup(name="empty", _replicas=())
    with pytest.raises(IndexError, match="has no replicas"):
        _ = group.first


def test_peer_group_hostnames_is_ordered_tuple(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=3))
    group = load_peer_groups(path)["worker"]
    assert group.hostnames == ("node-0", "node-1", "node-2")


def test_peer_group_ready_only_filters_state(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=3))
    # Mark replica 1 ready.
    update_peer_entry(path, "worker", replica_index=1, state=STATE_READY)
    groups = load_peer_groups(path)
    ready = list(groups["worker"].ready_only())
    assert len(ready) == 1
    assert ready[0].replica_index == 1


def test_peer_group_refresh_picks_up_new_metadata(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=2))
    group = load_peer_groups(path)["worker"]
    assert group[0].metadata == {}

    announce_peer_metadata(path, "worker", 0, fields={"foo": "bar"}, ready=True)
    # Stale snapshot doesn't see the announce until refresh.
    assert group[0].metadata == {}

    group.refresh()
    assert group[0].metadata == {"foo": "bar"}
    assert group[0].state == "ready"


def test_load_peer_groups_missing_registry_returns_empty(tmp_path):
    groups = load_peer_groups(tmp_path / "does_not_exist.json")
    assert len(groups) == 0
    assert list(groups) == []


def test_load_peer_groups_is_read_only_mapping(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=1))
    groups = load_peer_groups(path)
    with pytest.raises(TypeError):
        groups["x"] = None  # type: ignore[index]


def test_peer_group_refresh_stable_when_peer_disappears(tmp_path):
    """If the peer vanishes from the registry, keep the cached replicas.

    In practice this doesn't happen (the bootstrap-seeded entry stays put)
    but the safety path should log and keep going rather than blow up.
    """
    path = tmp_path / "registry.json"
    write_registry(path, _skeleton_registry(replica_count=1))
    group = load_peer_groups(path)["worker"]

    # Rewrite registry with the peer missing.
    write_registry(path, {"peers": {}, "nodes": {}})
    group.refresh()
    assert len(group) == 1  # stale replicas preserved


def test_peer_group_iteration_is_replica_index_sorted(tmp_path):
    """Replicas are returned in ``replica_index`` order regardless of JSON order."""
    entries = [
        {
            "name": "worker",
            "pool": "p",
            "replica_index": 2,
            "replica_count": 3,
            "hostname": "node-2",
            "hostnames": ["node-2"],
        },
        {
            "name": "worker",
            "pool": "p",
            "replica_index": 0,
            "replica_count": 3,
            "hostname": "node-0",
            "hostnames": ["node-0"],
        },
        {
            "name": "worker",
            "pool": "p",
            "replica_index": 1,
            "replica_count": 3,
            "hostname": "node-1",
            "hostnames": ["node-1"],
        },
    ]
    path = tmp_path / "registry.json"
    write_registry(path, {"peers": {"worker": entries}, "nodes": {}})
    group = load_peer_groups(path)["worker"]
    assert [r.replica_index for r in group] == [0, 1, 2]
