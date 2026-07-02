"""Tests for :meth:`PeerGroup.wait_all` — blocking + timeout + keys filter.

``wait_all`` polls the registry on disk with exponential backoff. The
tests here use a background thread to satisfy the wait condition mid-
flight, confirming the poll loop actually observes the updated state.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from slurm.parallel.registry import (
    STATE_PENDING,
    announce_peer_metadata,
    load_peer_groups,
    update_peer_hostinfo,
    write_registry,
)


def _registry(tmp_path: Path, peer: str = "worker", replicas: int = 2) -> Path:
    path = tmp_path / "registry.json"
    entries = [
        {
            "name": peer,
            "pool": "default",
            "replica_index": i,
            "replica_count": replicas,
            "hostname": f"h-{i}",
            "hostnames": [f"h-{i}"],
            "metadata": {},
            "state": STATE_PENDING,
        }
        for i in range(replicas)
    ]
    write_registry(path, {"peers": {peer: entries}, "nodes": {}})
    return path


def test_wait_all_returns_immediately_when_already_ready(tmp_path):
    """Fast-path: every replica is already ready when wait_all is called."""
    path = _registry(tmp_path, replicas=2)
    # Mark all replicas ready before the wait.
    announce_peer_metadata(path, "worker", 0, fields={}, ready=True)
    announce_peer_metadata(path, "worker", 1, fields={}, ready=True)

    group = load_peer_groups(path)["worker"]
    start = time.monotonic()
    group.wait_all(timeout=1.0)
    elapsed = time.monotonic() - start
    assert elapsed < 0.2  # no real waiting happened


def test_wait_all_blocks_until_all_ready(tmp_path):
    path = _registry(tmp_path, replicas=3)
    group = load_peer_groups(path)["worker"]

    def announcer():
        # Stagger the ready signals so the poll loop definitely runs a few
        # iterations before exiting.
        for i in range(3):
            time.sleep(0.08)
            announce_peer_metadata(path, "worker", i, fields={}, ready=True)

    t = threading.Thread(target=announcer)
    t.start()
    group.wait_all(timeout=5.0)
    t.join(timeout=5.0)

    # All replicas must read as ready after wait_all returns.
    assert all(r.state == "ready" for r in group)


def test_wait_all_with_keys_waits_for_metadata(tmp_path):
    path = _registry(tmp_path, replicas=2)
    group = load_peer_groups(path)["worker"]

    def announcer():
        time.sleep(0.05)
        # Replica 0 announces the requested keys; replica 1 doesn't.
        announce_peer_metadata(
            path, "worker", 0, fields={"endpoint": "tcp://h-0:50051"}
        )
        time.sleep(0.05)
        announce_peer_metadata(
            path, "worker", 1, fields={"endpoint": "tcp://h-1:50051"}
        )

    t = threading.Thread(target=announcer)
    t.start()
    group.wait_all(keys=["endpoint"], timeout=5.0)
    t.join(timeout=5.0)

    assert all("endpoint" in r.metadata for r in group)


def test_wait_all_with_hostname_field_waits_for_runtime_publish(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(
        path,
        {
            "peers": {
                "worker": [
                    {
                        "name": "worker",
                        "pool": "default",
                        "replica_index": 0,
                        "replica_count": 2,
                        "hostname": "",
                        "hostnames": [],
                        "metadata": {},
                        "state": STATE_PENDING,
                        "step_id": None,
                    },
                    {
                        "name": "worker",
                        "pool": "default",
                        "replica_index": 1,
                        "replica_count": 2,
                        "hostname": "",
                        "hostnames": [],
                        "metadata": {},
                        "state": STATE_PENDING,
                        "step_id": None,
                    },
                ]
            },
            "nodes": {},
        },
    )
    group = load_peer_groups(path)["worker"]

    def publisher():
        time.sleep(0.05)
        update_peer_hostinfo(path, "worker", 0, hostname="host-0", step_id=None)
        time.sleep(0.05)
        update_peer_hostinfo(path, "worker", 1, hostname="host-1", step_id=None)

    t = threading.Thread(target=publisher)
    t.start()
    group.wait_all(keys=["hostname"], timeout=5.0)
    t.join(timeout=5.0)

    assert [replica.hostname for replica in group] == ["host-0", "host-1"]


def test_wait_all_with_step_id_field_waits_for_runtime_publish(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(
        path,
        {
            "peers": {
                "worker": [
                    {
                        "name": "worker",
                        "pool": "default",
                        "replica_index": 0,
                        "replica_count": 2,
                        "hostname": "host-0",
                        "hostnames": ["host-0"],
                        "metadata": {},
                        "state": STATE_PENDING,
                        "step_id": None,
                    },
                    {
                        "name": "worker",
                        "pool": "default",
                        "replica_index": 1,
                        "replica_count": 2,
                        "hostname": "host-1",
                        "hostnames": ["host-1"],
                        "metadata": {},
                        "state": STATE_PENDING,
                        "step_id": None,
                    },
                ]
            },
            "nodes": {},
        },
    )
    group = load_peer_groups(path)["worker"]

    def publisher():
        time.sleep(0.05)
        update_peer_hostinfo(path, "worker", 0, hostname="host-0", step_id="10")
        time.sleep(0.05)
        update_peer_hostinfo(path, "worker", 1, hostname="host-1", step_id="11")

    t = threading.Thread(target=publisher)
    t.start()
    group.wait_all(keys=["step_id"], timeout=5.0)
    t.join(timeout=5.0)

    assert [replica.step_id for replica in group] == ["10", "11"]


def test_wait_all_times_out_cleanly(tmp_path):
    path = _registry(tmp_path, replicas=2)
    # Mark only one replica ready; the other stays pending so wait_all
    # cannot complete.
    announce_peer_metadata(path, "worker", 0, fields={}, ready=True)

    group = load_peer_groups(path)["worker"]
    with pytest.raises(TimeoutError, match="timed out"):
        group.wait_all(timeout=0.3)


def test_wait_all_keys_timeout_lists_missing_replicas(tmp_path):
    path = _registry(tmp_path, replicas=2)
    # Replica 0 announces the key; replica 1 never does.
    announce_peer_metadata(path, "worker", 0, fields={"endpoint": "x"})
    group = load_peer_groups(path)["worker"]
    with pytest.raises(TimeoutError) as excinfo:
        group.wait_all(keys=["endpoint"], timeout=0.3)
    assert "worker" in str(excinfo.value)


def test_wait_all_without_registry_path_and_unmet_raises(tmp_path):
    """Synthetic group built without a path cannot make progress."""
    from slurm.parallel.peer_info import PeerGroup, PeerInfo

    group = PeerGroup(
        name="synth",
        _replicas=(
            PeerInfo.from_entry({"name": "synth", "pool": "p", "state": "pending"}),
        ),
    )
    with pytest.raises(RuntimeError, match="no registry path"):
        group.wait_all(timeout=0.1)


def test_wait_all_without_registry_path_but_already_satisfied(tmp_path):
    """Synthetic group that already satisfies the condition — no error."""
    from slurm.parallel.peer_info import PeerGroup, PeerInfo

    group = PeerGroup(
        name="synth",
        _replicas=(
            PeerInfo.from_entry({"name": "synth", "pool": "p", "state": "ready"}),
        ),
    )
    # Should not raise — already ready.
    group.wait_all(timeout=0.1)
