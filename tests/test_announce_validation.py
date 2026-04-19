"""Tests for ``ctx.announce()`` — reserved-key rejection + atomic writes.

Phase 7's runtime announce path reuses the same reserved-key set as
Phase 1's ``Peer.announce={}`` static validator. These tests pin the
runtime behaviour: reserved names are rejected, legal fields merge into
metadata, and concurrent writers never produce torn files.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from slurm.parallel.registry import (
    _RESERVED_ANNOUNCE_KEYS,
    STATE_PENDING,
    STATE_READY,
    announce_peer_metadata,
    read_registry,
    write_registry,
)
from slurm.runtime import JobContext


def _registry(tmp_path: Path, peer: str = "worker", replicas: int = 1) -> Path:
    """Seed a minimal registry for announce tests."""
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
            "ports": {},
            "state": STATE_PENDING,
            "restart_count": 0,
            "component_index": 0,
        }
        for i in range(replicas)
    ]
    write_registry(path, {"peers": {peer: entries}, "nodes": {}})
    return path


def _ctx(
    registry_path: Path, peer: str = "worker", replica_index: int = 0
) -> JobContext:
    return JobContext(
        job_id="100",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
        peer_name=peer,
        peer_pool="default",
        replica_index=replica_index,
        replica_count=None,
        _registry_path=registry_path,
    )


# ---------------------------------------------------------------------------
# Reserved-key rejection — shared between static + runtime paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reserved_key", sorted(_RESERVED_ANNOUNCE_KEYS))
def test_announce_rejects_every_reserved_key(tmp_path, reserved_key):
    path = _registry(tmp_path)
    ctx = _ctx(path)
    with pytest.raises(ValueError, match="[Rr]eserved"):
        ctx.announce(**{reserved_key: "whatever"})


def test_announce_allows_regular_user_fields(tmp_path):
    path = _registry(tmp_path)
    ctx = _ctx(path)
    ctx.announce(model_version="r7", checkpoint_interval=500)
    meta = read_registry(path)["peers"]["worker"][0]["metadata"]
    assert meta == {"model_version": "r7", "checkpoint_interval": 500}


def test_announce_merges_fields_across_calls(tmp_path):
    path = _registry(tmp_path)
    ctx = _ctx(path)
    ctx.announce(a=1)
    ctx.announce(b=2)
    meta = read_registry(path)["peers"]["worker"][0]["metadata"]
    assert meta == {"a": 1, "b": 2}


def test_announce_ready_flag_flips_state(tmp_path):
    path = _registry(tmp_path)
    ctx = _ctx(path)
    assert read_registry(path)["peers"]["worker"][0]["state"] == STATE_PENDING
    ctx.announce(ready=True, model="v1")
    entry = read_registry(path)["peers"]["worker"][0]
    assert entry["state"] == STATE_READY
    assert entry["metadata"] == {"model": "v1"}


def test_announce_ready_alone_without_fields(tmp_path):
    path = _registry(tmp_path)
    ctx = _ctx(path)
    ctx.announce(ready=True)
    entry = read_registry(path)["peers"]["worker"][0]
    assert entry["state"] == STATE_READY
    assert entry["metadata"] == {}


def test_announce_without_peer_name_raises(tmp_path):
    """Announce is meaningful only inside a parallel allocation."""
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
        peer_name=None,  # non-parallel
        _registry_path=tmp_path / "registry.json",
    )
    with pytest.raises(RuntimeError, match="peer identity"):
        ctx.announce(ready=True)


def test_announce_without_registry_path_is_noop(tmp_path, caplog):
    """No registry path (local dev) — announce logs DEBUG and returns."""
    import logging

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
        peer_name="worker",
        replica_index=0,
        _registry_path=None,
    )
    with caplog.at_level(logging.DEBUG, logger="slurm.runtime"):
        ctx.announce(foo="bar")  # must not raise
    assert any("no registry path" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------
# Per-replica isolation — announcing from one replica doesn't smash siblings.
# ---------------------------------------------------------------------------


def test_announce_targets_own_replica_only(tmp_path):
    path = _registry(tmp_path, replicas=3)
    ctx0 = _ctx(path, replica_index=0)
    ctx2 = _ctx(path, replica_index=2)

    ctx0.announce(who="r0")
    ctx2.announce(who="r2")

    entries = read_registry(path)["peers"]["worker"]
    assert entries[0]["metadata"] == {"who": "r0"}
    assert entries[1]["metadata"] == {}
    assert entries[2]["metadata"] == {"who": "r2"}


# ---------------------------------------------------------------------------
# Concurrent writers — the tmp+rename atomic write must survive contention.
# ---------------------------------------------------------------------------


def test_announce_survives_concurrent_writers(tmp_path):
    """Many threads announcing simultaneously — zero lost updates.

    Each mutator acquires a file-level advisory lock around the read →
    mutate → write sequence so concurrent writers cannot overwrite each
    other's fields. Every announce lands in the final registry and the
    reader never sees a torn JSON.
    """
    path = _registry(tmp_path, replicas=4)
    writers = 4
    iterations = 15

    errors: list[Exception] = []
    done = threading.Event()

    def writer(idx: int):
        try:
            for i in range(iterations):
                announce_peer_metadata(
                    path,
                    "worker",
                    idx,
                    fields={f"key_{idx}_{i}": i},
                )
        except Exception as exc:  # pragma: no cover - race failure path
            errors.append(exc)

    def reader():
        while not done.is_set():
            data = read_registry(path)
            assert "peers" in data
            for entries in data["peers"].values():
                for e in entries:
                    assert isinstance(e.get("metadata"), dict)

    threads: list[threading.Thread] = [
        threading.Thread(target=writer, args=(i,)) for i in range(writers)
    ]
    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)
    done.set()
    reader_thread.join(timeout=5.0)

    assert not errors, f"writer errors: {errors!r}"

    # Every writer targeted its own replica index, so every (idx, i) pair
    # must have landed in that replica's metadata — no lost updates.
    entries = read_registry(path)["peers"]["worker"]
    assert len(entries) == writers
    for idx, entry in enumerate(entries):
        meta = entry["metadata"]
        for i in range(iterations):
            key = f"key_{idx}_{i}"
            assert key in meta, (
                f"writer {idx}'s update key={key!r} missing from replica "
                f"{idx}'s metadata (got keys={sorted(meta.keys())!r}) — "
                "file-lock regression"
            )
            assert meta[key] == i


def test_announce_concurrent_writers_same_replica_no_loss(tmp_path):
    """Multiple threads announcing into the *same* replica — every update lands.

    This is the stricter guarantee the file lock provides: even when
    writers target the same replica_index, no writer's fields get
    clobbered by a racing writer. Every (writer, iteration) key pair
    shows up in the final metadata.
    """
    path = _registry(tmp_path, replicas=1)
    writers = 4
    iterations = 20

    errors: list[Exception] = []

    def writer(idx: int):
        try:
            for i in range(iterations):
                announce_peer_metadata(
                    path,
                    "worker",
                    0,
                    fields={f"w{idx}_i{i}": i},
                )
        except Exception as exc:  # pragma: no cover - race failure path
            errors.append(exc)

    threads = [
        threading.Thread(target=writer, args=(i,)) for i in range(writers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)

    assert not errors, f"writer errors: {errors!r}"
    meta = read_registry(path)["peers"]["worker"][0]["metadata"]
    expected_keys = {f"w{idx}_i{i}" for idx in range(writers) for i in range(iterations)}
    missing = expected_keys - set(meta.keys())
    assert not missing, (
        f"{len(missing)} lost updates: sample={sorted(missing)[:5]!r}"
    )


def test_announce_serial_merge_semantics(tmp_path):
    """Serial announces to the same replica accumulate keys (no overwrites).

    This is the single-process, single-replica guarantee real peer code
    relies on — each peer runs in one process so its own announces never
    race with each other.
    """
    path = _registry(tmp_path, replicas=1)
    for i in range(20):
        announce_peer_metadata(path, "worker", 0, fields={f"k{i}": i})
    meta = read_registry(path)["peers"]["worker"][0]["metadata"]
    assert len(meta) == 20
    for i in range(20):
        assert meta[f"k{i}"] == i


def test_announce_reader_never_sees_partial_json(tmp_path):
    """Read-side stress: parsing JSON never fails during a storm of writes."""
    path = _registry(tmp_path, replicas=2)
    stop = threading.Event()
    parse_errors: list[Exception] = []

    def writer():
        i = 0
        while not stop.is_set():
            announce_peer_metadata(path, "worker", 0, fields={"i": i})
            i += 1

    def reader():
        while not stop.is_set():
            try:
                raw = path.read_text()
                json.loads(raw)
            except Exception as exc:
                parse_errors.append(exc)
                return

    w = threading.Thread(target=writer)
    r = threading.Thread(target=reader)
    w.start()
    r.start()
    import time as _time

    _time.sleep(0.3)
    stop.set()
    w.join(timeout=2.0)
    r.join(timeout=2.0)

    assert not parse_errors, f"reader saw partial JSON: {parse_errors!r}"
