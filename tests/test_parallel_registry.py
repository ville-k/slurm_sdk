"""Registry atomic-write + schema tests."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from slurm.parallel.registry import (
    PeerRegistryEntry,
    STATE_PENDING,
    STATE_READY,
    peers_from_registry,
    read_registry,
    update_peer_hostinfo,
    write_registry,
)


def test_peer_registry_entry_round_trip():
    entry = PeerRegistryEntry(
        name="learner",
        pool="gpu",
        replica_index=0,
        replica_count=1,
        hostname="gpu-01",
        hostnames=["gpu-01"],
        node_label="head",
        step_id="12345.0",
        ports={"rpc": 50051},
        metadata={"model_version": "r3"},
        state=STATE_READY,
        restart_count=0,
    )
    restored = PeerRegistryEntry.from_dict(entry.to_dict())
    assert restored == entry


def test_write_registry_atomic(tmp_path):
    path = tmp_path / "registry.json"
    registry = {
        "peers": {
            "learner": [
                PeerRegistryEntry(name="learner", pool="gpu").to_dict(),
            ]
        },
    }
    write_registry(path, registry)

    # File exists, no leftover tmp sibling.
    assert path.exists()
    assert not (path.parent / "registry.json.tmp").exists()

    # Round-trip shape is JSON-parseable and has the expected structure.
    restored = read_registry(path)
    assert "learner" in restored["peers"]
    peers = peers_from_registry(restored)
    assert peers["learner"][0].state == STATE_PENDING


def test_write_registry_creates_parent_dirs(tmp_path):
    nested = tmp_path / "deep" / "subdir"
    path = nested / "registry.json"
    write_registry(path, {"peers": {}})
    assert path.exists()


def test_read_registry_handles_empty_sections(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({}))
    registry = read_registry(path)
    assert peers_from_registry(registry) == {}


def test_concurrent_write_reader_never_sees_partial_file(tmp_path):
    """Stress-test the tmp+rename write path: concurrent reads must never
    observe a half-written JSON file."""

    path = tmp_path / "registry.json"
    registry = {"peers": {}}
    write_registry(path, registry)  # seed

    errors: list[Exception] = []
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            entry = PeerRegistryEntry(
                name=f"peer_{i}",
                pool="default",
                state=STATE_PENDING,
            ).to_dict()
            write_registry(path, {"peers": {"peer_x": [entry]}})
            i += 1

    def reader():
        while not stop.is_set():
            try:
                data = read_registry(path)
                # Every read must be valid JSON with the expected shape.
                assert "peers" in data
            except Exception as exc:  # pragma: no cover - only seen on race failure
                errors.append(exc)
                return

    w = threading.Thread(target=writer)
    r = threading.Thread(target=reader)
    w.start()
    r.start()
    time.sleep(0.3)
    stop.set()
    w.join(timeout=2.0)
    r.join(timeout=2.0)

    assert not errors, f"reader observed partial file(s): {errors!r}"


def test_update_peer_hostinfo_records_hostname_and_step(tmp_path):
    path = tmp_path / "registry.json"
    write_registry(
        path,
        {
            "peers": {
                "worker": [
                    PeerRegistryEntry(
                        name="worker",
                        pool="default",
                        hostnames=["node-a", "node-b"],
                    ).to_dict()
                ]
            },
        },
    )

    update_peer_hostinfo(path, "worker", 0, hostname="node-b", step_id="2")

    registry = read_registry(path)
    assert registry["peers"]["worker"][0]["hostname"] == "node-b"
    assert registry["peers"]["worker"][0]["step_id"] == "2"


def _unused_import_guard():
    # Silence lint on Path (imported for type hints in docstrings only).
    return Path("/")
