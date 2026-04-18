"""Tests for ``@task(ports={...})`` + ``ctx.my_ports`` / ``ctx.reserve_port``.

Exercises:

- Decorator-time validation of the ``ports=`` mapping.
- Propagation of ``ports`` through ``.with_options`` / ``.after`` /
  ``.with_dependencies``.
- Plan-level serialisation (``PlanPeer.ports`` round-trips through JSON).
- Runner-side resolution: ``"auto"`` entries get bound via ephemeral sockets
  and recorded into the peer's registry entry; fixed ints pass through.
- Peer-to-peer discovery: ``ctx.peers["<name>"].first.ports["<label>"]``
  reflects both startup resolution and ``reserve_port`` mid-function.
"""

from __future__ import annotations

import pytest

from slurm import task
from slurm.parallel.plan import Plan, PlanPeer
from slurm.parallel.registry import (
    STATE_PENDING,
    update_peer_ports,
    write_registry,
)
from slurm.runner.initialization import RunnerArgs, resolve_peer_ports
from slurm.runtime import JobContext


# ---------------------------------------------------------------------------
# Decorator-time validation
# ---------------------------------------------------------------------------


def test_ports_accepts_fixed_and_auto():
    @task(ports={"rpc": "auto", "metrics": 50051})
    def service(): ...

    assert service.ports == {"rpc": "auto", "metrics": 50051}


def test_ports_rejects_bool_value():
    with pytest.raises(ValueError, match="rpc"):

        @task(ports={"rpc": True})  # bool is an int — reject defensively
        def service(): ...


def test_ports_rejects_out_of_range_int():
    with pytest.raises(ValueError, match="out of range"):

        @task(ports={"rpc": 70000})
        def service(): ...


def test_ports_rejects_arbitrary_string():
    with pytest.raises(ValueError, match="valid port value"):

        @task(ports={"rpc": "random"})
        def service(): ...


def test_ports_rejects_non_dict():
    with pytest.raises(ValueError, match="must be a dict"):

        @task(ports=[("rpc", 5000)])  # type: ignore[arg-type]
        def service(): ...


def test_ports_default_is_empty_dict():
    @task()
    def plain(): ...

    assert plain.ports == {}


# ---------------------------------------------------------------------------
# .with_options / .after / .with_dependencies propagation
# ---------------------------------------------------------------------------


def test_ports_propagate_through_with_options():
    @task(ports={"rpc": "auto"})
    def service(): ...

    variant = service.with_options(partition="gpu")
    assert variant.ports == {"rpc": "auto"}
    # Independent dict — mutating one shouldn't affect the other.
    variant.ports["new"] = 1234
    assert "new" not in service.ports


# ---------------------------------------------------------------------------
# Plan serialisation round-trip
# ---------------------------------------------------------------------------


def test_plan_peer_ports_round_trip_through_json():
    peer = PlanPeer(
        name="srv",
        pool="default",
        leader=False,
        on_failure="kill",
        max_restarts=0,
        srun_command_line="srun echo",
        ports={"rpc": "auto", "metrics": 50051},
    )
    plan = Plan(
        peers=[peer],
        grace_period_seconds=10,
        pool_names=["default"],
    )
    round_trip = Plan.from_json(plan.to_json())
    assert round_trip.peers[0].ports == {"rpc": "auto", "metrics": 50051}


# ---------------------------------------------------------------------------
# Runner-side port resolution
# ---------------------------------------------------------------------------


def _make_runner_args(ports_spec: "dict | None") -> RunnerArgs:
    import base64 as _b64
    import json as _json

    encoded = None
    if ports_spec is not None:
        encoded = _b64.b64encode(_json.dumps(ports_spec).encode()).decode()
    return RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        peer_ports_b64=encoded,
    )


def test_resolve_peer_ports_none_when_missing():
    assert resolve_peer_ports(_make_runner_args(None)) is None


def test_resolve_peer_ports_fixed_int_passes_through():
    resolved = resolve_peer_ports(_make_runner_args({"metrics": 50051}))
    assert resolved == {"metrics": 50051}


def test_resolve_peer_ports_auto_yields_valid_ephemeral_port():
    resolved = resolve_peer_ports(_make_runner_args({"rpc": "auto"}))
    assert resolved is not None
    assert "rpc" in resolved
    port = resolved["rpc"]
    assert isinstance(port, int)
    # Ephemeral ports on Linux/macOS generally land above 10000 — just assert
    # it's a valid non-zero port.
    assert 1024 < port <= 65535


def test_resolve_peer_ports_mixed_spec():
    resolved = resolve_peer_ports(_make_runner_args({"rpc": "auto", "metrics": 50051}))
    assert resolved is not None
    assert resolved["metrics"] == 50051
    assert isinstance(resolved["rpc"], int)


# ---------------------------------------------------------------------------
# Registry update + peer discovery via ctx.peers
# ---------------------------------------------------------------------------


def _seed_two_peer_registry(path) -> None:
    def _entry(name):
        return {
            "name": name,
            "pool": "default",
            "replica_index": 0,
            "replica_count": 1,
            "hostname": f"{name}-host",
            "hostnames": [f"{name}-host"],
            "metadata": {},
            "ports": {},
            "state": STATE_PENDING,
            "restart_count": 0,
            "component_index": 0,
        }

    write_registry(
        path,
        {"peers": {"a": [_entry("a")], "b": [_entry("b")]}, "nodes": {}},
    )


def _ctx(name: str, registry_path) -> JobContext:
    return JobContext(
        job_id="1",
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


def test_peer_a_port_visible_to_peer_b_via_registry(tmp_path):
    registry_path = tmp_path / "registry.json"
    _seed_two_peer_registry(registry_path)

    update_peer_ports(registry_path, "a", 0, ports={"rpc": 54321})

    ctx_b = _ctx("b", registry_path)
    group_a = ctx_b.peers["a"]
    assert group_a.first.ports["rpc"] == 54321


def test_ctx_reserve_port_records_locally_and_in_registry(tmp_path):
    registry_path = tmp_path / "registry.json"
    _seed_two_peer_registry(registry_path)
    ctx_a = _ctx("a", registry_path)

    port = ctx_a.reserve_port("later")
    assert isinstance(port, int)
    assert ctx_a.my_ports["later"] == port

    # Other peer sees the same port number through the registry.
    ctx_b = _ctx("b", registry_path)
    assert ctx_b.peers["a"].first.ports["later"] == port


def test_ctx_my_ports_is_read_only_view(tmp_path):
    registry_path = tmp_path / "registry.json"
    _seed_two_peer_registry(registry_path)
    ctx = _ctx("a", registry_path)
    ctx.reserve_port("rpc")

    view = ctx.my_ports
    with pytest.raises(TypeError):
        view["new"] = 1234  # type: ignore[index]


def test_ctx_reserve_port_without_registry_path_still_caches_locally():
    """Outside a parallel allocation, ``reserve_port`` still populates my_ports."""
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
    port = ctx.reserve_port("rpc")
    assert ctx.my_ports["rpc"] == port
