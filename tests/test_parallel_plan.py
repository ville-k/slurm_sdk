"""JSON round-trip tests for :class:`slurm.parallel.plan.Plan`."""

from __future__ import annotations

from slurm.parallel.plan import Plan, PlanPeer, read_plan, write_plan


def _sample_plan() -> Plan:
    return Plan(
        peers=[
            PlanPeer(
                name="train",
                pool="default",
                leader=True,
                on_failure="kill",
                max_restarts=0,
                srun_command_line="srun --exact python -m slurm.runner --step peer:train",
            ),
            PlanPeer(
                name="metrics",
                pool="default",
                leader=False,
                on_failure="continue",
                max_restarts=0,
                srun_command_line="srun --exact python -m slurm.runner --step peer:metrics",
            ),
        ],
        grace_period_seconds=12,
        pool_names=["default"],
        pre_submission_id="abc123",
    )


def test_plan_json_round_trip():
    plan = _sample_plan()
    restored = Plan.from_json(plan.to_json())
    assert restored.grace_period_seconds == 12
    assert restored.pool_names == ["default"]
    assert restored.pre_submission_id == "abc123"
    assert restored.schema_version == 1
    assert [p.name for p in restored.peers] == ["train", "metrics"]
    assert restored.peer_by_name("train").leader is True
    assert restored.peer_by_name("metrics").on_failure == "continue"


def test_plan_peer_by_name_raises_keyerror():
    plan = _sample_plan()
    try:
        plan.peer_by_name("missing")
    except KeyError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("expected KeyError")


def test_write_read_plan_file(tmp_path):
    path = tmp_path / "plan.json"
    plan = _sample_plan()
    write_plan(path, plan)
    restored = read_plan(path)
    assert restored.pre_submission_id == plan.pre_submission_id
    assert [p.name for p in restored.peers] == ["train", "metrics"]


def test_plan_defaults_max_restarts_when_missing():
    raw = """\
{
  "schema_version": 1,
  "peers": [
    {"name": "x", "pool": "default", "leader": false, "on_failure": "kill",
     "srun_command_line": "echo"}
  ],
  "grace_period_seconds": 10,
  "pool_names": ["default"]
}"""
    plan = Plan.from_json(raw)
    assert plan.peers[0].max_restarts == 0
    # Missing callback field defaults to None — Phase 4 adds the field but
    # older plans written by the Phase 3 pipeline must still round-trip.
    assert plan.peers[0].callback is None


def test_plan_peer_preserves_callback_fqname():
    peer = PlanPeer(
        name="worker",
        pool="default",
        leader=False,
        on_failure="callback",
        max_restarts=0,
        srun_command_line="echo",
        callback="my.module:my_callback",
    )
    plan = Plan(
        peers=[peer],
        grace_period_seconds=5,
        pool_names=["default"],
        pre_submission_id="cb",
    )
    restored = Plan.from_json(plan.to_json())
    assert restored.peers[0].callback == "my.module:my_callback"
