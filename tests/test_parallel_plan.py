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
                srun_command_line="srun --exact python -m slurm.runner --step peer:train",
            ),
            PlanPeer(
                name="metrics",
                pool="default",
                leader=False,
                on_failure="continue",
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
