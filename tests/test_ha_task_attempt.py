import json
from pathlib import Path

from slurm.examples.high_availability_training.ha.task_attempt import (
    StopRequestedError,
    run_task_attempt,
)
from slurm.examples.high_availability_training.ha.task_result import StatusCode


def test_run_task_attempt_writes_commit_record(tmp_path: Path) -> None:
    output_dir = tmp_path / "attempt_001"

    result = run_task_attempt(
        output_dir=output_dir,
        input_payload={"hello": "world"},
        checkpoint_in=None,
        resiliency_config={"enabled": False, "implementation": "none"},
        run_once=lambda stop_requested: 123,
        is_retryable_in_job=lambda exc: False,
        user_error_types=(ValueError,),
        success_summary=lambda value: f"value={value}",
    )

    assert result["status_code"] == StatusCode.SUCCESS.value
    assert (output_dir / "input.json").exists()
    assert (output_dir / "checkpoint_in.json").exists()
    assert (output_dir / "result.json").exists()

    payload = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert payload["status_code"] == StatusCode.SUCCESS.value
    assert payload["status_summary"] == "value=123"


def test_run_task_attempt_classifies_user_errors(tmp_path: Path) -> None:
    output_dir = tmp_path / "attempt_001"

    def run_once(stop_requested) -> None:  # noqa: ARG001
        raise ValueError("bad config")

    result = run_task_attempt(
        output_dir=output_dir,
        input_payload={},
        checkpoint_in=None,
        resiliency_config={"enabled": False, "implementation": "none"},
        run_once=run_once,
        is_retryable_in_job=lambda exc: False,
        user_error_types=(ValueError,),
        user_error_summary="Non-retryable",
    )

    assert result["status_code"] == StatusCode.NONRETRYABLE_ERROR.value
    assert result["status_summary"] == "Non-retryable"

    payload = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
    assert payload["result_details"]["error_class"] == "user"
    assert payload["result_details"]["error_type"] == "ValueError"


def test_run_task_attempt_classifies_retryable_errors(tmp_path: Path) -> None:
    output_dir = tmp_path / "attempt_001"

    def run_once(stop_requested) -> None:  # noqa: ARG001
        raise RuntimeError("flaky")

    result = run_task_attempt(
        output_dir=output_dir,
        input_payload={},
        checkpoint_in=None,
        resiliency_config={"enabled": False, "implementation": "none"},
        run_once=run_once,
        is_retryable_in_job=lambda exc: False,
        user_error_types=(ValueError,),
        retryable_error_summary="Retryable",
    )

    assert result["status_code"] == StatusCode.RETRYABLE_ERROR.value
    assert result["status_summary"] == "Retryable"


def test_run_task_attempt_classifies_preemption(tmp_path: Path) -> None:
    output_dir = tmp_path / "attempt_001"

    def run_once(stop_requested) -> None:  # noqa: ARG001
        raise StopRequestedError("stop")

    result = run_task_attempt(
        output_dir=output_dir,
        input_payload={},
        checkpoint_in=None,
        resiliency_config={"enabled": False, "implementation": "none"},
        run_once=run_once,
        is_retryable_in_job=lambda exc: False,
        user_error_types=(ValueError,),
        preempted_summary="Preempted",
    )

    assert result["status_code"] == StatusCode.PREEMPTED.value
    assert result["status_summary"] == "Preempted"
