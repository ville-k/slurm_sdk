"""Tests for generalized reference placeholder resolver registry."""

import json
import pickle
from pathlib import Path

from slurm.core import RefPlaceholder
from slurm.runner.placeholder import register_placeholder_resolver, resolve_placeholder


def _write_job_result(job_base_dir: Path, job_id: str, value: object) -> None:
    target_dir = job_base_dir / "task" / "run_001"
    target_dir.mkdir(parents=True, exist_ok=True)

    result_name = "slurm_job_001_result.pkl"
    result_path = target_dir / result_name
    with result_path.open("wb") as handle:
        pickle.dump(value, handle)

    metadata_path = target_dir / "metadata.json"
    metadata_path.write_text(
        json.dumps({job_id: {"result_file": result_name}}, indent=2),
        encoding="utf-8",
    )


def test_builtin_job_placeholder_resolution(tmp_path: Path):
    _write_job_result(tmp_path, "123", {"value": 9})

    placeholder = RefPlaceholder(ref_type="job", payload={"job_id": "123"})
    resolved = resolve_placeholder(placeholder, job_base_dir=str(tmp_path))

    assert resolved == {"value": 9}


def test_custom_placeholder_registration():
    register_placeholder_resolver(
        "upper",
        lambda payload, _job_base_dir: str(payload["value"]).upper(),
    )

    placeholder = RefPlaceholder(ref_type="upper", payload={"value": "abc"})
    assert resolve_placeholder(placeholder) == "ABC"
