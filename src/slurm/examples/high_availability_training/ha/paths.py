from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def epoch_dir(epoch: int) -> str:
    return f"epoch_{epoch:03d}"


def chunk_dir(chunk: int) -> str:
    return f"chunk_{chunk:03d}"


def attempt_dir(attempt: int) -> str:
    return f"attempt_{attempt:03d}"


def state_dir(run_dir: Path) -> Path:
    return run_dir / "state"


def state_path(run_dir: Path) -> Path:
    return state_dir(run_dir) / "state.json"


def events_path(run_dir: Path) -> Path:
    return state_dir(run_dir) / "events.jsonl"


def run_config_path(run_dir: Path) -> Path:
    return run_dir / "config" / "run_config.json"


def exports_dir(run_dir: Path) -> Path:
    return run_dir / "exports"


def latest_checkpoint_pointer_path(run_dir: Path) -> Path:
    return exports_dir(run_dir) / "latest_checkpoint.json"


def train_dir(run_dir: Path) -> Path:
    return run_dir / "train"


def eval_dir(run_dir: Path) -> Path:
    return run_dir / "eval"


@dataclass(frozen=True)
class AttemptPaths:
    """Standard per-attempt artifact paths (relative to an attempt output directory)."""

    output_dir: Path

    @property
    def input_path(self) -> Path:
        return self.output_dir / "input.json"

    @property
    def progress_path(self) -> Path:
        return self.output_dir / "progress.json"

    @property
    def checkpoint_in_path(self) -> Path:
        return self.output_dir / "checkpoint_in.json"

    @property
    def checkpoint_out_path(self) -> Path:
        return self.output_dir / "checkpoint_out.json"

    @property
    def metrics_json_path(self) -> Path:
        return self.output_dir / "metrics.json"

    @property
    def metrics_jsonl_path(self) -> Path:
        return self.output_dir / "metrics.jsonl"

    @property
    def result_path(self) -> Path:
        return self.output_dir / "result.json"

    @property
    def logs_dir(self) -> Path:
        return self.output_dir / "logs"

    @property
    def nvrx_dir(self) -> Path:
        return self.output_dir / "nvrx"

    @property
    def nvrx_events_path(self) -> Path:
        return self.nvrx_dir / "events.jsonl"

    @property
    def nvrx_summary_path(self) -> Path:
        return self.nvrx_dir / "summary.json"


def train_attempt_dir(run_dir: Path, *, epoch: int, chunk: int, attempt: int) -> Path:
    return (
        train_dir(run_dir) / epoch_dir(epoch) / chunk_dir(chunk) / attempt_dir(attempt)
    )


def eval_attempt_dir(run_dir: Path, *, epoch: int, attempt: int) -> Path:
    return eval_dir(run_dir) / epoch_dir(epoch) / attempt_dir(attempt)


__all__ = [
    "AttemptPaths",
    "attempt_dir",
    "chunk_dir",
    "epoch_dir",
    "eval_attempt_dir",
    "eval_dir",
    "events_path",
    "exports_dir",
    "latest_checkpoint_pointer_path",
    "run_config_path",
    "state_dir",
    "state_path",
    "train_attempt_dir",
    "train_dir",
]
