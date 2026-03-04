from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

from slurm.examples.high_availability_training.ha.io import (
    append_jsonl,
    now_iso,
    read_json,
    write_json_atomic,
)
from slurm.examples.high_availability_training.ha.lock import (
    LockHeldError,
    SupervisorLeaseLock,
)
from slurm.examples.high_availability_training.ha.paths import (
    AttemptPaths,
    eval_attempt_dir,
    events_path,
    latest_checkpoint_pointer_path,
    run_config_path,
    state_dir,
    state_path,
    train_attempt_dir,
)
from slurm.examples.high_availability_training.ha.task_result import StatusCode
from slurm.workflow import WorkflowContext


SCHEMA_VERSION = 1


def init_state(
    *, run_id: str, run_dir: Path, config: Mapping[str, Any]
) -> Dict[str, Any]:
    """Initialize a new run state structure."""
    created_at = now_iso()
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "created_at": created_at,
        "updated_at": created_at,
        "state_revision": 0,
        "stop_requested": False,
        "completed": False,
        "failed": False,
        "failure": None,
        "config": dict(config),
        "epochs": {},
    }


def load_state(path: Path) -> MutableMapping[str, Any]:
    return read_json(path)


def save_state(path: Path, state: MutableMapping[str, Any]) -> None:
    state["state_revision"] = int(state.get("state_revision", 0)) + 1
    state["updated_at"] = now_iso()
    write_json_atomic(path, state)


def ensure_epoch_state(
    state: MutableMapping[str, Any], epoch: int
) -> MutableMapping[str, Any]:
    epochs = state.setdefault("epochs", {})
    key = str(epoch)
    if key not in epochs:
        epochs[key] = {
            "epoch": epoch,
            "train": {
                "steps_completed": 0,
                "latest_checkpoint_path": None,
                "active": None,
                "chunks": {},
            },
            "eval": {
                "active": None,
                "attempts": {},
                "completed": False,
                "metrics_path": None,
            },
        }
    return epochs[key]


def latest_attempt_number(attempts: Mapping[str, Any]) -> int:
    """Return max attempt number in a mapping keyed by attempt strings."""
    max_attempt = 0
    for key in attempts.keys():
        try:
            max_attempt = max(max_attempt, int(key))
        except Exception:
            continue
    return max_attempt


def _parse_dir_index(name: str, prefix: str) -> Optional[int]:
    if not name.startswith(prefix):
        return None
    suffix = name[len(prefix) :]
    try:
        return int(suffix)
    except Exception:
        return None


def _iter_result_files(run_dir: Path) -> Tuple[Path, ...]:
    train_results = tuple(run_dir.glob("train/epoch_*/chunk_*/attempt_*/result.json"))
    eval_results = tuple(run_dir.glob("eval/epoch_*/attempt_*/result.json"))
    return train_results + eval_results


def _reconcile_from_disk(state: Dict[str, Any], run_dir: Path) -> None:
    """Best-effort reconciliation: update state from on-disk result records."""
    for result_path in _iter_result_files(run_dir):
        parts = result_path.parts
        if "train" in parts:
            try:
                epoch_name = result_path.parents[2].name  # epoch_XXX
                chunk_name = result_path.parents[1].name  # chunk_XXX
                attempt_name = result_path.parents[0].name  # attempt_XXX
            except Exception:
                continue

            epoch = _parse_dir_index(epoch_name, "epoch_")
            chunk = _parse_dir_index(chunk_name, "chunk_")
            attempt = _parse_dir_index(attempt_name, "attempt_")
            if epoch is None or chunk is None or attempt is None:
                continue

            epoch_state = ensure_epoch_state(state, epoch)
            chunk_state = epoch_state["train"]["chunks"].setdefault(
                str(chunk), {"chunk": chunk, "attempts": {}}
            )
            existing = chunk_state["attempts"].get(str(attempt))

            try:
                task_result = read_json(result_path)
            except Exception:
                continue
            if isinstance(existing, dict) and existing.get("status_code"):
                continue

            record = {
                "attempt": attempt,
                "output_dir": str(result_path.parent),
                "status_code": task_result.get("status_code"),
                "completed_at": task_result.get("created_at"),
                "checkpoint_out": task_result.get("result_details", {}).get(
                    "checkpoint_out"
                ),
            }
            if isinstance(existing, dict):
                existing.update(record)
            else:
                chunk_state["attempts"][str(attempt)] = record

            checkpoint_out = record.get("checkpoint_out")
            if isinstance(checkpoint_out, str) and checkpoint_out:
                try:
                    payload = read_json(Path(checkpoint_out))
                    steps = int(payload.get("steps_completed", 0))
                    if steps > int(epoch_state["train"]["steps_completed"]):
                        epoch_state["train"]["steps_completed"] = steps
                        epoch_state["train"]["latest_checkpoint_path"] = checkpoint_out
                except Exception:
                    pass

        if "eval" in parts:
            try:
                epoch_name = result_path.parents[1].name  # epoch_XXX
                attempt_name = result_path.parents[0].name  # attempt_XXX
            except Exception:
                continue

            epoch = _parse_dir_index(epoch_name, "epoch_")
            attempt = _parse_dir_index(attempt_name, "attempt_")
            if epoch is None or attempt is None:
                continue

            epoch_state = ensure_epoch_state(state, epoch)
            attempts = epoch_state["eval"]["attempts"]
            existing = attempts.get(str(attempt))

            try:
                task_result = read_json(result_path)
            except Exception:
                continue
            if isinstance(existing, dict) and existing.get("status_code"):
                continue

            record = {
                "attempt": attempt,
                "output_dir": str(result_path.parent),
                "status_code": task_result.get("status_code"),
                "completed_at": task_result.get("created_at"),
                "metrics_path": task_result.get("result_details", {}).get(
                    "metrics_path"
                ),
            }
            if isinstance(existing, dict):
                existing.update(record)
            else:
                attempts[str(attempt)] = record
            if task_result.get("status_code") == StatusCode.SUCCESS.value:
                epoch_state["eval"]["completed"] = True
                epoch_state["eval"]["metrics_path"] = task_result.get(
                    "result_details", {}
                ).get("metrics_path")


def _owner_active_checker(ctx: WorkflowContext) -> Any:
    def is_owner_active(owner: Mapping[str, Any]) -> bool:
        job_id = owner.get("workflow_job_id")
        if not isinstance(job_id, str) or not job_id:
            return False
        try:
            status = ctx.cluster.get_job(job_id).get_status()
        except Exception:
            return False
        return status.get("JobState") in {"PENDING", "RUNNING"}

    return is_owner_active


@dataclass(frozen=True)
class TrainEvalSupervisorConfig:
    run_id: str
    run_dir: Path
    epochs: int
    epoch_steps: int
    max_steps_per_chunk: int
    max_train_attempts: int
    max_eval_attempts: int
    poll_interval_s: float = 0.2
    partition_train: Optional[str] = None
    partition_eval: Optional[str] = None
    resiliency_config: Optional[Mapping[str, Any]] = None


class TrainEvalSupervisor:
    """Scheduler-level supervisor for a high-availability train/eval run."""

    def __init__(
        self,
        *,
        config: TrainEvalSupervisorConfig,
        train_task: Any,
        eval_task: Any,
        ctx: WorkflowContext,
    ) -> None:
        self._cfg = config
        self._ctx = ctx
        self._train_task = train_task
        self._eval_task = eval_task

    def run(self) -> Path:
        cfg = self._cfg

        if cfg.epochs < 1:
            raise ValueError("epochs must be >= 1")
        if cfg.epoch_steps < 1:
            raise ValueError("epoch_steps must be >= 1")
        if cfg.max_steps_per_chunk < 1:
            raise ValueError("max_steps_per_chunk must be >= 1")
        if cfg.max_train_attempts < 1:
            raise ValueError("max_train_attempts must be >= 1")
        if cfg.max_eval_attempts < 1:
            raise ValueError("max_eval_attempts must be >= 1")

        run_dir_path = cfg.run_dir.expanduser()
        state_dir(run_dir_path).mkdir(parents=True, exist_ok=True)

        run_cfg_path = run_config_path(run_dir_path)
        run_cfg = {
            "run_id": cfg.run_id,
            "run_dir": str(run_dir_path),
            "epochs": cfg.epochs,
            "epoch_steps": cfg.epoch_steps,
            "max_steps_per_chunk": cfg.max_steps_per_chunk,
            "max_train_attempts": cfg.max_train_attempts,
            "max_eval_attempts": cfg.max_eval_attempts,
            "poll_interval_s": cfg.poll_interval_s,
            "resiliency_config": dict(cfg.resiliency_config or {}),
            "created_at": now_iso(),
        }
        if not run_cfg_path.exists():
            write_json_atomic(run_cfg_path, run_cfg)

        s_path = state_path(run_dir_path)
        if s_path.exists():
            state = dict(load_state(s_path))
        else:
            state = init_state(run_id=cfg.run_id, run_dir=run_dir_path, config=run_cfg)
            save_state(s_path, state)

        if state.get("completed") is True:
            return s_path

        lock = SupervisorLeaseLock(
            state_dir(run_dir_path) / "supervisor.lock",
            ttl_s=120.0,
            heartbeat_interval_s=5.0,
            is_owner_active=_owner_active_checker(self._ctx),
        )

        owner = {
            "run_id": cfg.run_id,
            "workflow_job_id": self._ctx.workflow_job_id,
            "pid": os.getpid(),
            "acquired_at": now_iso(),
        }

        try:
            lock.acquire(owner)
        except LockHeldError:
            return s_path

        events_log = events_path(run_dir_path)

        try:
            while True:
                lock.heartbeat()

                state = dict(load_state(s_path)) if s_path.exists() else state
                _reconcile_from_disk(state, run_dir_path)

                if state.get("stop_requested") is True:
                    append_jsonl(
                        events_log, {"time": now_iso(), "event": "stop_requested"}
                    )
                    state["completed"] = True
                    save_state(s_path, state)
                    return s_path

                self._reconcile_and_submit_train(state, run_dir_path, events_log)
                self._reconcile_and_submit_eval(state, run_dir_path, events_log)

                if state.get("completed") is True:
                    save_state(s_path, state)
                    return s_path

                if self._all_done(state):
                    state["completed"] = True
                    append_jsonl(
                        events_log, {"time": now_iso(), "event": "run_completed"}
                    )
                    save_state(s_path, state)
                    return s_path

                save_state(s_path, state)
                if cfg.poll_interval_s > 0:
                    time.sleep(float(cfg.poll_interval_s))
        finally:
            lock.release()

    def _reconcile_and_submit_train(
        self, state: Dict[str, Any], run_dir: Path, events_log: Path
    ) -> None:
        cfg = self._cfg

        next_train_epoch: Optional[int] = None
        for epoch in range(cfg.epochs):
            e_state = ensure_epoch_state(state, epoch)
            if int(e_state["train"]["steps_completed"]) < cfg.epoch_steps:
                next_train_epoch = epoch
                break

        if next_train_epoch is None:
            return

        e_state = ensure_epoch_state(state, next_train_epoch)
        train_section = e_state["train"]
        active = train_section.get("active")
        if isinstance(active, dict):
            self._reconcile_active_train_attempt(
                state, train_section, active, next_train_epoch, run_dir, events_log
            )

        if train_section.get("active") is None:
            steps_completed = int(train_section["steps_completed"])
            if steps_completed < cfg.epoch_steps:
                chunk = steps_completed // cfg.max_steps_per_chunk
                remaining = cfg.epoch_steps - steps_completed
                steps_this_chunk = min(cfg.max_steps_per_chunk, remaining)

                chunk_state = train_section["chunks"].setdefault(
                    str(chunk), {"chunk": chunk, "attempts": {}}
                )
                attempt = latest_attempt_number(chunk_state["attempts"]) + 1
                if attempt > cfg.max_train_attempts:
                    state["completed"] = True
                    state["failed"] = True
                    state["failure"] = {
                        "time": now_iso(),
                        "event": "train_exhausted_attempts",
                        "epoch": next_train_epoch,
                        "chunk": chunk,
                    }
                    return

                output_dir_path = train_attempt_dir(
                    run_dir,
                    epoch=next_train_epoch,
                    chunk=chunk,
                    attempt=attempt,
                )
                checkpoint_in = train_section.get("latest_checkpoint_path")

                task_to_submit = self._train_task
                if cfg.partition_train:
                    task_to_submit = task_to_submit.with_options(
                        partition=cfg.partition_train
                    )

                job = task_to_submit(
                    run_id=cfg.run_id,
                    run_dir=str(run_dir),
                    epoch=next_train_epoch,
                    chunk=chunk,
                    start_step=steps_completed,
                    max_steps=steps_this_chunk,
                    epoch_steps=cfg.epoch_steps,
                    checkpoint_in=checkpoint_in,
                    output_dir=str(output_dir_path),
                    resiliency_config=cfg.resiliency_config,
                )

                train_section["active"] = {
                    "epoch": next_train_epoch,
                    "chunk": chunk,
                    "attempt": attempt,
                    "job_id": job.id,
                    "output_dir": str(output_dir_path),
                    "submitted_at": now_iso(),
                }
                chunk_state["attempts"][str(attempt)] = {
                    "attempt": attempt,
                    "job_id": job.id,
                    "output_dir": str(output_dir_path),
                    "submitted_at": now_iso(),
                }
                append_jsonl(
                    events_log,
                    {
                        "time": now_iso(),
                        "event": "train_attempt_submitted",
                        "epoch": next_train_epoch,
                        "chunk": chunk,
                        "attempt": attempt,
                        "job_id": job.id,
                    },
                )

    def _reconcile_active_train_attempt(
        self,
        state: Dict[str, Any],
        train_section: MutableMapping[str, Any],
        active: Mapping[str, Any],
        epoch: int,
        run_dir: Path,
        events_log: Path,
    ) -> None:
        active_paths = AttemptPaths(output_dir=Path(active["output_dir"]))
        if active_paths.result_path.exists():
            task_result = read_json(active_paths.result_path)
            status_code = task_result.get("status_code")
            checkpoint_out = task_result.get("result_details", {}).get("checkpoint_out")
            if isinstance(checkpoint_out, str) and checkpoint_out:
                train_section["latest_checkpoint_path"] = checkpoint_out
                try:
                    steps = int(
                        read_json(Path(checkpoint_out)).get("steps_completed", 0)
                    )
                    train_section["steps_completed"] = steps
                    write_json_atomic(
                        latest_checkpoint_pointer_path(run_dir),
                        {
                            "checkpoint_path": checkpoint_out,
                            "epoch": epoch,
                            "steps_completed": steps,
                            "updated_at": now_iso(),
                        },
                    )
                except Exception:
                    pass

            chunk_state = train_section["chunks"].setdefault(
                str(active["chunk"]),
                {"chunk": int(active["chunk"]), "attempts": {}},
            )
            attempts = chunk_state["attempts"]
            attempts[str(active["attempt"])] = {
                "attempt": int(active["attempt"]),
                "job_id": active.get("job_id"),
                "output_dir": active.get("output_dir"),
                "status_code": status_code,
                "completed_at": task_result.get("created_at"),
                "checkpoint_out": checkpoint_out,
            }
            train_section["active"] = None
            append_jsonl(
                events_log,
                {
                    "time": now_iso(),
                    "event": "train_attempt_completed",
                    "epoch": epoch,
                    "chunk": active.get("chunk"),
                    "attempt": active.get("attempt"),
                    "status_code": status_code,
                },
            )

            if status_code == StatusCode.NONRETRYABLE_ERROR.value:
                state["completed"] = True
                state["failed"] = True
                state["failure"] = {
                    "time": now_iso(),
                    "event": "train_failed",
                    "epoch": epoch,
                    "chunk": active.get("chunk"),
                    "attempt": active.get("attempt"),
                }
            return

        job_id = active.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return
        try:
            job = self._ctx.cluster.get_job(job_id)
            if not job.is_completed():
                return
            status = job.get_status()
            job_state = status.get("JobState")
            if job_state == "TIMEOUT":
                status_code = StatusCode.TIMEOUT.value
            elif job_state == "CANCELLED":
                status_code = StatusCode.CANCELLED.value
            else:
                status_code = StatusCode.RETRYABLE_ERROR.value

            checkpoint_out_path = active_paths.checkpoint_out_path
            checkpoint_out = (
                str(checkpoint_out_path) if checkpoint_out_path.exists() else None
            )
            chunk_state = train_section["chunks"].setdefault(
                str(active["chunk"]),
                {"chunk": int(active["chunk"]), "attempts": {}},
            )
            chunk_state["attempts"][str(active["attempt"])] = {
                "attempt": int(active["attempt"]),
                "job_id": job_id,
                "output_dir": active.get("output_dir"),
                "status_code": status_code,
                "completed_at": now_iso(),
                "checkpoint_out": checkpoint_out,
                "job_state": job_state,
            }
            train_section["active"] = None
            append_jsonl(
                events_log,
                {
                    "time": now_iso(),
                    "event": "train_attempt_missing_result",
                    "epoch": epoch,
                    "chunk": active.get("chunk"),
                    "attempt": active.get("attempt"),
                    "job_id": job_id,
                    "job_state": job_state,
                },
            )
            if checkpoint_out:
                try:
                    steps = int(
                        read_json(Path(checkpoint_out)).get("steps_completed", 0)
                    )
                    if steps > int(train_section["steps_completed"]):
                        train_section["steps_completed"] = steps
                        train_section["latest_checkpoint_path"] = checkpoint_out
                except Exception:
                    pass
        except Exception:
            return

    def _reconcile_and_submit_eval(
        self, state: Dict[str, Any], run_dir: Path, events_log: Path
    ) -> None:
        cfg = self._cfg
        for epoch in range(cfg.epochs):
            e_state = ensure_epoch_state(state, epoch)
            if int(e_state["train"]["steps_completed"]) < cfg.epoch_steps:
                continue

            eval_section = e_state["eval"]
            if eval_section.get("completed") is True:
                continue

            active = eval_section.get("active")
            if isinstance(active, dict):
                self._reconcile_active_eval_attempt(
                    state, eval_section, active, epoch, events_log
                )

            if (
                eval_section.get("active") is None
                and eval_section.get("completed") is not True
            ):
                attempt = latest_attempt_number(eval_section["attempts"]) + 1
                if attempt > cfg.max_eval_attempts:
                    state["completed"] = True
                    state["failed"] = True
                    state["failure"] = {
                        "time": now_iso(),
                        "event": "eval_exhausted_attempts",
                        "epoch": epoch,
                    }
                    return

                checkpoint_in = e_state["train"].get("latest_checkpoint_path")
                if not isinstance(checkpoint_in, str) or not checkpoint_in:
                    continue

                output_dir_path = eval_attempt_dir(
                    run_dir, epoch=epoch, attempt=attempt
                )
                task_to_submit = self._eval_task
                if cfg.partition_eval:
                    task_to_submit = task_to_submit.with_options(
                        partition=cfg.partition_eval
                    )

                job = task_to_submit(
                    run_id=cfg.run_id,
                    run_dir=str(run_dir),
                    epoch=epoch,
                    checkpoint_in=checkpoint_in,
                    output_dir=str(output_dir_path),
                    resiliency_config=cfg.resiliency_config,
                )
                eval_section["active"] = {
                    "epoch": epoch,
                    "attempt": attempt,
                    "job_id": job.id,
                    "output_dir": str(output_dir_path),
                    "submitted_at": now_iso(),
                }
                eval_section["attempts"][str(attempt)] = {
                    "attempt": attempt,
                    "job_id": job.id,
                    "output_dir": str(output_dir_path),
                    "submitted_at": now_iso(),
                }
                append_jsonl(
                    events_log,
                    {
                        "time": now_iso(),
                        "event": "eval_attempt_submitted",
                        "epoch": epoch,
                        "attempt": attempt,
                        "job_id": job.id,
                    },
                )

    def _reconcile_active_eval_attempt(
        self,
        state: Dict[str, Any],
        eval_section: MutableMapping[str, Any],
        active: Mapping[str, Any],
        epoch: int,
        events_log: Path,
    ) -> None:
        active_paths = AttemptPaths(output_dir=Path(active["output_dir"]))
        if active_paths.result_path.exists():
            task_result = read_json(active_paths.result_path)
            status_code = task_result.get("status_code")
            eval_section["attempts"][str(active["attempt"])] = {
                "attempt": int(active["attempt"]),
                "job_id": active.get("job_id"),
                "output_dir": active.get("output_dir"),
                "status_code": status_code,
                "completed_at": task_result.get("created_at"),
                "metrics_path": task_result.get("result_details", {}).get(
                    "metrics_path"
                ),
            }
            eval_section["active"] = None
            append_jsonl(
                events_log,
                {
                    "time": now_iso(),
                    "event": "eval_attempt_completed",
                    "epoch": epoch,
                    "attempt": active.get("attempt"),
                    "status_code": status_code,
                },
            )
            if status_code == StatusCode.SUCCESS.value:
                eval_section["completed"] = True
                eval_section["metrics_path"] = task_result.get(
                    "result_details", {}
                ).get("metrics_path")
            if status_code == StatusCode.NONRETRYABLE_ERROR.value:
                state["completed"] = True
                state["failed"] = True
                state["failure"] = {
                    "time": now_iso(),
                    "event": "eval_failed",
                    "epoch": epoch,
                    "attempt": active.get("attempt"),
                }
            return

        job_id = active.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return
        try:
            job = self._ctx.cluster.get_job(job_id)
            if not job.is_completed():
                return
            status = job.get_status()
            job_state = status.get("JobState")
            if job_state == "TIMEOUT":
                status_code = StatusCode.TIMEOUT.value
            elif job_state == "CANCELLED":
                status_code = StatusCode.CANCELLED.value
            else:
                status_code = StatusCode.RETRYABLE_ERROR.value

            eval_section["attempts"][str(active["attempt"])] = {
                "attempt": int(active["attempt"]),
                "job_id": job_id,
                "output_dir": active.get("output_dir"),
                "status_code": status_code,
                "completed_at": now_iso(),
                "job_state": job_state,
            }
            eval_section["active"] = None
            append_jsonl(
                events_log,
                {
                    "time": now_iso(),
                    "event": "eval_attempt_missing_result",
                    "epoch": epoch,
                    "attempt": active.get("attempt"),
                    "job_id": job_id,
                    "job_state": job_state,
                },
            )
        except Exception:
            return

    def _all_done(self, state: Dict[str, Any]) -> bool:
        cfg = self._cfg
        for epoch in range(cfg.epochs):
            e_state = ensure_epoch_state(state, epoch)
            if int(e_state["train"]["steps_completed"]) < cfg.epoch_steps:
                return False
            if e_state["eval"].get("completed") is not True:
                return False
        return True


__all__ = [
    "TrainEvalSupervisor",
    "TrainEvalSupervisorConfig",
]
