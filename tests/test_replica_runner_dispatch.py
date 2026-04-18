"""Tests for ``--step peer:<name>:by-taskid`` runner dispatch.

The runner picks the per-replica args pickle based on ``SLURM_PROCID`` and
falls through to ``unpack_item_to_args_kwargs`` for the dict/tuple/scalar
shape. These tests write synthetic pickles and check the loader picks the
right one — no real Slurm involvement.
"""

from __future__ import annotations

import pytest

from slurm._serialization import dumps_pickled
from slurm.runner.initialization import (
    RunnerArgs,
    _load_peer_replica_arguments,
    load_task_arguments,
    parse_args,
)


def _replica_args_pickle(path, payload) -> None:
    path.write_bytes(dumps_pickled(payload))


# ---------------------------------------------------------------------------
# RunnerArgs / argparse
# ---------------------------------------------------------------------------


def test_parser_accepts_by_taskid_step():
    args = parse_args(
        [
            "--module",
            "pkg",
            "--function",
            "fn",
            "--output-file",
            "out.pkl",
            "--callbacks-file",
            "cbs.pkl",
            "--step",
            "peer:worker:by-taskid",
        ]
    )
    assert args.is_peer_step is True
    assert args.is_replica_peer_step is True
    assert args.peer_name == "worker"


def test_non_replica_peer_step_not_flagged_as_replica():
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:solo",
    )
    assert args.is_peer_step is True
    assert args.is_replica_peer_step is False


# ---------------------------------------------------------------------------
# _load_peer_replica_arguments — SLURM_PROCID drives selection
# ---------------------------------------------------------------------------


def test_replica_loader_picks_by_procid_dict(tmp_path, monkeypatch):
    _replica_args_pickle(tmp_path / "peer_worker_0_args.pkl", {"env_id": 0})
    _replica_args_pickle(tmp_path / "peer_worker_1_args.pkl", {"env_id": 7})
    _replica_args_pickle(tmp_path / "peer_worker_2_args.pkl", {"env_id": 42})

    monkeypatch.setenv("SLURM_PROCID", "1")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:worker:by-taskid",
    )
    task_args, task_kwargs = _load_peer_replica_arguments(args, str(tmp_path))
    assert task_args == ()
    assert task_kwargs == {"env_id": 7}


def test_replica_loader_tuple_becomes_positional(tmp_path, monkeypatch):
    _replica_args_pickle(tmp_path / "peer_w_0_args.pkl", ("host-0", 0))

    monkeypatch.setenv("SLURM_PROCID", "0")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:w:by-taskid",
    )
    task_args, task_kwargs = _load_peer_replica_arguments(args, str(tmp_path))
    assert task_args == ("host-0", 0)
    assert task_kwargs == {}


def test_replica_loader_scalar_becomes_first_positional(tmp_path, monkeypatch):
    _replica_args_pickle(tmp_path / "peer_w_3_args.pkl", 42)

    monkeypatch.setenv("SLURM_PROCID", "3")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:w:by-taskid",
    )
    task_args, task_kwargs = _load_peer_replica_arguments(args, str(tmp_path))
    assert task_args == (42,)
    assert task_kwargs == {}


def test_replica_loader_dispatch_via_load_task_arguments(tmp_path, monkeypatch):
    _replica_args_pickle(tmp_path / "peer_inference_2_args.pkl", {"env_id": 20})
    monkeypatch.setenv("SLURM_PROCID", "2")

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:inference:by-taskid",
    )
    task_args, task_kwargs = load_task_arguments(args, str(tmp_path))
    assert task_args == ()
    assert task_kwargs == {"env_id": 20}


def test_replica_loader_errors_without_procid(tmp_path, monkeypatch):
    monkeypatch.delenv("SLURM_PROCID", raising=False)
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:w:by-taskid",
    )
    with pytest.raises(RuntimeError, match="SLURM_PROCID"):
        _load_peer_replica_arguments(args, str(tmp_path))


def test_replica_loader_errors_on_invalid_procid(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_PROCID", "not-an-int")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:w:by-taskid",
    )
    with pytest.raises(RuntimeError, match="not a valid integer"):
        _load_peer_replica_arguments(args, str(tmp_path))


def test_replica_loader_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_PROCID", "0")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:missing:by-taskid",
    )
    with pytest.raises(FileNotFoundError):
        _load_peer_replica_arguments(args, str(tmp_path))
