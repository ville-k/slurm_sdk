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
    apply_replica_output_suffix,
    load_task_arguments,
    parse_args,
    resolve_replica_output_file,
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
    _replica_args_pickle(tmp_path / "peer_worker_replica0_args.pkl", {"env_id": 0})
    _replica_args_pickle(tmp_path / "peer_worker_replica1_args.pkl", {"env_id": 7})
    _replica_args_pickle(tmp_path / "peer_worker_replica2_args.pkl", {"env_id": 42})

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
    _replica_args_pickle(tmp_path / "peer_w_replica0_args.pkl", ("host-0", 0))

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
    _replica_args_pickle(tmp_path / "peer_w_replica3_args.pkl", 42)

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
    _replica_args_pickle(tmp_path / "peer_inference_replica2_args.pkl", {"env_id": 20})
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


# ---------------------------------------------------------------------------
# apply_replica_output_suffix / resolve_replica_output_file — per-replica
# result-file rewriting so N replicas do not race onto one pickle.
# ---------------------------------------------------------------------------


def test_apply_replica_output_suffix_canonical_filename():
    out = apply_replica_output_suffix("slurm_job_foo_result.pkl", 3)
    assert out == "slurm_job_foo_replica3_result.pkl"


def test_apply_replica_output_suffix_preserves_parent_dir():
    out = apply_replica_output_suffix("/jobs/foo/slurm_job_abc_result.pkl", 7)
    assert out == "/jobs/foo/slurm_job_abc_replica7_result.pkl"


def test_apply_replica_output_suffix_non_canonical_falls_back():
    # Weird extension — caller accepts an appended index segment.
    out = apply_replica_output_suffix("weird.output", 5)
    assert out == "weird.output.replica5"


def test_resolve_replica_output_file_noop_for_non_replica():
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="slurm_job_solo_result.pkl",
        callbacks_file="c",
        step="peer:solo",
    )
    assert resolve_replica_output_file(args) == "slurm_job_solo_result.pkl"


def test_resolve_replica_output_file_uses_procid(monkeypatch):
    monkeypatch.setenv("SLURM_PROCID", "2")
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="slurm_job_peer_result.pkl",
        callbacks_file="c",
        step="peer:peer:by-taskid",
    )
    assert resolve_replica_output_file(args) == "slurm_job_peer_replica2_result.pkl"


def test_resolve_replica_output_file_default_procid_zero(monkeypatch):
    monkeypatch.delenv("SLURM_PROCID", raising=False)
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="slurm_job_peer_result.pkl",
        callbacks_file="c",
        step="peer:peer:by-taskid",
    )
    # No PROCID → default to 0 so the runner still picks a valid pickle path
    # even when invoked outside Slurm (tests, local-mode).
    assert resolve_replica_output_file(args) == "slurm_job_peer_replica0_result.pkl"


def test_resolve_replica_output_file_distinct_across_replicas(monkeypatch):
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="slurm_job_peer_result.pkl",
        callbacks_file="c",
        step="peer:peer:by-taskid",
    )
    seen = set()
    for procid in range(4):
        monkeypatch.setenv("SLURM_PROCID", str(procid))
        seen.add(resolve_replica_output_file(args))
    # Four distinct per-replica result files — no collisions.
    assert len(seen) == 4


def test_replica_namespace_cannot_collide_with_singleton_names(tmp_path):
    """Regression: peer "worker_2" vs replica 2 of peer "worker".

    Both used to resolve to peer_worker_2_args.pkl and
    slurm_job_<base>_peer_worker_2_result.pkl — the last heredoc silently
    overwrote the first and both runners raced onto one result file. The
    _replica<N> marker keeps the namespaces disjoint.
    """
    from slurm.parallel.plan import (
        PEER_ARGS_BASENAME,
        PEER_REPLICA_ARGS_BASENAME,
    )

    singleton_args = PEER_ARGS_BASENAME.format(name="worker_2")
    replica_args = PEER_REPLICA_ARGS_BASENAME.format(name="worker", index=2)
    assert singleton_args != replica_args

    singleton_result = "slurm_job_base_peer_worker_2_result.pkl"
    replica_result = apply_replica_output_suffix(
        "slurm_job_base_peer_worker_result.pkl", 2
    )
    assert replica_result != singleton_result
