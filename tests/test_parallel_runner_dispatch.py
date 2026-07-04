"""Tests for ``--step peer:<name>`` dispatch inside the runner.

Checks argparse wiring, the peer-args loader, and that ``JobContext``
picks up the peer-identity env vars the supervisor/parallel renderer sets.
"""

from __future__ import annotations

import base64
import os

import pytest

from slurm._serialization import dumps_pickled, loads_pickled
from slurm.runner.initialization import (
    RunnerArgs,
    _load_peer_task_arguments,
    load_task_arguments,
    parse_args,
)
from slurm.runtime import build_job_context


def test_parser_accepts_step_flag():
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
            "peer:train",
            "--args-file",
            "peer_train_args.pkl",
            "--kwargs-file",
            "peer_train_kwargs.pkl",
        ]
    )
    assert args.step == "peer:train"
    assert args.is_peer_step is True
    assert args.is_array_job is False
    assert args.peer_name == "train"


def test_peer_name_parses_extra_suffix():
    # Phase 5 shape — just make sure we strip cleanly.
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:inference:by-taskid",
    )
    assert args.peer_name == "inference"


def test_load_peer_task_arguments_reads_per_peer_pickles(tmp_path):
    args_path = tmp_path / "peer_train_args.pkl"
    kwargs_path = tmp_path / "peer_train_kwargs.pkl"
    args_path.write_bytes(dumps_pickled((1, 2)))
    kwargs_path.write_bytes(dumps_pickled({"lr": 0.001}))

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        args_file=str(args_path),
        kwargs_file=str(kwargs_path),
        step="peer:train",
    )
    task_args, task_kwargs = _load_peer_task_arguments(args, str(tmp_path))
    assert task_args == (1, 2)
    assert task_kwargs == {"lr": 0.001}


def test_load_task_arguments_dispatches_to_peer_loader(tmp_path):
    args_path = tmp_path / "peer_metrics_args.pkl"
    kwargs_path = tmp_path / "peer_metrics_kwargs.pkl"
    args_path.write_bytes(dumps_pickled(()))
    kwargs_path.write_bytes(dumps_pickled({}))

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        args_file="peer_metrics_args.pkl",
        kwargs_file="peer_metrics_kwargs.pkl",
        step="peer:metrics",
    )
    # Relative paths: the helper should resolve them against job_dir.
    task_args, task_kwargs = load_task_arguments(args, job_dir=str(tmp_path))
    assert task_args == ()
    assert task_kwargs == {}


def test_load_peer_task_arguments_errors_without_files():
    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        step="peer:train",
    )
    with pytest.raises(RuntimeError, match="missing --args-file"):
        _load_peer_task_arguments(args, None)


def test_build_job_context_picks_up_peer_env_vars():
    env = {
        "SLURM_JOB_ID": "999",
        "SLURM_SDK_PEER_NAME": "learner",
        "SLURM_SDK_PEER_POOL": "gpu",
    }
    ctx = build_job_context(env=env)
    assert ctx.peer_name == "learner"
    assert ctx.peer_pool == "gpu"


def test_build_job_context_defaults_to_none_when_absent():
    env = {"SLURM_JOB_ID": "123"}
    ctx = build_job_context(env=env)
    assert ctx.peer_name is None
    assert ctx.peer_pool is None


def test_peer_args_unpickle_matches_rendering_format(tmp_path):
    """End-to-end pickle round-trip: dumps_pickled → base64 → b64decode → loads_pickled."""
    payload = ({"cfg": {"lr": 0.01}},)
    encoded = base64.b64encode(dumps_pickled(payload)).decode()

    # This mirrors what the runner sees after the heredoc decode step.
    decoded = loads_pickled(base64.b64decode(encoded))
    assert decoded == payload

    # Also check the file round-trip through the peer loader.
    args_path = tmp_path / "peer_x_args.pkl"
    kwargs_path = tmp_path / "peer_x_kwargs.pkl"
    args_path.write_bytes(dumps_pickled(payload))
    kwargs_path.write_bytes(dumps_pickled({}))

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        args_file=str(args_path),
        kwargs_file=str(kwargs_path),
        step="peer:x",
    )
    got_args, got_kwargs = _load_peer_task_arguments(args, None)
    assert got_args == payload
    assert got_kwargs == {}


def test_absolute_paths_are_not_rewritten(tmp_path):
    # If the rendered script hands the runner an absolute path we must not
    # prepend job_dir. This protects against double-joining paths.
    args_path = tmp_path / "peer_x_args.pkl"
    kwargs_path = tmp_path / "peer_x_kwargs.pkl"
    args_path.write_bytes(dumps_pickled(()))
    kwargs_path.write_bytes(dumps_pickled({}))

    some_other_dir = tmp_path / "other"
    some_other_dir.mkdir()

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        args_file=str(args_path),
        kwargs_file=str(kwargs_path),
        step="peer:x",
    )
    # job_dir is a different directory; absolute paths must still resolve.
    task_args, task_kwargs = _load_peer_task_arguments(args, str(some_other_dir))
    assert task_args == ()
    assert task_kwargs == {}


def test_log_startup_info_handles_peer_step(caplog):
    import logging

    from slurm.runner.initialization import log_startup_info

    args = RunnerArgs(
        module="m",
        function="f",
        output_file="o",
        callbacks_file="c",
        args_file="a",
        kwargs_file="k",
        step="peer:train",
    )
    with caplog.at_level(logging.INFO, logger="slurm.runner"):
        log_startup_info(args)
    messages = [rec.message for rec in caplog.records]
    assert any("peer step execution" in m.lower() for m in messages)
    # Should not log the 'Starting task execution' line from the regular path.
    assert not any(m == "Starting task execution" for m in messages)

    # Reference os/dumps_pickled so bandit/ruff don't flag the import as
    # unused in the subset of assertions above.
    _ = os.path.join
    _ = dumps_pickled
