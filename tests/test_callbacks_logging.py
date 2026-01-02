import logging

from slurm.callbacks.callbacks import (
    LoggerCallback,
    RichLoggerCallback,
    RunBeginContext,
)


def _run_begin_ctx() -> RunBeginContext:
    return RunBeginContext(
        module="mod",
        function="func",
        args_file="args.json",
        kwargs_file="kwargs.json",
        output_file="out.pkl",
    )


def test_logger_callback_configures_runner_logging_once(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "slurm.callbacks.callbacks.configure_sdk_logging",
        lambda level, use_rich: calls.append((level, use_rich)),
    )

    callback = LoggerCallback(log_level=logging.DEBUG)

    ctx = _run_begin_ctx()
    callback.on_begin_run_job_ctx(ctx)
    callback.on_begin_run_job_ctx(ctx)

    assert calls == [(logging.DEBUG, False)]


def test_logger_callback_respects_configure_logging_flag(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "slurm.callbacks.callbacks.configure_sdk_logging",
        lambda level, use_rich: calls.append((level, use_rich)),
    )

    callback = LoggerCallback(log_level=logging.WARNING, configure_logging=False)
    callback.on_begin_run_job_ctx(_run_begin_ctx())

    assert calls == []


def test_rich_logger_callback_configures_runner_logging(monkeypatch):
    calls = []
    printed = []

    monkeypatch.setattr(
        "slurm.callbacks.callbacks.configure_sdk_logging",
        lambda level, use_rich: calls.append((level, use_rich)),
    )

    class DummyConsole:
        def print(self, *args, **kwargs):
            printed.append((args, kwargs))

    monkeypatch.setattr("slurm.callbacks.callbacks.Console", DummyConsole)

    callback = RichLoggerCallback(log_level=logging.WARNING)
    callback.on_begin_run_job_ctx(_run_begin_ctx())
    callback.on_begin_run_job_ctx(_run_begin_ctx())

    assert calls == [(logging.WARNING, True)]
    assert printed  # ensure console paths are exercised
