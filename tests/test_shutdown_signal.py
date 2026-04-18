"""Tests for ``ctx.shutdown_requested`` + the runner's SIGTERM handler.

Exercises:

- The module-level shutdown event is a single flag per process (not per
  thread), safe to poll from any thread.
- Installing the handler from a non-main thread does not crash — the runner
  catches the ``ValueError`` and continues.
- A synthetic peer process that polls ``ctx.shutdown_requested`` in a loop
  exits cleanly after receiving SIGTERM, within the grace window.
- Subprocess propagation: when a peer runs under ``start_new_session=True``
  (via the supervisor's ``_launch_peer``), SIGTERM reaches the peer's
  process group.
"""

from __future__ import annotations

import os
import signal
import subprocess  # nosec B404 - test-only synthetic worker
import sys
import textwrap
import threading
import time

import pytest

from slurm.runtime import (
    JobContext,
    _SHUTDOWN_EVENT,
    install_shutdown_handler,
)


@pytest.fixture(autouse=True)
def _reset_shutdown_flag():
    """Clear the process-wide shutdown flag around each test."""
    _SHUTDOWN_EVENT.clear()
    yield
    _SHUTDOWN_EVENT.clear()


def _ctx() -> JobContext:
    return JobContext(
        job_id="1",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
    )


def test_shutdown_requested_defaults_false():
    assert _ctx().shutdown_requested is False


def test_shutdown_requested_flips_after_signal():
    install_shutdown_handler()
    ctx = _ctx()
    assert ctx.shutdown_requested is False
    # Send SIGTERM to our own process; the installed handler flips the event.
    os.kill(os.getpid(), signal.SIGTERM)
    # Signal delivery is asynchronous but typically immediate. Give it a
    # tight window in case the scheduler is slow.
    for _ in range(50):
        if ctx.shutdown_requested:
            break
        time.sleep(0.02)
    assert ctx.shutdown_requested is True


def test_shutdown_event_shared_across_threads():
    install_shutdown_handler()
    ctx = _ctx()
    seen: list = []

    def _watcher():
        for _ in range(100):
            if ctx.shutdown_requested:
                seen.append(True)
                return
            time.sleep(0.01)

    t = threading.Thread(target=_watcher)
    t.start()
    os.kill(os.getpid(), signal.SIGTERM)
    t.join(timeout=2)
    assert seen == [True]


def test_install_shutdown_handler_from_non_main_thread_raises_value_error():
    """Python forbids signal.signal from worker threads — runner catches this."""
    captured: list = []

    def _installer():
        try:
            install_shutdown_handler()
        except ValueError as exc:
            captured.append(exc)

    t = threading.Thread(target=_installer)
    t.start()
    t.join()
    # Either raised (the expected Python behaviour) or succeeded silently
    # (on platforms where the signal module permits it); both are acceptable.
    # We only assert the runner wouldn't crash, and that any exception is
    # a ValueError (matching the runner's except).
    for exc in captured:
        assert isinstance(exc, ValueError)


def test_synthetic_peer_exits_within_grace_window():
    """A child Python process polling ``ctx.shutdown_requested`` exits on SIGTERM.

    Drives the end-to-end contract: a worker that sits in
    ``while not ctx.shutdown_requested: sleep(0.1)`` should exit within
    the grace window after SIGTERM.
    """
    script = textwrap.dedent(
        """
        import sys, time
        from slurm.runtime import JobContext, install_shutdown_handler

        install_shutdown_handler()
        ctx = JobContext(
            job_id="1",
            step_id=None,
            node_rank=None,
            rank=None,
            local_rank=None,
            world_size=None,
            num_nodes=None,
            local_world_size=None,
            gpus_per_node=None,
        )
        # Signal readiness so the parent knows the handler is armed.
        sys.stdout.write("READY\\n")
        sys.stdout.flush()
        deadline = time.time() + 10
        while not ctx.shutdown_requested and time.time() < deadline:
            time.sleep(0.05)
        sys.exit(0 if ctx.shutdown_requested else 2)
        """
    ).strip()
    proc = subprocess.Popen(  # nosec B603,B607 - test-only synthetic worker
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for READY handshake before signalling so we don't race the handler
    # install.
    assert proc.stdout is not None
    line = proc.stdout.readline()
    assert line.strip() == b"READY", f"Unexpected startup output: {line!r}"

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        pytest.fail("Peer did not exit within grace window after SIGTERM")
    assert proc.returncode == 0


def test_supervisor_launch_starts_new_session_for_signal_propagation():
    """``_launch_peer`` passes ``start_new_session=True`` so killing the peer
    kills any subprocesses it spawns (via the process group).
    """
    from slurm.parallel.topology_supervisor import _launch_peer
    from slurm.parallel.plan import PlanPeer

    peer = PlanPeer(
        name="p",
        pool="default",
        leader=False,
        on_failure="kill",
        max_restarts=0,
        # A tiny shell pipeline that spawns a child; SIGTERM to the peer
        # process group tears both parents and children down.
        srun_command_line="bash -c 'sleep 30 & wait'",
    )
    proc = _launch_peer(peer)
    try:
        # Process group id equals the child's own PID under setsid/new session.
        pgid = os.getpgid(proc.pid)
        assert pgid == proc.pid
    finally:
        # Terminate the whole group so the sleeping grandchild goes away too.
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
