"""Python supervisor for ``parallel(...)`` allocations.

Reads ``$JOB_DIR/plan.json``, launches each peer's ``srun`` (via ``bash -c``
so shell variables exported by the batch script still expand), monitors
exits with a poll loop, and applies per-peer failure policies:

- ``on_failure="kill"`` — a non-zero peer exit triggers cascading shutdown.
- ``on_failure="continue"`` — a non-zero peer exit is logged and tolerated.
- ``on_failure="restart"`` — supervisor re-launches the peer up to
  ``max_restarts`` times before falling through to ``"kill"``. Restart is
  transparent to the outer state machine: the peer keeps the same name /
  command / placement and its entry in the registry is updated atomically.
- ``on_failure="callback"`` — supervisor imports the user's callback by
  fully-qualified name (``module:qualname``) and invokes it with a
  :class:`JobSnapshot` built from the registry + exit state. The callback's
  return value (``"kill"`` / ``"continue"``) dispatches the same code paths
  those policies already use.
- ``leader=True`` — the peer's exit (any outcome) triggers shutdown; the
  leader's exit code becomes the supervisor's exit code.

Cascading shutdown fires two signals: a ``scancel --signal=TERM`` to reach
remote step processes through Slurm, plus a direct SIGTERM to the local
``srun`` wrappers (because ``scancel`` may not exist in tests / local mode).
After ``grace_period_seconds`` the supervisor hard-kills any stragglers.

Each peer's terminal state is recorded in the registry as an ``outcome``
string (see :data:`slurm.parallel.registry.OUTCOME_*`) so
:meth:`ParallelJob.peer_outcomes` can report precisely what happened.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import signal
import subprocess  # nosec B404 - Popen is the supervisor's core mechanism
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .plan import Plan, PlanPeer, read_plan
from .registry import (
    OUTCOME_CONTINUE_ON_FAILURE,
    OUTCOME_FATAL,
    OUTCOME_NOT_STARTED,
    OUTCOME_RESTARTED,
    OUTCOME_SHUTDOWN_BY_LEADER,
    OUTCOME_SUCCESS,
    STATE_FAILED,
    STATE_RUNNING,
    STATE_SHUTDOWN_BY_LEADER,
    STATE_SUCCESS,
    update_peer_entry,
)

logger = logging.getLogger("slurm.parallel.supervisor")


# Module-level shutdown flag toggled by the SIGTERM handler. The main loop
# polls it so external ``scancel`` signals propagate without racing with
# Python's Popen bookkeeping.
_external_shutdown = False


def _install_sigterm_handler() -> None:
    def _handler(signum, _frame):
        global _external_shutdown
        _external_shutdown = True
        logger.info(
            "Supervisor received signal %s — marking external shutdown",
            signum,
        )

    signal.signal(signal.SIGTERM, _handler)
    # Also forward SIGINT (Ctrl+C) the same way so interactive sessions exit
    # cleanly instead of leaving peer processes orphaned.
    signal.signal(signal.SIGINT, _handler)


def _signal_slurm_job(job_id: Optional[str], sig: str = "TERM") -> None:
    """Best-effort ``scancel --signal=<sig> <job_id>``; no-op without scancel."""
    if not job_id:
        return
    try:
        subprocess.run(  # nosec B603,B607 - trusted scancel invocation
            ["scancel", f"--signal={sig}", job_id],
            check=False,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as err:
        logger.debug("scancel --signal=%s not issued: %s", sig, err)


def _hard_cancel_slurm_job(job_id: Optional[str]) -> None:
    """Best-effort ``scancel <job_id>``; no-op without scancel."""
    if not job_id:
        return
    try:
        subprocess.run(  # nosec B603,B607 - trusted scancel invocation
            ["scancel", job_id],
            check=False,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as err:
        logger.debug("scancel (hard) not issued: %s", err)


def _terminate_local_processes(processes: Dict[int, subprocess.Popen]) -> None:
    for pid, proc in list(processes.items()):
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("terminate pid=%s failed: %s", pid, exc)


def _kill_local_processes(processes: Dict[int, subprocess.Popen]) -> None:
    for pid, proc in list(processes.items()):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("kill pid=%s failed: %s", pid, exc)


def _launch_peer(peer: PlanPeer) -> subprocess.Popen:
    """Launch one peer's ``srun`` under ``bash -c``.

    ``bash -c`` matters because ``PlanPeer.srun_command_line`` references
    environment variables (``$PY_EXEC_RESOLVED``, ``$JOB_DIR``) that the
    surrounding batch script exports — the supervisor inherits them, and
    bash expands them when launching the child.
    """
    logger.info("Launching peer %s", peer.name)
    logger.debug("Peer %s command: %s", peer.name, peer.srun_command_line)
    return subprocess.Popen(  # nosec B603 - command comes from plan.json written by the SDK
        ["/bin/bash", "-c", peer.srun_command_line]
    )


def _exit_code_of(proc: subprocess.Popen) -> int:
    """Map a Popen return code to a shell-style exit code."""
    rc = proc.returncode
    if rc is None:
        return 0
    if rc >= 0:
        return rc
    # Negative returncode ⇒ killed by signal (-N == SIGN). Follow shell
    # conventions: 128 + signal number.
    return 128 + (-rc)


def _resolve_callback(fqname: str) -> Callable[..., Any]:
    """Import a user callback from ``"module:qualname"``.

    The qualname walk handles methods and nested class attributes (``A.B.c``)
    even though validation forbids ``<locals>`` / ``<lambda>`` — supporting
    attribute walks keeps the resolver symmetric with Python's normal import
    semantics.

    Raises ImportError / AttributeError (wrapped by ``_invoke_callback``) so
    callers can decide whether to treat a broken callback as a fatal abort.
    """
    module_name, _, qualname = fqname.partition(":")
    if not module_name or not qualname:
        raise ImportError(
            f"Callback {fqname!r} is not a valid 'module:qualname' reference"
        )
    obj: Any = importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    if not callable(obj):
        raise TypeError(f"Callback {fqname!r} resolved to non-callable {obj!r}")
    return obj


def _build_snapshot(
    peer: PlanPeer, exit_code: int, registry_path: Optional[Path]
) -> Any:
    """Construct a :class:`JobSnapshot`-shaped object for a failed peer.

    We import :class:`JobSnapshot` lazily because the supervisor process
    doesn't otherwise need anything from :mod:`slurm.job`, and circular
    imports are cheaper to avoid than to debug. All fields get conservative
    defaults — the caller's callback sees the true exit code / state but
    log tails stay empty because the supervisor does not stream stdout.
    """
    from ..job import JobSnapshot

    state = "COMPLETED" if exit_code == 0 else "FAILED"
    # Format matches ``sacct``: ``<exit>:<signal>``. Signal detection from the
    # Popen-level return code happens in ``_exit_code_of`` — by the time we
    # reach here the signal has already been folded into ``exit_code``.
    exit_code_str = f"{exit_code}:0"
    reason = None
    if registry_path is not None:
        try:
            from .registry import read_registry

            registry = read_registry(registry_path)
            entry = registry.get("peers", {}).get(peer.name, [{}])[0]
            reason = entry.get("message")
        except Exception:  # pragma: no cover - defensive
            reason = None
    return JobSnapshot(
        job_id=os.environ.get("SLURM_JOB_ID", ""),
        state=state,
        exit_code=exit_code_str,
        reason=reason,
        stdout_tail="",
        stderr_tail="",
        elapsed_seconds=None,
        is_terminal=True,
        is_successful=(exit_code == 0),
    )


def _invoke_callback(
    peer: PlanPeer, exit_code: int, registry_path: Optional[Path]
) -> str:
    """Run the user callback, return ``"kill"`` / ``"continue"``.

    Any exception during import or invocation is treated as ``"kill"`` so
    a broken callback fails loudly instead of silently tolerating failures.
    """
    assert peer.callback is not None  # guaranteed by policy routing
    try:
        fn = _resolve_callback(peer.callback)
        snapshot = _build_snapshot(peer, exit_code, registry_path)
        result = fn(snapshot)
    except Exception as exc:
        logger.error(
            "Peer %s callback %s raised %s — treating as kill",
            peer.name,
            peer.callback,
            exc,
        )
        return "kill"
    if result not in ("kill", "continue"):
        logger.error(
            "Peer %s callback %s returned %r (not 'kill'/'continue') — treating as kill",
            peer.name,
            peer.callback,
            result,
        )
        return "kill"
    return result


def _record_outcome(
    registry_path: Optional[Path],
    peer_name: str,
    outcome: str,
    *,
    exit_code: Optional[int] = None,
    state: Optional[str] = None,
    restart_count: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Persist a peer's terminal outcome to the registry atomically."""
    if registry_path is None:
        return
    changes: Dict[str, Any] = {"outcome": outcome}
    if exit_code is not None:
        changes["final_exit_code"] = exit_code
    if state is not None:
        changes["state"] = state
    if restart_count is not None:
        changes["restart_count"] = restart_count
    if message is not None:
        changes["message"] = message
    try:
        update_peer_entry(registry_path, peer_name, 0, **changes)
    except (FileNotFoundError, KeyError, OSError) as exc:
        logger.debug("Could not record outcome for %s: %s", peer_name, exc)


def _increment_restart_count(
    registry_path: Optional[Path], peer_name: str, new_value: int
) -> None:
    """Bump ``restart_count`` atomically before re-launching a peer."""
    if registry_path is None:
        return
    try:
        update_peer_entry(
            registry_path,
            peer_name,
            0,
            restart_count=new_value,
            state=STATE_RUNNING,
        )
    except (FileNotFoundError, KeyError, OSError) as exc:
        logger.debug("Could not bump restart_count for %s: %s", peer_name, exc)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def _resolve_policy(
    peer: PlanPeer,
    exit_code: int,
    restart_count: int,
    registry_path: Optional[Path],
) -> Tuple[str, Optional[str]]:
    """Decide what to do with a non-zero exit.

    Returns ``(action, note)`` where ``action`` is one of:

    - ``"kill"`` — treat this peer as fatal; triggers cascading shutdown.
    - ``"continue"`` — tolerate the failure; keep other peers running.
    - ``"restart"`` — re-launch the peer.

    ``note`` is an optional diagnostic string persisted to the registry
    (e.g. "restart budget exhausted after 3 attempts").
    """
    # Leader failures always win over per-peer policy because the leader
    # drives the group's termination condition; a restarting leader is
    # allowed (Phase 4 semantics) but a leader with no restart budget left
    # still triggers shutdown with its exit code.
    if peer.on_failure == "restart":
        if restart_count < peer.max_restarts:
            return (
                "restart",
                f"restart {restart_count + 1}/{peer.max_restarts} after exit {exit_code}",
            )
        return (
            "kill",
            f"restart budget exhausted after {restart_count} attempt(s)",
        )
    if peer.on_failure == "callback":
        decision = _invoke_callback(peer, exit_code, registry_path)
        return decision, f"callback decided {decision!r}"
    if peer.on_failure == "continue":
        return "continue", None
    # Default + explicit "kill"
    return "kill", None


def run_supervisor(
    plan: Plan,
    *,
    job_id: Optional[str] = None,
    poll_interval: float = 0.05,
    launch: Optional[Callable[[PlanPeer], subprocess.Popen]] = None,
    registry_path: Optional[Path] = None,
) -> int:
    """Launch every peer, apply failure policies, return final exit code.

    Args:
        plan: The parsed :class:`Plan`.
        job_id: Slurm job id used for ``scancel``. When ``None`` (e.g. local
            tests without Slurm), scancel is skipped and only local
            ``Popen.terminate()`` / ``.kill()`` drive shutdown.
        poll_interval: Seconds between ``poll()`` calls. Small enough that
            fast-exiting test peers are observed promptly; large enough to
            avoid burning CPU on long runs.
        launch: Optional factory ``(PlanPeer) -> Popen`` — used by tests to
            substitute synthetic subprocesses. Defaults to
            :func:`_launch_peer`.
        registry_path: Path to ``registry.json`` for outcome recording. When
            ``None``, outcomes are not persisted (tests that don't need
            outcome tracking). The main CLI passes this path so the
            registry stays current.
    """
    launch_fn = launch or _launch_peer

    processes: Dict[int, subprocess.Popen] = {}
    peer_by_pid: Dict[int, PlanPeer] = {}
    # Track restart counts so we can decide whether to restart again when a
    # peer with on_failure="restart" keeps failing.
    restart_count: Dict[str, int] = {peer.name: 0 for peer in plan.peers}
    # Peers that still have an outstanding process (or will be restarted).
    # Used to write OUTCOME_NOT_STARTED for peers that never got a chance
    # (e.g. an upstream peer aborted before they launched — rare but possible
    # if we extend this loop later to stagger launches).
    peers_by_name: Dict[str, PlanPeer] = {peer.name: peer for peer in plan.peers}
    recorded: Dict[str, str] = {}

    for peer in plan.peers:
        proc = launch_fn(peer)
        processes[proc.pid] = proc
        peer_by_pid[proc.pid] = peer

    final_exit = 0
    shutdown_deadline: Optional[float] = None
    hard_killed = False

    while processes:
        # 1. Honour external SIGTERM — propagate to children and schedule
        #    the hard-cancel deadline.
        if _external_shutdown and shutdown_deadline is None:
            logger.info(
                "External shutdown requested — signalling %d peer(s)",
                len(processes),
            )
            shutdown_deadline = time.time() + plan.grace_period_seconds
            _signal_slurm_job(job_id, "TERM")
            _terminate_local_processes(processes)

        # 2. Reap any peers that exited since the last poll.
        for pid in list(processes):
            proc = processes[pid]
            rc = proc.poll()
            if rc is None:
                continue

            del processes[pid]
            peer = peer_by_pid.pop(pid)
            exit_code = _exit_code_of(proc)
            logger.info(
                "Peer %s exited with code %s (leader=%s, policy=%s)",
                peer.name,
                exit_code,
                peer.leader,
                peer.on_failure,
            )

            # Success path — record outcome and (if leader) trigger shutdown.
            if exit_code == 0:
                count = restart_count.get(peer.name, 0)
                outcome = OUTCOME_RESTARTED if count > 0 else OUTCOME_SUCCESS
                _record_outcome(
                    registry_path,
                    peer.name,
                    outcome,
                    exit_code=0,
                    state=STATE_SUCCESS,
                    restart_count=count,
                )
                recorded[peer.name] = outcome
                if peer.leader and shutdown_deadline is None:
                    logger.info(
                        "Leader %s exited cleanly — signalling %d sibling(s)",
                        peer.name,
                        len(processes),
                    )
                    shutdown_deadline = time.time() + plan.grace_period_seconds
                    _signal_slurm_job(job_id, "TERM")
                    _terminate_local_processes(processes)
                continue

            # Non-zero: shutdown-in-progress peers became victims of the
            # cascade. Record shutdown_by_leader outcome unless this peer
            # had already decided its own fate earlier (a leader that
            # itself exited non-zero, for instance).
            if shutdown_deadline is not None and not peer.leader:
                if peer.name not in recorded:
                    _record_outcome(
                        registry_path,
                        peer.name,
                        OUTCOME_SHUTDOWN_BY_LEADER,
                        exit_code=exit_code,
                        state=STATE_SHUTDOWN_BY_LEADER,
                        restart_count=restart_count.get(peer.name, 0),
                    )
                    recorded[peer.name] = OUTCOME_SHUTDOWN_BY_LEADER
                continue

            # Fresh failure — consult the peer's policy.
            action, note = _resolve_policy(
                peer,
                exit_code,
                restart_count.get(peer.name, 0),
                registry_path,
            )

            if action == "restart":
                new_count = restart_count.get(peer.name, 0) + 1
                restart_count[peer.name] = new_count
                _increment_restart_count(registry_path, peer.name, new_count)
                logger.info(
                    "Restarting peer %s (attempt %d/%d)",
                    peer.name,
                    new_count,
                    peer.max_restarts,
                )
                new_proc = launch_fn(peer)
                processes[new_proc.pid] = new_proc
                peer_by_pid[new_proc.pid] = peer
                continue

            if action == "continue":
                _record_outcome(
                    registry_path,
                    peer.name,
                    OUTCOME_CONTINUE_ON_FAILURE,
                    exit_code=exit_code,
                    state=STATE_FAILED,
                    restart_count=restart_count.get(peer.name, 0),
                    message=note,
                )
                recorded[peer.name] = OUTCOME_CONTINUE_ON_FAILURE
                # Leader with continue is rejected at validation time, so
                # reaching here means non-leader continue — no shutdown.
                continue

            # action == "kill" — fatal for the group.
            _record_outcome(
                registry_path,
                peer.name,
                OUTCOME_FATAL,
                exit_code=exit_code,
                state=STATE_FAILED,
                restart_count=restart_count.get(peer.name, 0),
                message=note,
            )
            recorded[peer.name] = OUTCOME_FATAL
            if shutdown_deadline is None:
                # First fatal exit wins. Later exits don't overwrite the
                # recorded exit code so the user sees the original cause.
                final_exit = exit_code
                logger.info(
                    "Shutdown triggered by peer %s — signalling %d sibling(s)",
                    peer.name,
                    len(processes),
                )
                shutdown_deadline = time.time() + plan.grace_period_seconds
                _signal_slurm_job(job_id, "TERM")
                _terminate_local_processes(processes)

        # 3. Enforce grace window — hard-kill anything still running when
        #    the deadline passes.
        if (
            shutdown_deadline is not None
            and not hard_killed
            and time.time() >= shutdown_deadline
            and processes
        ):
            logger.warning(
                "Grace window expired; hard-killing %d remaining peer(s)",
                len(processes),
            )
            _kill_local_processes(processes)
            _hard_cancel_slurm_job(job_id)
            hard_killed = True

        if processes:
            time.sleep(poll_interval)

    # Peers that never had a process (shouldn't happen in this phase — every
    # peer is launched up front — but record it for completeness so the
    # OUTCOME_NOT_STARTED path has a source of truth once later phases stagger
    # launches).
    for name, peer in peers_by_name.items():
        if name not in recorded:
            _record_outcome(
                registry_path,
                name,
                OUTCOME_NOT_STARTED,
                exit_code=None,
                state=STATE_FAILED,
                restart_count=restart_count.get(name, 0),
                message="peer never entered Popen",
            )
            del peer  # silence unused local

    return final_exit


def _configure_logging() -> None:
    loglevel = os.environ.get("SLURM_SDK_LOGLEVEL", "INFO").upper()
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _preload_callbacks(plan: Plan) -> None:
    """Import every plan callback up front so failures surface at start.

    Fail-fast is the point: if a user misconfigures a callback's module
    path, we'd rather die before launching peers than discover the typo
    only when the callback is needed (which may be mid-run, hours in).
    """
    for peer in plan.peers:
        if peer.on_failure == "callback" and peer.callback:
            try:
                _resolve_callback(peer.callback)
            except Exception as exc:
                logger.error(
                    "Failed to resolve callback %s for peer %s: %s",
                    peer.callback,
                    peer.name,
                    exc,
                )
                raise


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Parallel-allocation supervisor")
    parser.add_argument(
        "--job-dir",
        required=True,
        help="Target job directory containing plan.json and registry.json",
    )
    parser.add_argument(
        "--grace",
        type=int,
        default=None,
        help=(
            "Override grace period (seconds) between SIGTERM and SIGKILL "
            "during cascading shutdown. Defaults to the plan's value."
        ),
    )
    args = parser.parse_args(argv)

    _configure_logging()
    _install_sigterm_handler()

    job_dir = Path(args.job_dir)
    plan_path = job_dir / "plan.json"
    plan = read_plan(plan_path)
    if args.grace is not None:
        plan = Plan(
            peers=plan.peers,
            grace_period_seconds=int(args.grace),
            pool_names=plan.pool_names,
            pre_submission_id=plan.pre_submission_id,
            schema_version=plan.schema_version,
        )

    _preload_callbacks(plan)

    registry_path = job_dir / "registry.json"
    job_id = os.environ.get("SLURM_JOB_ID")
    exit_code = run_supervisor(
        plan,
        job_id=job_id,
        registry_path=registry_path if registry_path.exists() else None,
    )
    logger.info("Supervisor exiting with code %s", exit_code)
    return exit_code


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
