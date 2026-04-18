"""Python supervisor for ``parallel(...)`` allocations.

Reads ``$JOB_DIR/plan.json``, launches each peer's ``srun`` (via ``bash -c``
so shell variables exported by the batch script still expand), monitors
exits with a poll loop, and applies per-peer failure policies:

- ``on_failure="kill"`` — a non-zero peer exit triggers cascading shutdown.
- ``on_failure="continue"`` — a non-zero peer exit is logged and tolerated.
- ``leader=True`` — the peer's exit (any outcome) triggers shutdown; the
  leader's exit code becomes the supervisor's exit code.

Cascading shutdown fires two signals: a ``scancel --signal=TERM`` to reach
remote step processes through Slurm, plus a direct SIGTERM to the local
``srun`` wrappers (because ``scancel`` may not exist in tests / local mode).
After ``grace_period_seconds`` the supervisor hard-kills any stragglers.

Restart and callback policies live in :mod:`slurm.parallel.topology_supervisor`
as explicit ``NotImplementedError`` rejects for Phase 3; Phase 4 adds them.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess  # nosec B404 - Popen is the supervisor's core mechanism
import sys
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .plan import Plan, PlanPeer, read_plan

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


def _classify_exit(peer: PlanPeer, exit_code: int) -> Tuple[bool, bool]:
    """Decide whether this peer's exit triggers shutdown and sets final_exit.

    Returns ``(trigger_shutdown, claims_final_exit)``.

    - Leader exit always triggers shutdown; leader exit code wins.
    - kill-policy non-zero triggers shutdown; its exit code wins.
    - continue-policy non-zero is tolerated.
    - Non-leader success is tolerated (others keep running).
    """
    if peer.leader:
        return True, True
    if exit_code != 0 and peer.on_failure == "kill":
        return True, True
    if exit_code != 0 and peer.on_failure == "continue":
        return False, False
    if peer.on_failure not in ("kill", "continue"):
        # Phase 3 scope guard. The submission pipeline should have rejected
        # these earlier; belt and suspenders here surfaces the misuse at
        # supervisor start instead of silently treating it as "kill".
        raise NotImplementedError(
            f"Peer {peer.name!r} has on_failure={peer.on_failure!r} which is "
            "not supported until Phase 4."
        )
    return False, False


def run_supervisor(
    plan: Plan,
    *,
    job_id: Optional[str] = None,
    poll_interval: float = 0.05,
    launch: Optional[Callable[[PlanPeer], subprocess.Popen]] = None,
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
    """
    launch_fn = launch or _launch_peer

    processes: Dict[int, subprocess.Popen] = {}
    peer_by_pid: Dict[int, PlanPeer] = {}

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

            trigger, claims_final = _classify_exit(peer, exit_code)
            if claims_final and shutdown_deadline is None:
                # First "fatal" exit wins. Later exits don't overwrite the
                # recorded exit code so the user sees the original cause.
                final_exit = exit_code
            if trigger and shutdown_deadline is None:
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

    return final_exit


def _configure_logging() -> None:
    loglevel = os.environ.get("SLURM_SDK_LOGLEVEL", "INFO").upper()
    logging.basicConfig(
        level=loglevel,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


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

    plan_path = Path(args.job_dir) / "plan.json"
    plan = read_plan(plan_path)
    if args.grace is not None:
        plan = Plan(
            peers=plan.peers,
            grace_period_seconds=int(args.grace),
            pool_names=plan.pool_names,
            pre_submission_id=plan.pre_submission_id,
            schema_version=plan.schema_version,
        )

    job_id = os.environ.get("SLURM_JOB_ID")
    exit_code = run_supervisor(plan, job_id=job_id)
    logger.info("Supervisor exiting with code %s", exit_code)
    return exit_code


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
