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

Each peer's terminal state is recorded in the registry as an ``outcome``
string (see :data:`slurm.parallel.registry.OUTCOME_*`) so
:meth:`ParallelJob.peer_outcomes` can report precisely what happened.
"""

from __future__ import annotations

import argparse
import logging
import os
import shlex
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
    OUTCOME_SHUTDOWN_BY_LEADER,
    OUTCOME_SUCCESS,
    STATE_FAILED,
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


def _run_scancel(targets: List[str], *, sig: Optional[str]) -> None:
    """Best-effort ``scancel [--signal=<sig>] <targets...>``; no-op on absence.

    ``sig=None`` issues a hard cancel. Swallows missing-binary / timeout
    errors so shutdown never crashes the supervisor on a host without Slurm.
    """
    if not targets:
        return
    cmd = ["scancel", *([] if sig is None else [f"--signal={sig}"]), *targets]
    try:
        subprocess.run(cmd, check=False, timeout=5)  # nosec B603,B607 - trusted scancel
    except (FileNotFoundError, subprocess.TimeoutExpired) as err:
        logger.debug("scancel %s not issued: %s", cmd[1:], err)


def _signal_slurm_job(job_id: Optional[str], sig: str = "TERM") -> None:
    """Best-effort ``scancel --signal=<sig> <job_id>``."""
    _run_scancel([job_id] if job_id else [], sig=sig)


def _hard_cancel_slurm_job(job_id: Optional[str]) -> None:
    """Best-effort ``scancel <job_id>``; no-op without scancel."""
    _run_scancel([job_id] if job_id else [], sig=None)


def _signal_local_processes(
    processes: Dict[int, subprocess.Popen],
    sig: int,
    *,
    use_process_groups: bool = False,
) -> None:
    """Send ``sig`` (SIGTERM or SIGKILL) to each peer subprocess.

    When ``use_process_groups`` is True (local mode), signal the whole process
    group via :func:`os.killpg` so descendants the peer spawned also receive
    it. Real-Slurm mode relies on ``scancel --signal=...`` for remote step
    processes and only needs ``terminate()``/``kill()`` on the local ``srun``
    wrapper.
    """
    for pid, proc in list(processes.items()):
        try:
            if use_process_groups:
                try:
                    pgid = os.getpgid(pid)
                except ProcessLookupError:
                    continue
                os.killpg(pgid, sig)
            elif sig == signal.SIGKILL:
                proc.kill()
            else:
                proc.terminate()
        except ProcessLookupError:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("signal %s pid=%s failed: %s", sig, pid, exc)


def _extract_runner_argv(srun_command_line: str) -> List[str]:
    """Pull the ``python -m slurm.runner ...`` portion out of an srun line.

    Rendered srun commands follow the shape::

        srun <flags> --export=... "$PY_EXEC_RESOLVED" -m slurm.runner <args>

    Local mode skips the ``srun`` prefix entirely — we need the runner's
    argv so we can launch it directly under the current Python interpreter.
    Split on the ``"$PY_EXEC_RESOLVED"`` sentinel; the tail is the runner
    invocation modulo the interpreter token (which the caller prepends
    using :data:`sys.executable`).

    Returns:
        A list of tokens starting at ``-m`` (inclusive). Raises
        :class:`RuntimeError` when the sentinel is absent — that would
        indicate a rendering change that broke the local-mode contract.
    """
    sentinel = '"$PY_EXEC_RESOLVED"'
    idx = srun_command_line.find(sentinel)
    if idx < 0:
        raise RuntimeError(
            "Cannot extract runner argv from srun command line — "
            f"missing sentinel {sentinel!r}: {srun_command_line!r}"
        )
    tail = srun_command_line[idx + len(sentinel) :].strip()
    # shlex handles the double-quoted arg values the renderer emits.
    return shlex.split(tail)


def _launch_peer_local(
    peer: PlanPeer,
    *,
    registry_path: Optional[Path] = None,
    shared_dir: Optional[Path] = None,
    replica_index: int = 0,
    replica_count: int = 1,
    job_id: Optional[str] = None,
) -> subprocess.Popen:
    """Launch one peer's runner directly (no ``srun``) for local mode.

    Real Slurm would use ``srun`` to spawn the runner with
    ``SLURM_PROCID`` / ``SLURM_NTASKS`` / ``SLURM_JOB_ID`` set. In local
    mode we synthesise those env vars per subprocess so
    :func:`build_job_context` sees a coherent picture.

    For replica peers (``replica_count > 1``) this helper is called once
    per replica index — each call gets its own ``SLURM_PROCID``.
    """
    argv = _extract_runner_argv(peer.srun_command_line)
    env = os.environ.copy()
    env["PY_EXEC_RESOLVED"] = sys.executable
    # The runner argv carries literal ``$JOB_DIR`` tokens because the real
    # srun path expects bash expansion. In local mode we do the expansion
    # ourselves so the runner sees concrete paths (``os.path.expandvars``
    # relies on the supervisor's env which inherits ``JOB_DIR`` from the
    # backend bootstrap).
    argv = [os.path.expandvars(arg) for arg in argv]
    cmd = [sys.executable, *argv]
    logger.info(
        "Launching peer %s[%d/%d] locally: %s",
        peer.name,
        replica_index,
        replica_count,
        cmd,
    )
    # Synthesize the SLURM_* env vars the runner's JobContext reads.
    synthetic_job_id = job_id or str(os.getpid())
    env["SLURM_JOB_ID"] = synthetic_job_id
    env["SLURM_PROCID"] = str(replica_index)
    env["SLURM_NTASKS"] = str(replica_count)
    env["SLURM_SDK_PEER_NAME"] = peer.name
    env["SLURM_SDK_PEER_POOL"] = peer.pool
    if replica_count > 1:
        env["SLURM_SDK_REPLICA_COUNT"] = str(replica_count)
    if registry_path is not None:
        env["SLURM_SDK_REGISTRY_PATH"] = str(registry_path)
    if shared_dir is not None:
        env["SLURM_SDK_SHARED_DIR"] = str(shared_dir)

    # Each peer gets its own process group so a cascading SIGTERM to one
    # process group doesn't take down siblings accidentally and so the
    # supervisor can ``killpg`` descendants too.
    return subprocess.Popen(  # nosec B603 - runner argv comes from SDK-rendered plan.json
        cmd,
        env=env,
        start_new_session=True,
    )


def _launch_peer(
    peer: PlanPeer,
    *,
    registry_path: Optional[Path] = None,
    shared_dir: Optional[Path] = None,
) -> subprocess.Popen:
    """Launch one peer's ``srun`` under ``bash -c``.

    ``bash -c`` matters because ``PlanPeer.srun_command_line`` references
    environment variables (``$PY_EXEC_RESOLVED``, ``$JOB_DIR``) that the
    surrounding batch script exports — the supervisor inherits them, and
    bash expands them when launching the child.

    ``registry_path`` is exported as ``SLURM_SDK_REGISTRY_PATH`` so peer
    code can build a :class:`JobContext` whose ``peers`` mapping points
    at the live registry. We pass the env through ``env=`` (copying the
    supervisor's own environment) rather than relying on shell ``export``
    so the variable propagates through ``srun`` to the remote step.
    """
    logger.info("Launching peer %s", peer.name)
    cmd = peer.srun_command_line
    logger.debug("Peer %s command: %s", peer.name, cmd)
    env = os.environ.copy()
    if registry_path is not None:
        env["SLURM_SDK_REGISTRY_PATH"] = str(registry_path)
    if shared_dir is not None:
        env["SLURM_SDK_SHARED_DIR"] = str(shared_dir)
    # ``os.setsid`` gives every peer its own process group so a SIGTERM
    # that the runner installs a handler for still propagates to any
    # subprocesses the peer spawns (tensorboard, node-exporter, ...). The
    # supervisor already calls ``proc.terminate()`` on each Popen; the
    # process group ensures descendants receive the signal too.
    return subprocess.Popen(  # nosec B603 - command comes from plan.json written by the SDK
        ["/bin/bash", "-c", cmd],
        env=env,
        start_new_session=True,
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


def _record_outcome(
    registry_path: Optional[Path],
    peer: "PlanPeer",
    outcome: str,
    *,
    exit_code: Optional[int] = None,
    state: Optional[str] = None,
    message: Optional[str] = None,
) -> None:
    """Persist a peer's terminal outcome to the registry atomically.

    For replica peers (``peer.replica_count > 1``) the same outcome is
    written to every replica entry — Phase 5's supervisor treats the srun
    step as one atomic unit, so every replica inherits the step's exit code.
    Phase 7 will add per-replica granular outcomes (via runner-written state
    files) at which point this helper should narrow its write surface.
    """
    if registry_path is None:
        return
    changes: Dict[str, Any] = {"outcome": outcome}
    if exit_code is not None:
        changes["final_exit_code"] = exit_code
    if state is not None:
        changes["state"] = state
    if message is not None:
        changes["message"] = message
    count = max(1, peer.replica_count)
    for replica_index in range(count):
        try:
            update_peer_entry(registry_path, peer.name, replica_index, **changes)
        except (FileNotFoundError, KeyError, OSError, IndexError) as exc:
            logger.debug(
                "Could not record outcome for %s[%d]: %s",
                peer.name,
                replica_index,
                exc,
            )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def _resolve_policy(peer: PlanPeer) -> Tuple[str, Optional[str]]:
    """Decide what to do with a non-zero exit.

    Returns ``(action, note)`` where ``action`` is one of:

    - ``"kill"`` — treat this peer as fatal; triggers cascading shutdown.
    - ``"continue"`` — tolerate the failure; keep other peers running.

    ``note`` is an optional diagnostic string persisted to the registry.
    """
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
    shared_dir: Optional[Path] = None,
    local_mode: bool = False,
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
        local_mode: When ``True``, peers are launched directly (no ``srun``)
            via :func:`_launch_peer_local`. Replica peers spawn one subprocess
            per replica (real srun would use ``--ntasks=N`` to do this for us).
            Shutdown uses process-group SIGTERM / SIGKILL instead of ``scancel``
            because there is no Slurm controller in local mode.
    """
    if launch is None:
        # Bind the registry path into the default launcher so peer ``Popen``s
        # inherit ``SLURM_SDK_REGISTRY_PATH``. Tests that supply their own
        # launcher are responsible for setting the env var themselves (or
        # skipping it — most tests don't need peer-discovery surfaces).
        from functools import partial as _partial

        if local_mode:
            # Local mode launches peers as direct Python subprocesses.
            launch_fn = None  # type: ignore[assignment]
        else:
            launch_fn = _partial(
                _launch_peer, registry_path=registry_path, shared_dir=shared_dir
            )
    else:
        launch_fn = launch

    processes: Dict[int, subprocess.Popen] = {}
    peer_by_pid: Dict[int, PlanPeer] = {}
    # Peers that still have an outstanding process.
    # Used to write OUTCOME_NOT_STARTED for peers that never got a chance
    # (e.g. an upstream peer aborted before they launched — rare but possible
    # if we extend this loop later to stagger launches).
    peers_by_name: Dict[str, PlanPeer] = {peer.name: peer for peer in plan.peers}
    recorded: Dict[str, str] = {}
    # In local mode we spawn one subprocess per replica (real srun would use
    # --ntasks=N inside one step). Track which peer "owns" each Popen so the
    # policy logic can treat every child uniformly while still consulting
    # the peer's on_failure setting.
    #
    # We close over the registry / shared dir / job_id when launching, so
    # the default launcher keeps its minimal (peer,) signature while the
    # local launcher can accept replica_index / replica_count.

    def _launch_one(peer: PlanPeer, replica_index: int) -> subprocess.Popen:
        """Launch one subprocess for ``peer`` — real Slurm or local mode.

        In local mode we call :func:`_launch_peer_local` with the replica
        coordinates. In Slurm mode we fall through to the caller-provided
        (or default) launch function, which launches exactly one ``srun``
        per peer regardless of replica count.
        """
        if local_mode:
            return _launch_peer_local(
                peer,
                registry_path=registry_path,
                shared_dir=shared_dir,
                replica_index=replica_index,
                replica_count=max(1, peer.replica_count),
                job_id=job_id,
            )
        assert launch_fn is not None
        return launch_fn(peer)

    for peer in plan.peers:
        launches = max(1, peer.replica_count) if local_mode else 1
        for replica_index in range(launches):
            proc = _launch_one(peer, replica_index)
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
            if not local_mode:
                _signal_slurm_job(job_id, "TERM")
            _signal_local_processes(
                processes, signal.SIGTERM, use_process_groups=local_mode
            )

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
                _record_outcome(
                    registry_path,
                    peer,
                    OUTCOME_SUCCESS,
                    exit_code=0,
                    state=STATE_SUCCESS,
                )
                recorded[peer.name] = OUTCOME_SUCCESS
                if peer.leader and shutdown_deadline is None:
                    logger.info(
                        "Leader %s exited cleanly — signalling %d sibling(s)",
                        peer.name,
                        len(processes),
                    )
                    shutdown_deadline = time.time() + plan.grace_period_seconds
                    if not local_mode:
                        _signal_slurm_job(job_id, "TERM")
                    _signal_local_processes(
                        processes, signal.SIGTERM, use_process_groups=local_mode
                    )
                continue

            # Non-zero: shutdown-in-progress peers became victims of the
            # cascade. Record shutdown_by_leader outcome unless this peer
            # had already decided its own fate earlier (a leader that
            # itself exited non-zero, for instance).
            if shutdown_deadline is not None and not peer.leader:
                if peer.name not in recorded:
                    _record_outcome(
                        registry_path,
                        peer,
                        OUTCOME_SHUTDOWN_BY_LEADER,
                        exit_code=exit_code,
                        state=STATE_SHUTDOWN_BY_LEADER,
                    )
                    recorded[peer.name] = OUTCOME_SHUTDOWN_BY_LEADER
                continue

            # Fresh failure — consult the peer's policy.
            action, note = _resolve_policy(peer)

            if action == "continue":
                _record_outcome(
                    registry_path,
                    peer,
                    OUTCOME_CONTINUE_ON_FAILURE,
                    exit_code=exit_code,
                    state=STATE_FAILED,
                    message=note,
                )
                recorded[peer.name] = OUTCOME_CONTINUE_ON_FAILURE
                # Leader with continue is rejected at validation time, so
                # reaching here means non-leader continue — no shutdown.
                continue

            # action == "kill" — fatal for the group.
            _record_outcome(
                registry_path,
                peer,
                OUTCOME_FATAL,
                exit_code=exit_code,
                state=STATE_FAILED,
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
                if not local_mode:
                    _signal_slurm_job(job_id, "TERM")
                _signal_local_processes(
                    processes, signal.SIGTERM, use_process_groups=local_mode
                )

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
            _signal_local_processes(
                processes, signal.SIGKILL, use_process_groups=local_mode
            )
            if not local_mode:
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
                peer,
                OUTCOME_NOT_STARTED,
                exit_code=None,
                state=STATE_FAILED,
                message="peer never entered Popen",
            )

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
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help=(
            "Launch peers directly (no ``srun``) with synthesized "
            "SLURM_* env vars. Used by LocalBackend when Slurm is not "
            "installed on the developer workstation."
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

    registry_path = job_dir / "registry.json"
    shared_dir = job_dir / "shared"
    job_id = os.environ.get("SLURM_JOB_ID")
    exit_code = run_supervisor(
        plan,
        job_id=job_id,
        registry_path=registry_path if registry_path.exists() else None,
        shared_dir=shared_dir if shared_dir.exists() else None,
        local_mode=bool(args.local_mode),
    )
    logger.info("Supervisor exiting with code %s", exit_code)
    return exit_code


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
