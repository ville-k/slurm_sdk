"""Rendering for ``parallel(...)`` — single-pool batch script with N srun steps.

Single implicit pool (or an explicit :class:`Topology` with exactly one
pool), one ``#SBATCH`` header, one ``srun`` per peer. The lifecycle is
owned by a Python supervisor (:mod:`slurm.parallel.topology_supervisor`)
invoked at the end of the rendered script; the per-peer ``srun`` commands
are handed to it via a ``plan.json`` file emitted as a base64 heredoc.

Everything structural (sbatch directives, packaging setup, per-peer runner
command construction) reuses helpers from :mod:`slurm.rendering` so the
two rendering paths stay aligned.
"""

from __future__ import annotations

import base64
import logging
import os
import pickle
import shlex
import sys
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .._serialization import dumps_pickled
from ..rendering import (
    CALLBACKS_FILENAME,
    _emit_environment_exports,
    _emit_packaging_setup,
    _emit_python_path_setup,
    _emit_sbatch_directives,
    _escape_quotes,
    _get_importable_module_name,
    _job_directory_setup_lines,
)
from ..task import BoundTask
from .plan import Plan, PlanPeer

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..packaging.base import PackagingStrategy
    from .types import Peer, Pool, _ParallelSpec

logger = logging.getLogger(__name__)


# Names under $JOB_DIR for per-peer artifacts. Kept short so the output
# directory listings read well during debugging.
PEER_ARGS_BASENAME = "peer_{name}_args.pkl"
PEER_KWARGS_BASENAME = "peer_{name}_kwargs.pkl"
PEER_RESULT_BASENAME = "peer_{name}_result.pkl"
# Replica sets (count > 1) write one pickle per replica index. Unlike the
# singleton files above, these hold a single value (dict/tuple/scalar) that
# the runner unpacks with ``unpack_item_to_args_kwargs`` — matching the
# :meth:`SlurmTask.map` item shape.
PEER_REPLICA_ARGS_BASENAME = "peer_{name}_{index}_args.pkl"


def peer_pre_submission_id(base_id: str, peer_name: str) -> str:
    """Derive the per-peer ``pre_submission_id`` used by the peer's :class:`Job`.

    The per-peer id drives the default result-file name that
    :class:`~slurm.job.Job` expects — see
    :attr:`slurm.job.Job._result_filename`. We layer ``peer_<name>`` onto the
    base id so all peers share the same base and ``sacct -j`` groups them
    naturally when inspecting hetjob state.
    """
    return f"{base_id}_peer_{peer_name}"


def _sbatch_params_from_pool(
    pool: "Pool",
    spec: "_ParallelSpec",
    task_defaults: Dict[str, Any],
    sbatch_overrides: Dict[str, Any],
) -> Dict[str, Any]:
    """Build the ``#SBATCH`` parameter dict for a single-pool allocation.

    Precedence (lowest to highest):

    1. Task-decorator defaults from the leader/first peer (``time``, ``mem``,
       ``cpus_per_task``, etc.) — same source the single-job renderer uses.
    2. Pool shape (``nodes``, ``cpus_per_node``, ``mem_per_node``,
       ``gpus_per_node``, ``partition``, ``qos``, ``account``, …).
    3. Spec-level overrides (``time``, ``account``, ``qos``, ``reservation``,
       ``network``).
    4. Ad-hoc ``sbatch_overrides`` from the submission pipeline.
    """
    params: Dict[str, Any] = dict(task_defaults)

    # Pool shape — only fields that the pool declares explicitly.
    params["nodes"] = pool.nodes
    if pool.cpus_per_node is not None:
        # Translate to per-task for compatibility with existing rendering
        # helpers. Single-peer step = 1 task, so cpus_per_task equals the
        # pool's per-node budget in Phase 2.
        params["cpus_per_node"] = pool.cpus_per_node
    if pool.mem_per_node is not None:
        params["mem"] = pool.mem_per_node
    if pool.gpus_per_node is not None and pool.gpus_per_node > 0:
        params["gpus_per_node"] = pool.gpus_per_node
    if pool.partition is not None:
        params["partition"] = pool.partition
    if pool.qos is not None:
        params["qos"] = pool.qos
    if pool.account is not None:
        params["account"] = pool.account
    if pool.constraint is not None:
        params["constraint"] = pool.constraint
    if pool.reservation is not None:
        params["reservation"] = pool.reservation
    if pool.time is not None:
        params["time"] = pool.time
    if pool.exclusive:
        params["exclusive"] = None
    if pool.gpu_type is not None and pool.gpus_per_node:
        params["gres"] = f"gpu:{pool.gpu_type}:{pool.gpus_per_node}"
    for key, value in pool.gres.items():
        # gres= already set above from gpu_type → validator forbids both.
        existing = params.get("gres")
        addition = f"{key}:{value}"
        params["gres"] = f"{existing},{addition}" if existing else addition
    for key, value in pool.extra_sbatch.items():
        params[key] = value

    # Spec-level defaults override the pool where the pool did not say anything.
    if spec.time is not None and "time" not in params:
        params["time"] = spec.time
    if spec.account is not None and "account" not in params:
        params["account"] = spec.account
    if spec.qos is not None and "qos" not in params:
        params["qos"] = spec.qos
    if spec.reservation is not None and "reservation" not in params:
        params["reservation"] = spec.reservation
    if spec.network is not None and "network" not in params:
        params["network"] = spec.network

    params.update(sbatch_overrides)
    return params


def _peer_sbatch_options(peer: "Peer") -> Dict[str, Any]:
    """Return the peer task's ``sbatch_options`` (from its ``@task`` decorator)."""
    bt = peer.task if isinstance(peer.task, BoundTask) else None
    if bt is None:
        return {}
    return dict(bt.task.sbatch_options)


def _srun_flags_for_peer(peer: "Peer") -> List[str]:
    """Compute the per-step ``srun`` flags from the peer's task decorator.

    These mirror what the single-job renderer derives from the ``@task``
    decorator — Phase 2 only needs resources that the single-pool shell
    supervisor honours: ``--ntasks``, ``--cpus-per-task``, ``--mem``, and
    optional ``--gpus``. Advanced placement flags live in Phase 6 / 8.

    For replica peers (``count > 1``) the step runs ``--ntasks=<count>`` and
    each Slurm task inside the step selects its per-replica args by
    ``SLURM_PROCID``. ``--ntasks-per-node`` is emitted when the peer
    declares ``tasks_per_node``.
    """
    opts = _peer_sbatch_options(peer)
    ntasks = peer.count if peer.is_replica_set else 1
    flags: List[str] = ["--exact", "--overlap", f"--ntasks={ntasks}"]
    if peer.is_replica_set and peer.tasks_per_node is not None:
        flags.append(f"--ntasks-per-node={peer.tasks_per_node}")

    cpt = opts.get("cpus_per_task")
    if cpt is not None:
        flags.append(f"--cpus-per-task={cpt}")

    mem = opts.get("mem")
    if mem is not None:
        flags.append(f"--mem={mem}")

    gpt = opts.get("gpus_per_task")
    if gpt is not None and gpt > 0:
        flags.append(f"--gpus-per-task={gpt}")
    elif opts.get("gpus") is not None:
        flags.append(f"--gpus={opts['gpus']}")

    if peer.exclusive:
        flags.append("--exclusive")

    flags.extend(peer.srun_args)

    flags.extend(
        [
            f"--job-name={shlex.quote(peer.resolved_name)}",
        ]
    )
    return flags


def _export_clause(peer: "Peer", pool_name: str) -> str:
    """Build the ``--export=`` clause that seeds peer-identity env vars.

    ``SLURM_SDK_PEER_NAME`` / ``SLURM_SDK_PEER_POOL`` are read by
    :func:`slurm.runtime.build_job_context` to populate
    :attr:`JobContext.peer_name` / :attr:`JobContext.peer_pool` inside the
    step. Replica peers also seed ``SLURM_SDK_REPLICA_COUNT`` so the runtime
    can distinguish replica from non-replica contexts (and pair it with
    ``SLURM_PROCID`` for :attr:`JobContext.replica_index`). Using ``ALL``
    preserves the batch script's environment (``JOB_DIR``, ``PYTHONPATH``,
    packaging variables).
    """
    extras = [
        f"SLURM_SDK_PEER_NAME={peer.resolved_name}",
        f"SLURM_SDK_PEER_POOL={pool_name}",
    ]
    if peer.is_replica_set:
        extras.append(f"SLURM_SDK_REPLICA_COUNT={peer.count}")
    return "--export=ALL," + ",".join(extras)


def _emit_peer_arg_heredocs(
    peer: "Peer",
    pre_submission_id: str,
) -> Tuple[List[str], str, str]:
    """Emit base64 heredocs that unpack per-peer args/kwargs at allocation start.

    The heredocs produce files with the bare ``peer_<name>_*.pkl`` names in
    ``$JOB_DIR``. ``pre_submission_id`` is passed through for logging only —
    the filenames deliberately do *not* include it so the rendered directory
    stays legible to humans.

    Singleton peers (``count == 1``) get one pair of args/kwargs pickles
    built from the :class:`BoundTask` binding. Replica peers (``count > 1``)
    are handled by :func:`_emit_replica_arg_heredocs` — the bare args/kwargs
    files are still emitted for the :attr:`BoundTask` base binding so the
    replica loader can fall back to the shared binding when a replica's
    per-index args are empty.

    Returns:
        (lines, args_basename, kwargs_basename)
    """
    args_basename = PEER_ARGS_BASENAME.format(name=peer.resolved_name)
    kwargs_basename = PEER_KWARGS_BASENAME.format(name=peer.resolved_name)

    bt = peer.task if isinstance(peer.task, BoundTask) else None
    args_tuple: Tuple[Any, ...] = tuple(bt.args) if bt else ()
    kwargs_dict: Dict[str, Any] = dict(bt.kwargs) if bt else {}

    try:
        pickled_args = base64.b64encode(dumps_pickled(args_tuple)).decode()
        pickled_kwargs = base64.b64encode(dumps_pickled(kwargs_dict)).decode()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to pickle arguments for peer {peer.resolved_name!r}.\n\n"
            f"Original error: {exc}\n\n"
            "Task arguments must be pickle-serializable to cross the "
            "submission boundary."
        ) from exc

    # Silence unused-locals for lints: pre_submission_id is already threaded
    # through the callers; we accept it so future phases can extend filenames
    # without changing the signature.
    del pre_submission_id
    # Sanity check — pickle import hygiene; tools like bandit complain if we
    # carry a bare `import pickle` without demonstrating it is exercised.
    assert pickle.HIGHEST_PROTOCOL >= 2

    lines = [
        f"# --- peer {peer.resolved_name}: serialize args ---",
        f'base64 -d > "{args_basename}" << "BASE64_PEER_ARGS_{peer.resolved_name.upper()}"',
        pickled_args,
        f"BASE64_PEER_ARGS_{peer.resolved_name.upper()}",
        f'base64 -d > "{kwargs_basename}" << "BASE64_PEER_KWARGS_{peer.resolved_name.upper()}"',
        pickled_kwargs,
        f"BASE64_PEER_KWARGS_{peer.resolved_name.upper()}",
    ]
    return lines, args_basename, kwargs_basename


def _emit_replica_arg_heredocs(
    peer: "Peer",
    replica_items: List[Any],
) -> List[str]:
    """Emit one base64 heredoc per replica for a replica peer.

    Called with the pre-computed per-index payload list (dict / tuple / scalar)
    prepared at submission time by ``_parallel_submission.resolve_replica_args``.
    Each heredoc materialises ``peer_<name>_<index>_args.pkl`` in ``$JOB_DIR``
    — the runner loads exactly one of these per ``SLURM_PROCID`` via
    :func:`_load_peer_replica_arguments`.
    """
    if len(replica_items) != peer.count:
        raise RuntimeError(
            f"Replica peer {peer.resolved_name!r}: expected {peer.count} "
            f"item(s), got {len(replica_items)}. Internal error — validation "
            "should have rejected this at spec-build time."
        )

    lines: List[str] = [f"# --- replica peer {peer.resolved_name}: per-index args ---"]
    for idx, item in enumerate(replica_items):
        try:
            pickled = base64.b64encode(dumps_pickled(item)).decode()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to pickle replica {idx} args for peer "
                f"{peer.resolved_name!r}: {exc}"
            ) from exc
        filename = PEER_REPLICA_ARGS_BASENAME.format(name=peer.resolved_name, index=idx)
        # Heredoc labels must be unique across the whole script — include the
        # replica index in the label so two replicas of the same peer cannot
        # collide.
        label = f"BASE64_PEER_REPLICA_{peer.resolved_name.upper()}_{idx}"
        lines.extend(
            [
                f'base64 -d > "{filename}" << "{label}"',
                pickled,
                label,
            ]
        )
    return lines


def _emit_peer_srun_command(
    *,
    peer: "Peer",
    pool_name: str,
    base_pre_submission_id: str,
    args_basename: str,
    kwargs_basename: str,
    callbacks_file: str,
    pickled_sys_path: str,
    packaging_strategy: "PackagingStrategy",
) -> str:
    """Build the full ``srun ... python -m slurm.runner ...`` line for one peer.

    The runner command is identical in shape to the one emitted by the
    single-task renderer — same flags, same file locations — but uses the
    per-peer args / result filenames and tags the invocation with
    ``--step peer:<name>``. The whole thing is wrapped by the packaging
    strategy so containerised peers prepend ``srun --container-image=…``.
    """
    peer_pre_id = peer_pre_submission_id(base_pre_submission_id, peer.resolved_name)
    result_basename = PEER_RESULT_BASENAME.format(name=peer.resolved_name)

    # ``Job._result_filename`` expects ``slurm_job_{pre_submission_id}_result.pkl``.
    # We normalise the runner's ``--output-file`` to match so peer Jobs can
    # read their results without overriding the derived filename.
    from ..rendering import RESULT_FILENAME

    result_filename = f"slurm_job_{peer_pre_id}_{RESULT_FILENAME}"

    # Prefer the conventional name but symlink/copy via a second filename so
    # humans reading the directory see ``peer_<name>_result.pkl`` too. For
    # Phase 2 we simply use the conventional name — Job.get_result() pulls
    # from it, and the human-friendly alias can arrive later without breaking
    # the read path.
    del result_basename  # reserved for future peer_<name>_result.pkl aliasing

    # The peer task's SlurmTask — needed for module / function names and for
    # the packaging strategy's ``wrap_execution_command`` hook.
    bt = peer.task if isinstance(peer.task, BoundTask) else None
    if bt is None:
        raise RuntimeError(
            f"Peer {peer.resolved_name!r} did not normalise to a BoundTask. "
            "This is an internal error in the parallel spec pipeline."
        )
    task_func = bt.task.func
    module_name = _get_importable_module_name(task_func)
    func_name = task_func.__name__

    if peer.is_replica_set:
        # Replica dispatch: the runner looks up ``peer_<name>_<procid>_args.pkl``
        # based on ``SLURM_PROCID`` at runtime — no single --args-file applies.
        step_selector = f"peer:{peer.resolved_name}:by-taskid"
        runner_parts = [
            '"$PY_EXEC_RESOLVED"',
            "-m slurm.runner",
            f'--module "{_escape_quotes(module_name)}"',
            f'--function "{_escape_quotes(func_name)}"',
            f"--step {step_selector}",
            f'--output-file "{result_filename}"',
            f'--callbacks-file "{_escape_quotes(callbacks_file)}"',
            f'--sys-path "{_escape_quotes(pickled_sys_path)}"',
            '--job-dir "$JOB_DIR"',
            f'--pre-submission-id "{_escape_quotes(peer_pre_id)}"',
        ]
        # Intentionally omit args/kwargs file; dispatch happens by PROCID.
        del args_basename, kwargs_basename
    else:
        runner_parts = [
            '"$PY_EXEC_RESOLVED"',
            "-m slurm.runner",
            f'--module "{_escape_quotes(module_name)}"',
            f'--function "{_escape_quotes(func_name)}"',
            f"--step peer:{peer.resolved_name}",
            f'--args-file "{args_basename}"',
            f'--kwargs-file "{kwargs_basename}"',
            f'--output-file "{result_filename}"',
            f'--callbacks-file "{_escape_quotes(callbacks_file)}"',
            f'--sys-path "{_escape_quotes(pickled_sys_path)}"',
            '--job-dir "$JOB_DIR"',
            f'--pre-submission-id "{_escape_quotes(peer_pre_id)}"',
        ]
    runner_command = " ".join(runner_parts)

    wrapped = packaging_strategy.wrap_execution_command(
        command=runner_command,
        task=task_func,
        job_id=peer_pre_id,
        job_dir='"$JOB_DIR"',
    )

    srun_flags = _srun_flags_for_peer(peer)
    export = _export_clause(peer, pool_name)
    srun_prefix = "srun " + " ".join(srun_flags + [export])
    return f"{srun_prefix} {wrapped}"


def _serialize_callback(cb: Any) -> "str | None":
    """Serialize a user callback to ``module:qualname`` for the supervisor.

    Returns ``None`` for peers without a callback. Validation has already
    rejected lambdas / nested functions, so callers here can assume a
    resolvable location.
    """
    if cb is None:
        return None
    module = getattr(cb, "__module__", None)
    qualname = getattr(cb, "__qualname__", None)
    if not module or not qualname:
        raise RuntimeError(
            "Callback has no module/qualname; this should have been rejected "
            "by spec validation."
        )
    return f"{module}:{qualname}"


def build_plan(
    *,
    spec: "_ParallelSpec",
    peer_commands: List[Tuple["Peer", str]],
    pool_name: str,
    pre_submission_id: str,
) -> Plan:
    """Translate a ``_ParallelSpec`` + per-peer srun strings into a :class:`Plan`.

    Called by the renderer (and directly by tests that want to round-trip
    plan.json without invoking the full rendering pipeline).
    """
    plan_peers = [
        PlanPeer(
            name=peer.resolved_name,
            pool=peer.pool or pool_name,
            leader=peer.leader,
            on_failure=peer.on_failure,
            max_restarts=peer.max_restarts,
            srun_command_line=cmd,
            callback=_serialize_callback(peer.callback),
            replica_count=peer.count,
        )
        for peer, cmd in peer_commands
    ]
    return Plan(
        peers=plan_peers,
        grace_period_seconds=spec.grace_period_seconds,
        pool_names=[pool_name],
        pre_submission_id=pre_submission_id,
    )


def _emit_plan_heredoc(plan: Plan) -> List[str]:
    """Emit a base64 heredoc that materialises ``$JOB_DIR/plan.json``.

    The heredoc lives alongside the per-peer arg heredocs so the plan file
    appears on disk before the bootstrap reads it, whether the batch script
    runs on the submission host (local backend) or on a remote compute node
    (SSH backend — the whole script is uploaded verbatim).
    """
    encoded = base64.b64encode(plan.to_json().encode("utf-8")).decode("ascii")
    return [
        "# --- parallel supervisor plan ---",
        'base64 -d > "plan.json" << "BASE64_PARALLEL_PLAN"',
        encoded,
        "BASE64_PARALLEL_PLAN",
    ]


def _emit_supervisor_invocation() -> List[str]:
    """Emit the lines that hand control to the Python supervisor.

    Bootstrap runs first (resolves hostnames, writes the registry skeleton),
    then ``exec`` replaces the shell with the supervisor so the supervisor's
    exit code becomes the batch job's exit code and Slurm accounting sees
    the supervisor's PID as the batch step leader.
    """
    return [
        "# --- bootstrap then supervisor ---",
        'echo "Starting parallel bootstrap + supervisor"',
        '"$PY_EXEC_RESOLVED" -m slurm.parallel.topology_bootstrap --job-dir "$JOB_DIR"',
        "BOOTSTRAP_EXIT=$?",
        "if [ $BOOTSTRAP_EXIT -ne 0 ]; then",
        '    echo "Parallel bootstrap failed with code $BOOTSTRAP_EXIT" >&2',
        "    exit $BOOTSTRAP_EXIT",
        "fi",
        'exec "$PY_EXEC_RESOLVED" -m slurm.parallel.topology_supervisor --job-dir "$JOB_DIR"',
    ]


def render_parallel_script(
    *,
    spec: "_ParallelSpec",
    packaging_strategy: "PackagingStrategy",
    target_job_dir: str,
    pre_submission_id: str,
    cluster: Optional["Cluster"],
    task_defaults: Dict[str, Any],
    sbatch_overrides: Dict[str, Any],
    callbacks: Optional[List[Any]] = None,
    replica_items: Optional[Dict[str, List[Any]]] = None,
) -> str:
    """Render the batch script for a single-pool ``parallel(...)`` submission.

    Args:
        spec: Validated :class:`_ParallelSpec`. Must contain exactly one pool
            (multi-pool / hetjob support ships in Phase 6).
        packaging_strategy: Shared packaging strategy — every peer uses the
            same image / venv. Per-peer packaging arrives in Phase 10.
        target_job_dir: Absolute path on the backend where the job runs.
        pre_submission_id: The base SDK id for this allocation. Each peer
            derives its own id via :func:`peer_pre_submission_id`.
        cluster: Active :class:`Cluster`, if any. Used for environment export
            setup (slurmfile, env name, prebuilt images).
        task_defaults: ``#SBATCH`` defaults (typically from the leader/first
            peer's ``@task`` decorator).
        sbatch_overrides: Runtime overrides applied on top of task_defaults
            and pool shape.
        callbacks: Callback instances shipped to every peer. Picklable
            callbacks are base64-encoded into the shared ``callbacks.pkl``.
        replica_items: Optional ``{peer_name: [item_0, item_1, ...]}`` for
            replica peers. Each item is the per-index payload (dict / tuple /
            scalar) already resolved by the submission pipeline — the
            renderer emits one heredoc per replica index. Required for every
            replica peer in ``spec``; raises otherwise.

    Returns:
        The rendered bash script as a single string.
    """
    if len(spec.topology.pools) != 1:
        raise NotImplementedError(
            "render_parallel_script supports single-pool submissions only. "
            "Multi-pool topologies (hetjobs) arrive in Phase 6."
        )
    pool_name, pool = next(iter(spec.topology.pools.items()))

    # We need a representative task_func for helpers that expect one (sbatch
    # directives, packaging setup, environment exports). Prefer the leader,
    # else the first peer. The surface they read (``__name__``, ``__module__``)
    # is homogeneous across peers in a single-pool submission.
    representative_peer = next((p for p in spec.peers if p.leader), spec.peers[0])
    representative_task = (
        representative_peer.task.task.func
        if isinstance(representative_peer.task, BoundTask)
        else representative_peer.task
    )

    sbatch_params = _sbatch_params_from_pool(
        pool, spec, task_defaults, sbatch_overrides
    )

    # "cpus_per_node" is a Pool concept, not an sbatch flag. Flatten it into
    # cpus_per_task here since each peer runs ``--ntasks=1`` in Phase 2 — the
    # pool's per-node budget becomes the allocation's per-task budget. This
    # keeps _emit_sbatch_directives happy (it expects sbatch-style keys).
    if "cpus_per_node" in sbatch_params and "cpus_per_task" not in sbatch_params:
        sbatch_params["cpus_per_task"] = sbatch_params.pop("cpus_per_node")
    else:
        sbatch_params.pop("cpus_per_node", None)

    # Ensure output/error paths are set so the rendered directives reference
    # the job directory. The single-task renderer does this via
    # _resolve_output_paths; we inline a simpler version — parallel Phase 2
    # does not have an array index so the substitution is trivial.
    stdout_path = sbatch_params.get("output") or (
        f"{target_job_dir}/slurm_{pre_submission_id}.out"
    )
    stderr_path = sbatch_params.get("error") or (
        f"{target_job_dir}/slurm_{pre_submission_id}.err"
    )
    sbatch_params["output"] = stdout_path
    sbatch_params["error"] = stderr_path

    script_lines = _emit_sbatch_directives(
        sbatch_params, representative_task, packaging_strategy
    )

    script_lines.append("")
    script_lines.extend(
        _emit_environment_exports(
            target_job_dir, representative_task, packaging_strategy, cluster
        )
    )

    script_lines.append("")
    script_lines.extend(_job_directory_setup_lines())
    script_lines.append("")
    script_lines.extend(
        _emit_packaging_setup(
            packaging_strategy, representative_task, pre_submission_id
        )
    )
    script_lines.append("")

    # Callbacks + sys.path are shared across peers — one heredoc for each.
    pickled_sys_path, callbacks_lines, callbacks_file = _emit_shared_runner_inputs(
        callbacks=callbacks or [],
        pre_submission_id=pre_submission_id,
    )
    script_lines.extend(callbacks_lines)
    script_lines.append("")

    # Per-peer arg serialisation — one heredoc pair per peer, plus one
    # per-replica heredoc for replica peers.
    replica_items = replica_items or {}
    peer_arg_basenames: Dict[str, Tuple[str, str]] = {}
    for peer in spec.peers:
        lines, args_basename, kwargs_basename = _emit_peer_arg_heredocs(
            peer, pre_submission_id
        )
        script_lines.extend(lines)
        script_lines.append("")
        peer_arg_basenames[peer.resolved_name] = (args_basename, kwargs_basename)
        if peer.is_replica_set:
            items = replica_items.get(peer.resolved_name)
            if items is None:
                raise RuntimeError(
                    f"Replica peer {peer.resolved_name!r} is missing "
                    "pre-resolved replica items. The submission pipeline "
                    "must pass replica_items={name: [...]} for every "
                    "replica peer."
                )
            script_lines.extend(_emit_replica_arg_heredocs(peer, items))
            script_lines.append("")

    script_lines.extend(_emit_python_path_setup())
    script_lines.append("")

    # Build per-peer srun commands. The tuple order follows spec.peers so the
    # rendered script respects the caller's declaration order.
    peer_commands: List[Tuple["Peer", str]] = []
    for peer in spec.peers:
        args_basename, kwargs_basename = peer_arg_basenames[peer.resolved_name]
        cmd = _emit_peer_srun_command(
            peer=peer,
            pool_name=pool_name,
            base_pre_submission_id=pre_submission_id,
            args_basename=args_basename,
            kwargs_basename=kwargs_basename,
            callbacks_file=callbacks_file,
            pickled_sys_path=pickled_sys_path,
            packaging_strategy=packaging_strategy,
        )
        peer_commands.append((peer, cmd))

    # Serialise the supervisor plan and hand control to the Python
    # bootstrap/supervisor chain.
    plan = build_plan(
        spec=spec,
        peer_commands=peer_commands,
        pool_name=pool_name,
        pre_submission_id=pre_submission_id,
    )
    script_lines.extend(_emit_plan_heredoc(plan))
    script_lines.append("")
    script_lines.extend(_emit_supervisor_invocation())

    return "\n".join(line.rstrip("\r") for line in "\n".join(script_lines).splitlines())


def _emit_shared_runner_inputs(
    *,
    callbacks: List[Any],
    pre_submission_id: str,
) -> Tuple[str, List[str], str]:
    """Serialize callbacks + sys.path shared across every peer's runner call.

    Returns:
        ``(pickled_sys_path_b64, script_lines, callbacks_filename)``.
    """
    submission_sys_path = [p for p in sys.path if isinstance(p, str) and p]
    repo_root = os.getcwd()
    if repo_root not in submission_sys_path:
        submission_sys_path.insert(0, repo_root)
    try:
        pickled_sys_path = base64.b64encode(dumps_pickled(submission_sys_path)).decode()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to pickle sys.path for parallel submission: {exc}"
        ) from exc

    callbacks_filename = f"slurm_job_{pre_submission_id}_{CALLBACKS_FILENAME}"

    picklable_callbacks: List[Any] = []
    for cb in callbacks:
        try:
            pickle.dumps(cb)
            picklable_callbacks.append(cb)
        except Exception as err:
            logger.debug(
                "Skipping non-picklable callback %s: %s", type(cb).__name__, err
            )

    lines: List[str] = []
    if picklable_callbacks:
        pickled_cbs = base64.b64encode(dumps_pickled(picklable_callbacks)).decode()
        lines.append(f'base64 -d > "{callbacks_filename}" << "BASE64_PARALLEL_CBS"')
        lines.append(pickled_cbs)
        lines.append("BASE64_PARALLEL_CBS")
    else:
        lines.append(f'touch "{callbacks_filename}"')

    return pickled_sys_path, lines, callbacks_filename


__all__ = [
    "render_parallel_script",
    "build_plan",
    "peer_pre_submission_id",
    "PEER_ARGS_BASENAME",
    "PEER_KWARGS_BASENAME",
    "PEER_RESULT_BASENAME",
    "PEER_REPLICA_ARGS_BASENAME",
]
