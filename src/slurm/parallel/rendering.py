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
import pathlib
import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .._serialization import dumps_pickled
from ..rendering import (
    CALLBACKS_FILENAME,
    _build_environment_exports_map,
    _filter_picklable_callbacks,
    _emit_export_assignments,
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


@dataclass(frozen=True)
class SharedRunnerInputs:
    """Callbacks/sys.path payload shared across every peer runner."""

    pickled_sys_path: str
    callbacks_filename: str
    callbacks_payload_b64: Optional[str]


@dataclass(frozen=True)
class PeerArtifactBundle:
    """Serialized args/kwargs payloads for one peer."""

    args_basename: str
    kwargs_basename: str
    args_payload_b64: str
    kwargs_payload_b64: str
    replica_payloads_b64: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparedParallelSubmission:
    """Internal bundle shared by script rendering and local-mode prep."""

    plan: Plan
    representative_task: Any
    environment_exports: Dict[str, str]
    python_runtime_exports: Dict[str, str]
    shared_inputs: SharedRunnerInputs
    peer_artifacts: Dict[str, PeerArtifactBundle]
    peer_packaging_strategies: Dict[str, "PackagingStrategy"]
    unique_packaging_strategies: Tuple["PackagingStrategy", ...]
    target_job_dir: str
    pre_submission_id: str


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


def _build_local_python_runtime_exports() -> Dict[str, str]:
    """Build the Python runtime env the local prep path must preserve.

    The rendered batch script exports the same values via
    :func:`slurm.rendering._emit_python_path_setup`. Local mode launches
    bootstrap/supervisor directly from Python, so it needs the concrete env
    mapping as well — especially ``PYTHONPATH`` and ``PY_EXEC_RESOLVED`` so
    peer subprocesses import the same code that the rendered script would.
    """
    submission_sys_path = [p for p in sys.path if isinstance(p, str) and p]
    repo_root = os.getcwd()
    if repo_root not in submission_sys_path:
        submission_sys_path.insert(0, repo_root)
    try:
        _slurm_mod = importlib.import_module("slurm")
        slurm_parent = pathlib.Path(_slurm_mod.__file__).resolve().parent.parent
        slurm_parent_str = str(slurm_parent)
        if slurm_parent_str not in submission_sys_path:
            submission_sys_path.insert(0, slurm_parent_str)
    except Exception:
        pass

    pythonpath_contrib = ":".join(submission_sys_path)
    exports: Dict[str, str] = {
        "PY_EXEC_RESOLVED": os.environ.get("PY_EXEC", "python"),
        "PYTHONUNBUFFERED": "1",
    }
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if pythonpath_contrib and existing_pythonpath:
        exports["PYTHONPATH"] = f"{pythonpath_contrib}:{existing_pythonpath}"
    elif pythonpath_contrib:
        exports["PYTHONPATH"] = pythonpath_contrib
    elif existing_pythonpath:
        exports["PYTHONPATH"] = existing_pythonpath
    return exports


def peer_pre_submission_id(base_id: str, peer_name: str) -> str:
    """Derive the per-peer ``pre_submission_id`` used by the peer's :class:`Job`.

    The per-peer id drives the default result-file name that
    :class:`~slurm.job.Job` expects — see
    :attr:`slurm.job.Job._result_filename`. We layer ``peer_<name>`` onto the
    base id so all peers share the same base and ``sacct -j`` groups them
    naturally when inspecting allocation state.
    """
    return f"{base_id}_peer_{peer_name}"


def _sbatch_params_from_pool(
    pool: "Pool",
    spec: "_ParallelSpec",
    task_defaults: Dict[str, Any],
    sbatch_overrides: Dict[str, Any],
    *,
    pool_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the ``#SBATCH`` parameter dict for a single-pool allocation.

    Precedence (lowest to highest):

    1. Task-decorator defaults from the leader/first peer (``time``,
       ``job_name``, etc.) — with per-task *resource* keys stripped: the
       pool owns allocation sizing, and per-peer claims are emitted on each
       step's ``srun`` line instead.
    2. Pool shape (``nodes``, ``cpus_per_node`` → ``--mincpus``,
       ``mem_per_node``, ``gpus_per_node``, ``partition``, ``qos``,
       ``account``, …).
    3. Spec-level overrides (``time``, ``account``, ``qos``, ``reservation``,
       ``network``).
    4. Ad-hoc ``sbatch_overrides`` from the submission pipeline.
    """
    # The pool is authoritative for allocation sizing. Per-task resource
    # claims from the representative task's decorator must not leak into
    # the batch header: combined with the summed ``ntasks`` below they
    # multiply (ntasks × cpus-per-task), requesting N× the pool's budget.
    # Per-peer claims belong on each step's srun line, not the header.
    _PER_TASK_RESOURCE_KEYS = {
        "cpus_per_task",
        "mem",
        "mem_per_cpu",
        "gpus",
        "gpus_per_task",
        "gpus_per_node",
        "gres",
        "nodes",
        "ntasks",
        "ntasks_per_node",
        "mincpus",
    }
    params: Dict[str, Any] = {
        k: v for k, v in task_defaults.items() if k not in _PER_TASK_RESOURCE_KEYS
    }

    # Pool shape — only fields that the pool declares explicitly.
    params["nodes"] = pool.nodes
    if pool.cpus_per_node is not None:
        # Per-node CPU floor. ``--mincpus`` sizes the allocation to the
        # pool's per-node budget without multiplying by ntasks the way
        # ``--cpus-per-task`` would (total CPUs = ntasks × cpus-per-task).
        params["mincpus"] = pool.cpus_per_node
    if pool.mem_per_node is not None:
        params["mem"] = pool.mem_per_node
    if pool.gpus_per_node is not None and pool.gpus_per_node > 0:
        if pool.gpu_type is None:
            params["gpus_per_node"] = pool.gpus_per_node
        else:
            # Typed GPUs are expressed via --gres only; also emitting
            # --gpus-per-node makes some Slurm versions reject the pair as
            # conflicting GPU specifications.
            params["gres"] = f"gpu:{pool.gpu_type}:{pool.gpus_per_node}"
    if pool.partition is not None:
        params["partition"] = pool.partition
    if pool.qos is not None:
        params["qos"] = pool.qos
    if pool.account is not None:
        params["account"] = pool.account
    if pool.constraint is not None:
        params["constraint"] = pool.constraint
    if pool.features:
        # Features AND-join into the constraint expression (design §5.3).
        joined = "&".join(pool.features)
        existing_constraint = params.get("constraint")
        params["constraint"] = (
            f"{existing_constraint}&{joined}" if existing_constraint else joined
        )
    if pool.exclude_nodes:
        params["exclude"] = ",".join(pool.exclude_nodes)
    if pool.reservation is not None:
        params["reservation"] = pool.reservation
    if pool.time is not None:
        params["time"] = pool.time
    if pool.exclusive:
        params["exclusive"] = None
    for key, value in pool.gres.items():
        # gres= may already be set from gpu_type → validator forbids both
        # for the "gpu" key, so this only ever appends distinct resources.
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

    # Outer allocation must declare enough ``ntasks`` for every peer's
    # ``srun --ntasks=<peer.count>`` step to fit. Without this the
    # supervisor's srun inside the allocation gets rejected with
    # "More processors requested than permitted" — Slurm tracks per-step
    # task budgets against the parent allocation's ntasks. Sum across every
    # peer targeting this pool gives the densest-packing upper bound
    # (peers run concurrently inside the allocation).
    resolved_pool_name = pool_name or spec.topology.default_pool
    peer_ntasks_total = sum(
        max(1, peer.count) for peer in spec.peers if peer.pool == resolved_pool_name
    )
    if peer_ntasks_total > 0:
        existing_ntasks = params.get("ntasks")
        if existing_ntasks is None or int(existing_ntasks) < peer_ntasks_total:
            params["ntasks"] = peer_ntasks_total

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
    decorator. Resource flags (``--ntasks``, ``--cpus-per-task``, ``--mem``,
    ``--gpus``) come from the peer's underlying ``@task`` decorator.

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


def _build_peer_artifact_bundle(
    peer: "Peer",
    *,
    replica_items: Optional[List[Any]] = None,
) -> PeerArtifactBundle:
    """Serialize the per-peer artifact payloads needed before launch."""
    args_basename = PEER_ARGS_BASENAME.format(name=peer.resolved_name)
    kwargs_basename = PEER_KWARGS_BASENAME.format(name=peer.resolved_name)

    bt = peer.task if isinstance(peer.task, BoundTask) else None
    args_tuple: Tuple[Any, ...] = tuple(bt.args) if bt else ()
    kwargs_dict: Dict[str, Any] = dict(bt.kwargs) if bt else {}

    try:
        args_payload_b64 = base64.b64encode(dumps_pickled(args_tuple)).decode()
        kwargs_payload_b64 = base64.b64encode(dumps_pickled(kwargs_dict)).decode()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to pickle arguments for peer {peer.resolved_name!r}.\n\n"
            f"Original error: {exc}\n\n"
            "Task arguments must be pickle-serializable to cross the "
            "submission boundary."
        ) from exc

    if replica_items is None:
        replica_payloads_b64: Tuple[str, ...] = ()
    else:
        if len(replica_items) != peer.count:
            raise RuntimeError(
                f"Replica peer {peer.resolved_name!r}: expected {peer.count} "
                f"item(s), got {len(replica_items)}. Internal error — validation "
                "should have rejected this at spec-build time."
            )
        payloads: List[str] = []
        for idx, item in enumerate(replica_items):
            try:
                payloads.append(base64.b64encode(dumps_pickled(item)).decode())
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to pickle replica {idx} args for peer "
                    f"{peer.resolved_name!r}: {exc}"
                ) from exc
        replica_payloads_b64 = tuple(payloads)

    assert pickle.HIGHEST_PROTOCOL >= 2
    return PeerArtifactBundle(
        args_basename=args_basename,
        kwargs_basename=kwargs_basename,
        args_payload_b64=args_payload_b64,
        kwargs_payload_b64=kwargs_payload_b64,
        replica_payloads_b64=replica_payloads_b64,
    )


def _emit_peer_artifact_lines(
    peer_name: str,
    artifact: PeerArtifactBundle,
) -> List[str]:
    """Emit the shell lines that materialise one peer's shared pickle files."""
    label_base = peer_name.upper()
    return [
        f"# --- peer {peer_name}: serialize args ---",
        f'base64 -d > "{artifact.args_basename}" << "BASE64_PEER_ARGS_{label_base}"',
        artifact.args_payload_b64,
        f"BASE64_PEER_ARGS_{label_base}",
        f'base64 -d > "{artifact.kwargs_basename}" << "BASE64_PEER_KWARGS_{label_base}"',
        artifact.kwargs_payload_b64,
        f"BASE64_PEER_KWARGS_{label_base}",
    ]


def _emit_replica_artifact_lines(
    peer_name: str,
    replica_payloads_b64: Tuple[str, ...],
) -> List[str]:
    """Emit the shell lines for replica-specific pickle payloads."""
    lines: List[str] = [f"# --- replica peer {peer_name}: per-index args ---"]
    for idx, payload_b64 in enumerate(replica_payloads_b64):
        filename = PEER_REPLICA_ARGS_BASENAME.format(name=peer_name, index=idx)
        label = f"BASE64_PEER_REPLICA_{peer_name.upper()}_{idx}"
        lines.extend(
            [
                f'base64 -d > "{filename}" << "{label}"',
                payload_b64,
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

    Args:
        spec: The validated parallel spec.
        peer_commands: ``(peer, srun_command_line)`` pairs in declaration order.
        pool_name: The single pool name for the allocation.
        pre_submission_id: SDK base id for logging.
    """
    plan_peers = []
    for peer, cmd in peer_commands:
        peer_pool = peer.pool or pool_name
        plan_peers.append(
            PlanPeer(
                name=peer.resolved_name,
                pool=peer_pool,
                leader=peer.leader,
                on_failure=peer.on_failure,
                srun_command_line=cmd,
                replica_count=peer.count,
            )
        )

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


def prepare_parallel_submission(
    *,
    spec: "_ParallelSpec",
    packaging_strategy: "PackagingStrategy",
    target_job_dir: str,
    pre_submission_id: str,
    cluster: Optional["Cluster"],
    callbacks: Optional[List[Any]] = None,
    replica_items: Optional[Dict[str, List[Any]]] = None,
    peer_packaging_strategies: Optional[Dict[str, "PackagingStrategy"]] = None,
) -> PreparedParallelSubmission:
    """Build the artifact/environment bundle shared by render and local prep."""
    if peer_packaging_strategies is None:
        peer_packaging_strategies = {
            peer.resolved_name: packaging_strategy for peer in spec.peers
        }
    else:
        missing = [
            p.resolved_name
            for p in spec.peers
            if p.resolved_name not in peer_packaging_strategies
        ]
        if missing:
            raise RuntimeError(
                "peer_packaging_strategies is missing entries for peer(s): "
                f"{missing}. Every peer in the spec must have a resolved "
                "packaging strategy."
            )

    pool_items: List[Tuple[str, "Pool"]] = list(spec.topology.pools.items())

    representative_peer = next((p for p in spec.peers if p.leader), spec.peers[0])
    representative_task = (
        representative_peer.task.task.func
        if isinstance(representative_peer.task, BoundTask)
        else representative_peer.task
    )

    unique_strategies: List["PackagingStrategy"] = []
    seen_ids: set[int] = set()
    for peer in spec.peers:
        strategy = peer_packaging_strategies[peer.resolved_name]
        if id(strategy) in seen_ids:
            continue
        seen_ids.add(id(strategy))
        unique_strategies.append(strategy)

    shared_inputs = _build_shared_runner_inputs(
        callbacks=callbacks or [],
        pre_submission_id=pre_submission_id,
    )

    replica_items = replica_items or {}
    peer_artifacts: Dict[str, PeerArtifactBundle] = {}
    for peer in spec.peers:
        artifact = _build_peer_artifact_bundle(
            peer,
            replica_items=replica_items.get(peer.resolved_name)
            if peer.is_replica_set
            else None,
        )
        peer_artifacts[peer.resolved_name] = artifact

    peer_commands: List[Tuple["Peer", str]] = []
    default_pool_name = pool_items[0][0]
    for peer in spec.peers:
        artifact = peer_artifacts[peer.resolved_name]
        peer_pool_name = peer.pool or default_pool_name
        cmd = _emit_peer_srun_command(
            peer=peer,
            pool_name=peer_pool_name,
            base_pre_submission_id=pre_submission_id,
            args_basename=artifact.args_basename,
            kwargs_basename=artifact.kwargs_basename,
            callbacks_file=shared_inputs.callbacks_filename,
            pickled_sys_path=shared_inputs.pickled_sys_path,
            packaging_strategy=peer_packaging_strategies[peer.resolved_name],
        )
        peer_commands.append((peer, cmd))

    plan = build_plan(
        spec=spec,
        peer_commands=peer_commands,
        pool_name=default_pool_name,
        pre_submission_id=pre_submission_id,
    )

    return PreparedParallelSubmission(
        plan=plan,
        representative_task=representative_task,
        environment_exports=_build_environment_exports_map(
            target_job_dir,
            representative_task,
            packaging_strategy,
            cluster,
        ),
        python_runtime_exports=_build_local_python_runtime_exports(),
        shared_inputs=shared_inputs,
        peer_artifacts=peer_artifacts,
        peer_packaging_strategies=dict(peer_packaging_strategies),
        unique_packaging_strategies=tuple(unique_strategies),
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
    )


def render_parallel_local_prep_script(prepared: PreparedParallelSubmission) -> str:
    """Render the dedicated local-mode prep script.

    This script is intentionally narrower than the full batch script: it only
    exports the shared environment, changes into ``JOB_DIR``, runs packaging
    setup, materialises the artifact bundle, and exits. Bootstrap and the
    supervisor are launched directly from Python afterward.
    """
    lines: List[str] = ["#!/bin/bash"]
    lines.append(
        f'echo "Target Job Directory (from Python): {prepared.target_job_dir}"'
    )
    lines.extend(_emit_export_assignments(prepared.environment_exports))
    lines.extend(_emit_export_assignments(prepared.python_runtime_exports))
    lines.append("")
    lines.extend(_job_directory_setup_lines())
    lines.append("")
    for strategy in prepared.unique_packaging_strategies:
        lines.extend(
            _emit_packaging_setup(
                strategy,
                prepared.representative_task,
                prepared.pre_submission_id,
            )
        )
    lines.append("")
    lines.extend(_emit_shared_runner_input_lines(prepared.shared_inputs))
    lines.append("")
    for peer_name, artifact in prepared.peer_artifacts.items():
        lines.extend(_emit_peer_artifact_lines(peer_name, artifact))
        lines.append("")
        if artifact.replica_payloads_b64:
            lines.extend(
                _emit_replica_artifact_lines(peer_name, artifact.replica_payloads_b64)
            )
            lines.append("")
    lines.extend(_emit_plan_heredoc(prepared.plan))
    lines.append("")
    return "\n".join(line.rstrip("\r") for line in "\n".join(lines).splitlines())


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
    peer_packaging_strategies: Optional[Dict[str, "PackagingStrategy"]] = None,
    prepared_submission: Optional[PreparedParallelSubmission] = None,
) -> str:
    """Render the batch script for a ``parallel(...)`` submission.

    Emits one ``#SBATCH`` header block for the single pool and one ``srun``
    step per peer.

    Args:
        spec: Validated :class:`_ParallelSpec`. Contains exactly one pool.
        packaging_strategy: Representative packaging strategy — drives the
            batch-level sbatch directives and environment exports. When
            ``peer_packaging_strategies`` is omitted (tests, single-image
            submissions) this strategy is also used to wrap every peer's
            srun command.
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
        peer_packaging_strategies: Optional ``{peer_name: PackagingStrategy}``
            giving each peer its own strategy (Phase 10 heterogeneous
            packaging). Strategies are deduped by identity so the rendered
            script emits setup/cleanup once per unique strategy instance.
            When omitted every peer falls back to ``packaging_strategy``.

    Returns:
        The rendered bash script as a single string.
    """
    prepared = prepared_submission or prepare_parallel_submission(
        spec=spec,
        packaging_strategy=packaging_strategy,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
        cluster=cluster,
        callbacks=callbacks,
        replica_items=replica_items,
        peer_packaging_strategies=peer_packaging_strategies,
    )
    # The single pool for this allocation.
    pool_items: List[Tuple[str, "Pool"]] = list(spec.topology.pools.items())
    # We need a representative task_func for helpers that expect one (sbatch
    # directives, packaging setup, environment exports). Prefer the leader,
    # else the first peer. The surface they read (``__name__``, ``__module__``)
    # is homogeneous across peers.
    representative_task = prepared.representative_task

    # Build the single #SBATCH header block for the one pool.
    script_lines: List[str] = []
    pool_name, pool = pool_items[0]
    sbatch_params = _sbatch_params_from_pool(
        pool, spec, task_defaults, sbatch_overrides, pool_name=pool_name
    )

    # Top-level output/error point at the batch script's stdout; Slurm directs
    # the supervisor's output here.
    sbatch_params["output"] = sbatch_params.get("output") or (
        f"{target_job_dir}/slurm_{pre_submission_id}.out"
    )
    sbatch_params["error"] = sbatch_params.get("error") or (
        f"{target_job_dir}/slurm_{pre_submission_id}.err"
    )

    script_lines.extend(
        _emit_sbatch_directives(sbatch_params, representative_task, packaging_strategy)
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

    # Emit packaging setup once per unique strategy instance. Dedup by
    # Python object identity: ``_prepare_per_peer_packaging`` already
    # collapsed equivalent configs to the same instance, so object identity
    # is the right dedup key here (comparing .config dicts would also work
    # but would re-hash on every peer).
    #
    # Peers are iterated in declaration order so setup blocks appear in the
    # order strategies were first encountered — this keeps the rendered
    # script deterministic for golden-file tests.
    # Use the representative task as the token handed to each strategy;
    # packaging hooks only read ``__name__``/``__module__`` which are
    # homogeneous across peers in a single parallel() call.
    for strategy in prepared.unique_packaging_strategies:
        script_lines.extend(
            _emit_packaging_setup(strategy, representative_task, pre_submission_id)
        )
    script_lines.append("")

    # Callbacks + sys.path are shared across peers — one heredoc for each.
    script_lines.extend(_emit_shared_runner_input_lines(prepared.shared_inputs))
    script_lines.append("")

    # Per-peer arg serialisation — one heredoc pair per peer, plus one
    # per-replica heredoc for replica peers.
    for peer in spec.peers:
        artifact = prepared.peer_artifacts[peer.resolved_name]
        script_lines.extend(_emit_peer_artifact_lines(peer.resolved_name, artifact))
        script_lines.append("")
        if artifact.replica_payloads_b64:
            script_lines.extend(
                _emit_replica_artifact_lines(
                    peer.resolved_name, artifact.replica_payloads_b64
                )
            )
            script_lines.append("")

    script_lines.extend(_emit_python_path_setup())
    script_lines.append("")

    # Serialise the supervisor plan and hand control to the Python
    # bootstrap/supervisor chain.
    script_lines.extend(_emit_plan_heredoc(prepared.plan))
    script_lines.append("")
    script_lines.extend(_emit_supervisor_invocation())

    # Per-strategy cleanup — emitted after the supervisor's ``exec`` so it
    # only runs if ``exec`` fails (defensive) and, importantly, gives each
    # strategy a place to record teardown hooks that container-only
    # deployments might need. Reverse-declaration order so inter-strategy
    # dependencies (e.g. a venv strategy built on top of a container) tear
    # down safely. Currently every concrete strategy returns ``[]`` here so
    # this is a no-op in practice, but the scaffolding lets strategies opt
    # in without another rendering change.
    for strategy in reversed(prepared.unique_packaging_strategies):
        cleanup = strategy.generate_cleanup_commands(
            task=representative_task,
            job_id=pre_submission_id,
            job_dir="$JOB_DIR",
        )
        if cleanup:
            script_lines.append("")
            script_lines.extend(cleanup)

    return "\n".join(line.rstrip("\r") for line in "\n".join(script_lines).splitlines())


def _build_shared_runner_inputs(
    *,
    callbacks: List[Any],
    pre_submission_id: str,
) -> SharedRunnerInputs:
    """Serialize callbacks + sys.path shared across every peer runner."""
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

    picklable_callbacks = _filter_picklable_callbacks(callbacks)

    callbacks_payload_b64 = None
    if picklable_callbacks:
        callbacks_payload_b64 = base64.b64encode(
            dumps_pickled(picklable_callbacks)
        ).decode()

    return SharedRunnerInputs(
        pickled_sys_path=pickled_sys_path,
        callbacks_filename=callbacks_filename,
        callbacks_payload_b64=callbacks_payload_b64,
    )


def _emit_shared_runner_input_lines(shared_inputs: SharedRunnerInputs) -> List[str]:
    """Emit shell lines that materialise shared callbacks/sys.path inputs."""
    if shared_inputs.callbacks_payload_b64:
        return [
            f'base64 -d > "{shared_inputs.callbacks_filename}" << "BASE64_PARALLEL_CBS"',
            shared_inputs.callbacks_payload_b64,
            "BASE64_PARALLEL_CBS",
        ]
    return [f'touch "{shared_inputs.callbacks_filename}"']


__all__ = [
    "render_parallel_script",
    "build_plan",
    "peer_pre_submission_id",
    "PEER_ARGS_BASENAME",
    "PEER_KWARGS_BASENAME",
    "PEER_RESULT_BASENAME",
    "PEER_REPLICA_ARGS_BASENAME",
]
