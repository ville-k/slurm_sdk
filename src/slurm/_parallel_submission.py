"""Submission pipeline for ``parallel(...)`` — sibling to ``_submission.py``.

The flow mirrors the single-job path — ``setup_job_directory`` →
per-peer ``prepare_packaging_strategy`` → ``merge_sbatch_options`` → render →
``backend.submit_job`` → build per-peer :class:`Job` objects → return
:class:`ParallelJob` — so callers reading both files can map concepts
one-to-one.

Per-peer packaging (Phase 10): each peer can carry its own
``@task(packaging=...)`` declaration — different container images, wheels,
or ``"none"`` bare-node steps. Configs are deduped by canonical key so
shared images only run ``prepare()`` once; ``"inherit"`` peers are resolved
to the leader (or first peer) at submission time so rendering sees a flat
``{peer_name: strategy}`` dict.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple

from ._submission import (
    merge_sbatch_options,
    prepare_packaging_strategy,
    setup_job_directory,
)
from .callbacks import SubmitBeginContext, SubmitEndContext
from .errors import SubmissionError
from .packaging.inherit import InheritPackagingStrategy
from .parallel.rendering import (
    peer_pre_submission_id,
    render_parallel_script,
)
from .task import BoundTask

if TYPE_CHECKING:
    from .cluster import Cluster
    from .job import Job
    from .packaging.base import PackagingStrategy
    from .parallel.types import Peer, _ParallelSpec
    from .parallel_job import ParallelJob

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-peer packaging preparation (Phase 10)
# ---------------------------------------------------------------------------


def _packaging_config_key(config: Dict[str, Any] | None) -> str:
    """Return a stable canonical key for deduping prepared packaging strategies.

    Two peers whose ``@task(packaging=...)`` declarations resolve to the
    same key share one :class:`PackagingStrategy` instance — so a container
    image is pulled / pushed / probed exactly once per unique resolved
    reference, even when three peers reference it.

    Volatile fields are excluded: an auto-generated ``tag`` from
    :class:`ContainerPackagingStrategy` (``build-<uuid>``) should NOT make
    two otherwise-identical configs look different. The caller is expected
    to have already resolved the config so deterministic keys
    (image / dockerfile / context / digest) stand in for the whole thing.
    """
    if not config:
        return "none:"

    ctype = str(config.get("type") or "none")
    # Volatile fields that do not affect what the strategy will actually run.
    volatile = {"tag"}
    canonical = sorted(
        (k, str(v)) for k, v in config.items() if k not in volatile and v is not None
    )
    return f"{ctype}:" + "|".join(f"{k}={v}" for k, v in canonical)


def _peer_packaging_config(peer: "Peer") -> Dict[str, Any] | None:
    """Pull the effective packaging dict off a peer's underlying task."""
    if isinstance(peer.task, BoundTask):
        return peer.task.task.packaging or None
    return None


def _resolve_inherit_source_peer(spec: "_ParallelSpec") -> "Peer":
    """Return the peer whose packaging ``"inherit"`` peers clone.

    Rule: the leader if one exists, otherwise the first peer in declaration
    order. Documented in CHANGELOG and the design doc's §7.
    """
    for peer in spec.peers:
        if peer.leader:
            return peer
    return spec.peers[0]


def _prepare_per_peer_packaging(
    cluster: "Cluster",
    spec: "_ParallelSpec",
) -> Dict[str, "PackagingStrategy"]:
    """Prepare one :class:`PackagingStrategy` per unique peer config.

    Returns a ``{peer_name: strategy}`` dict. Peers with equivalent
    packaging configs share the same strategy instance (so containers pull
    once); peers declaring ``packaging="inherit"`` are resolved to the
    leader (or first peer) *before* :meth:`prepare` runs — inheritance is a
    submission-time concept only.
    """
    # First pass: decide which config each peer resolves to. Inherit peers
    # get redirected to the inheritance source's config.
    inherit_source = _resolve_inherit_source_peer(spec)
    inherit_source_config = _peer_packaging_config(inherit_source)

    resolved_configs: Dict[str, Dict[str, Any] | None] = {}
    for peer in spec.peers:
        cfg = _peer_packaging_config(peer)
        if cfg and cfg.get("type") == "inherit":
            if peer.resolved_name == inherit_source.resolved_name:
                # Validation should have caught this, but belt-and-braces:
                # the inheritance source cannot itself inherit.
                raise SubmissionError(
                    f'Peer {peer.resolved_name!r} uses packaging="inherit" '
                    "but is the inheritance source (leader or first peer). "
                    "Point inheriting peers at a concrete packaging target."
                )
            resolved_configs[peer.resolved_name] = inherit_source_config
        else:
            resolved_configs[peer.resolved_name] = cfg

    # Second pass: dedupe by canonical key, preparing once per unique config.
    # We preserve first-encounter order so the rendered script emits setup
    # blocks deterministically.
    strategies_by_key: Dict[str, "PackagingStrategy"] = {}
    peer_to_strategy: Dict[str, "PackagingStrategy"] = {}
    for peer in spec.peers:
        cfg = resolved_configs[peer.resolved_name]
        key = _packaging_config_key(cfg)
        if key not in strategies_by_key:
            bound = peer.task if isinstance(peer.task, BoundTask) else None
            representative_task = bound.task if bound is not None else peer.task
            strategy = prepare_packaging_strategy(
                cluster, representative_task, packaging_config=cfg
            )
            if isinstance(strategy, InheritPackagingStrategy):
                # Defensive: if inherit resolution above failed to redirect
                # (e.g. an unexpected code path), fail loudly rather than
                # let an inherit strategy leak into the rendered script
                # with no parent metadata.
                raise SubmissionError(
                    f"Peer {peer.resolved_name!r} resolved to an "
                    "InheritPackagingStrategy at submission time. Parallel "
                    'peers using packaging="inherit" are rewritten to the '
                    "leader/first peer's config before prepare() — this "
                    "path should never be hit."
                )
            strategies_by_key[key] = strategy
        peer_to_strategy[peer.resolved_name] = strategies_by_key[key]

    return peer_to_strategy


def _leader_or_first(spec: "_ParallelSpec") -> "Peer":
    """Return the peer whose @task defaults drive the shared sbatch header.

    Rationale: the shared allocation needs *one* representative for things
    like ``job_name`` and packaging. The leader is the natural choice; when
    there is no leader (symmetric peers) the first peer wins to keep the
    choice deterministic.
    """
    for peer in spec.peers:
        if peer.leader:
            return peer
    return spec.peers[0]


def _representative_task_and_defaults(
    spec: "_ParallelSpec",
) -> Tuple[Any, Dict[str, Any], Any]:
    """Return ``(slurm_task, task_defaults, task_func)`` for the representative peer."""
    peer = _leader_or_first(spec)
    if not isinstance(peer.task, BoundTask):
        raise RuntimeError(
            f"Peer {peer.resolved_name!r} did not normalise to a BoundTask. "
            "This is an internal error in the parallel spec pipeline."
        )
    slurm_task = peer.task.task
    task_defaults = dict(slurm_task.sbatch_options or {})
    return slurm_task, task_defaults, slurm_task.func


def _resolve_replica_items(peer: "Peer") -> List[Any]:
    """Compute the per-replica payload list for a replica peer.

    Each entry is a dict / tuple / scalar consumed by
    :func:`slurm.array_items.unpack_item_to_args_kwargs` — matching the
    :meth:`SlurmTask.map` item shape. Callables are evaluated eagerly here
    so the supervisor and runner never have to carry user-defined closures
    across the submission boundary.

    Args:
        peer: A replica peer (``peer.is_replica_set is True``).

    Returns:
        A list of length ``peer.count``. For ``args=None`` every entry is
        an empty dict — each replica inherits the shared ``BoundTask``
        binding via the bare ``peer_<name>_args.pkl`` fallback path.
    """
    import collections.abc as _abc

    if not peer.is_replica_set:
        raise RuntimeError(
            f"_resolve_replica_items called on singleton peer "
            f"{peer.resolved_name!r}. Internal error."
        )

    count = peer.count
    spec = peer.args

    # ``args=None`` — each replica receives an empty dict. The runner's
    # replica loader unpacks it as kwargs (no extras) and the shared
    # ``BoundTask`` binding provides the real args.
    if spec is None:
        return [{} for _ in range(count)]

    # ``args=range(...)`` — each replica gets its index as the first
    # positional arg. Length validation happened at spec-build time.
    if isinstance(spec, range):
        return [list(spec)[i] for i in range(count)]

    # ``args=list/tuple`` — one entry per replica. Checked before the
    # callable branch so list/tuple sequences (which are not callable) get
    # the index-lookup fast path without type narrowing headaches.
    if isinstance(spec, (list, tuple)) or isinstance(spec, _abc.Sequence):
        seq = list(spec)
        if len(seq) != count:
            # Defensive: validation should have caught this.
            raise RuntimeError(
                f"Replica peer {peer.resolved_name!r}: args length "
                f"{len(seq)} != count {count}"
            )
        return seq

    # ``args=callable`` — evaluate eagerly at submission time. The return
    # value follows the same dict/tuple/scalar rules as list items.
    if callable(spec):
        fn: Callable[[int], Any] = spec
        return [fn(i) for i in range(count)]

    raise TypeError(
        f"Peer.replicas args must be None, a sequence, a range, or a callable; "
        f"got {type(spec).__name__}"
    )


def _per_peer_stdout_stderr(
    target_job_dir: str, pre_submission_id: str, peer_name: str
) -> Tuple[str, str]:
    """Per-peer output paths used for the :class:`Job`'s stdout/stderr pointers.

    Slurm writes one stdout / stderr for the *batch* step, but each ``srun``
    step also emits its own interleaved lines. For Phase 2 we point each
    per-peer :class:`Job` at the shared batch output; per-step capture lands
    with the Python supervisor in Phase 3.
    """
    stdout = f"{target_job_dir}/slurm_{pre_submission_id}.out"
    stderr = f"{target_job_dir}/slurm_{pre_submission_id}.err"
    del peer_name  # reserved for Phase 3 per-step capture
    return stdout, stderr


def submit_parallel_spec(
    cluster: "Cluster",
    spec: "_ParallelSpec",
) -> "ParallelJob":
    """Submit a validated :class:`_ParallelSpec` through ``cluster``.

    Returns a :class:`ParallelJob` aggregating one :class:`Job` per peer.
    """
    from .job import Job
    from .parallel_job import ParallelJob

    slurm_task, task_defaults, task_func = _representative_task_and_defaults(spec)

    pre_submission_id, _, target_job_dir, _ = setup_job_directory(
        cluster, slurm_task, task_defaults
    )

    # Per-peer packaging — one prepare() call per unique config. Peers
    # sharing an image share one strategy instance; ``packaging="inherit"``
    # is resolved to the leader/first peer *before* prepare() runs.
    peer_packaging_strategies = _prepare_per_peer_packaging(cluster, spec)
    # Representative strategy for callbacks that expect a single value. The
    # leader / first-peer strategy is the natural pick.
    representative_peer = _leader_or_first(spec)
    packaging_strategy = peer_packaging_strategies[representative_peer.resolved_name]

    # Build the ``#SBATCH`` header using the same precedence as the single-job
    # path — cluster defaults → slurmfile submit defaults → task defaults →
    # runtime overrides. The pool-shape overlay happens inside
    # ``render_parallel_script`` so the two renderers agree on naming.
    effective_sbatch_options, stdout_path, stderr_path = merge_sbatch_options(
        cluster, task_defaults, {}, pre_submission_id, target_job_dir
    )
    # stdout/stderr are shared at the batch level in Phase 2; per-peer
    # plumbing lives in Phase 3. Silence unused-locals lints.
    del stdout_path, stderr_path

    submit_begin_ctx = SubmitBeginContext(
        task=slurm_task,
        sbatch_options=dict(effective_sbatch_options),
        pre_submission_id=pre_submission_id,
        target_job_dir=target_job_dir,
        cluster=cluster,
        packaging_strategy=packaging_strategy,
        backend_type=cluster.backend_type,
    )
    cluster._dispatch_callbacks("on_begin_submit_job_ctx", submit_begin_ctx)

    # Pre-resolve replica payloads (including any callable args=) so the
    # rendered script can materialise per-index pickles via heredocs.
    replica_items: Dict[str, List[Any]] = {}
    for peer in spec.peers:
        if peer.is_replica_set:
            replica_items[peer.resolved_name] = _resolve_replica_items(peer)

    script = render_parallel_script(
        spec=spec,
        packaging_strategy=packaging_strategy,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
        cluster=cluster,
        task_defaults=effective_sbatch_options,
        sbatch_overrides={},
        callbacks=cluster.callbacks,
        replica_items=replica_items,
        peer_packaging_strategies=peer_packaging_strategies,
    )
    logger.debug(
        "[%s] --- RENDERED PARALLEL SCRIPT ---\n%s\n[%s] --- END RENDERED SCRIPT ---",
        pre_submission_id,
        script,
        pre_submission_id,
    )

    submit_account = effective_sbatch_options.get("account")
    submit_partition = effective_sbatch_options.get("partition")
    try:
        job_submission_result = cluster.backend.submit_job(
            script,
            target_job_dir=target_job_dir,
            pre_submission_id=pre_submission_id,
            account=submit_account,
            partition=submit_partition,
        )
    except Exception as exc:
        raise SubmissionError(
            f"Failed to submit parallel job via backend "
            f"{cluster.backend_type!r}: {exc}",
            script=script,
            metadata={
                "backend": cluster.backend_type,
                "account": submit_account,
                "partition": submit_partition,
                "pre_submission_id": pre_submission_id,
                "target_job_dir": target_job_dir,
                "peers": [p.resolved_name for p in spec.peers],
            },
        ) from exc

    if isinstance(job_submission_result, str):
        base_job_id = job_submission_result
    elif isinstance(job_submission_result, tuple) and len(job_submission_result) == 2:
        base_job_id, _ = job_submission_result
    else:
        raise TypeError(
            f"Unexpected return type from backend.submit_job: "
            f"{type(job_submission_result)!r}"
        )

    # Build per-peer (and per-replica) Job objects. Every peer shares the
    # base Slurm job id — the allocation is one sbatch — but each peer /
    # replica points at its own result file. Replica peers produce
    # ``count`` Job objects, each with a distinct ``pre_submission_id`` so
    # ``Job._result_filename`` yields distinct result paths.
    peer_jobs: Dict[str, "Job"] = {}
    peer_replica_jobs: Dict[str, List["Job"]] = {}
    for peer in spec.peers:
        if not isinstance(peer.task, BoundTask):
            raise RuntimeError(
                f"Peer {peer.resolved_name!r} did not normalise to a BoundTask. "
                "Internal error."
            )
        peer_slurm_task = peer.task.task

        if peer.is_replica_set:
            replica_jobs: List["Job"] = []
            items = replica_items.get(peer.resolved_name, [])
            for replica_idx in range(peer.count):
                replica_pre_id = (
                    f"{peer_pre_submission_id(pre_submission_id, peer.resolved_name)}"
                    f"_{replica_idx}"
                )
                item = items[replica_idx] if replica_idx < len(items) else {}
                # Compose the effective args/kwargs for this replica by
                # layering the replica-specific item over the shared
                # BoundTask binding — the runner does the same under the
                # hood via ``unpack_item_to_args_kwargs`` + the shared
                # pickle fallback.
                base_args = tuple(peer.task.args)
                base_kwargs = dict(peer.task.kwargs)
                replica_args: tuple
                replica_kwargs: dict
                if isinstance(item, dict):
                    replica_args = base_args
                    replica_kwargs = {**base_kwargs, **item}
                elif isinstance(item, tuple):
                    replica_args = base_args + item
                    replica_kwargs = base_kwargs
                else:
                    replica_args = base_args + (item,)
                    replica_kwargs = base_kwargs
                peer_stdout = f"{target_job_dir}/slurm_{pre_submission_id}.out"
                peer_stderr = f"{target_job_dir}/slurm_{pre_submission_id}.err"
                job = Job(
                    id=base_job_id,
                    cluster=cluster,
                    task_func=peer_slurm_task,
                    args=replica_args,
                    kwargs=replica_kwargs,
                    target_job_dir=target_job_dir,
                    pre_submission_id=replica_pre_id,
                    sbatch_options=dict(effective_sbatch_options),
                    stdout_path=peer_stdout,
                    stderr_path=peer_stderr,
                    backend=cluster.backend,
                    on_completed=cluster._emit_completed_context,
                )
                replica_jobs.append(job)
            peer_replica_jobs[peer.resolved_name] = replica_jobs
            # The leader / representative Job for the peer's aggregate
            # surface is replica 0 — used anywhere the legacy
            # ``peer_jobs[name]`` path returned a singleton.
            peer_jobs[peer.resolved_name] = replica_jobs[0]
        else:
            peer_pre_id = peer_pre_submission_id(pre_submission_id, peer.resolved_name)
            peer_args = tuple(peer.task.args)
            peer_kwargs = dict(peer.task.kwargs)
            peer_stdout, peer_stderr = _per_peer_stdout_stderr(
                target_job_dir, pre_submission_id, peer.resolved_name
            )
            job = Job(
                id=base_job_id,
                cluster=cluster,
                task_func=peer_slurm_task,
                args=peer_args,
                kwargs=peer_kwargs,
                target_job_dir=target_job_dir,
                pre_submission_id=peer_pre_id,
                sbatch_options=dict(effective_sbatch_options),
                stdout_path=peer_stdout,
                stderr_path=peer_stderr,
                backend=cluster.backend,
                on_completed=cluster._emit_completed_context,
            )
            peer_jobs[peer.resolved_name] = job

    parallel_job = ParallelJob(
        cluster=cluster,
        job_id=base_job_id,
        peer_jobs=peer_jobs,
        spec=spec,
        target_job_dir=target_job_dir,
        peer_replica_jobs=peer_replica_jobs,
    )

    # Emit end-of-submit callback using the representative peer's Job so
    # existing callback consumers (loggers, benchmarks) continue to work.
    # Per-peer callback fanout is a Phase 11 concern.
    representative_peer = _leader_or_first(spec)
    submit_end_ctx = SubmitEndContext(
        job=peer_jobs[representative_peer.resolved_name],
        job_id=str(base_job_id),
        pre_submission_id=pre_submission_id,
        target_job_dir=target_job_dir,
        sbatch_options=dict(effective_sbatch_options),
        cluster=cluster,
        backend_type=cluster.backend_type,
    )
    cluster._dispatch_callbacks("on_end_submit_job_ctx", submit_end_ctx)

    # Start the poller for each Job so existing status-polling
    # infrastructure fires ``on_completed_ctx`` once per peer/replica. They
    # share a job id so the backend query is redundant on multi-peer jobs —
    # we accept that cost in Phase 2 and revisit in Phase 11.
    polled_jobs: List["Job"] = []
    for peer in spec.peers:
        if peer.is_replica_set:
            polled_jobs.extend(peer_replica_jobs[peer.resolved_name])
        else:
            polled_jobs.append(peer_jobs[peer.resolved_name])
    for job in polled_jobs:
        cluster._maybe_start_job_poller(job)

    return parallel_job


__all__ = ["submit_parallel_spec"]


# Silence unused-import lints for types that appear only in annotations when
# TYPE_CHECKING is False.
_unused: Tuple[Any, ...] = (List,)
