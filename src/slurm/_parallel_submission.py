"""Submission pipeline for ``parallel(...)`` — sibling to ``_submission.py``.

Phase 2 scope: single implicit pool (or an explicit one-pool topology), one
shared packaging config, inline shell supervision. The flow mirrors the
single-job path — ``setup_job_directory`` → ``prepare_packaging_strategy`` →
``merge_sbatch_options`` → render → ``backend.submit_job`` → build per-peer
:class:`Job` objects → return :class:`ParallelJob` — so callers reading both
files can map concepts one-to-one.

Scope-boundary errors (``NotImplementedError`` for replicas, multi-pool,
per-peer packaging, and advanced failure policies) are raised here rather
than in :mod:`slurm.parallel` so the entry point stays small and the
validator/rendering layers don't have to know about phasing.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from ._submission import (
    merge_sbatch_options,
    prepare_packaging_strategy,
    setup_job_directory,
)
from .callbacks import SubmitBeginContext, SubmitEndContext
from .errors import SubmissionError
from .parallel.rendering import (
    peer_pre_submission_id,
    render_parallel_script,
)
from .task import BoundTask

if TYPE_CHECKING:
    from .cluster import Cluster
    from .job import Job
    from .parallel.types import Peer, _ParallelSpec
    from .parallel_job import ParallelJob

logger = logging.getLogger(__name__)


def _check_phase2_scope(spec: "_ParallelSpec") -> None:
    """Reject submissions that need functionality deferred to later phases.

    Each branch points at the phase that delivers the missing piece, so the
    message is actionable rather than confusing.
    """
    if len(spec.topology.pools) > 1:
        names = ", ".join(spec.topology.pools.keys())
        raise NotImplementedError(
            f"parallel(...) with multiple pools is not supported yet "
            f"(got pools: {names}). Multi-pool hetjob support ships in Phase 6."
        )

    for peer in spec.peers:
        if peer.count > 1:
            raise NotImplementedError(
                f"Peer.replicas(count={peer.count}) is not supported yet. "
                "Replica sets ship in Phase 5."
            )
        if peer.on_node is not None or peer.colocate_with is not None:
            raise NotImplementedError(
                "Peer placement directives (on_node, colocate_with) are not "
                "supported yet. Named-node placement ships in Phase 8."
            )

    # All peers share one packaging config in Phase 2 — catch the common
    # footgun of passing a mix of tasks with different ``packaging=`` set.
    packaging_signatures = set()
    for peer in spec.peers:
        if isinstance(peer.task, BoundTask):
            pkg = peer.task.task.packaging or {}
        else:
            pkg = {}
        # Make comparable: sort keys and freeze into a tuple of (k, str(v)).
        sig = tuple(sorted((k, str(v)) for k, v in pkg.items()))
        packaging_signatures.add(sig)
    if len(packaging_signatures) > 1:
        raise NotImplementedError(
            "Per-peer packaging is not supported yet — every peer in a "
            "parallel(...) call must share the same @task(packaging=...) "
            "config. Heterogeneous packaging ships in Phase 10."
        )


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

    _check_phase2_scope(spec)

    slurm_task, task_defaults, task_func = _representative_task_and_defaults(spec)

    pre_submission_id, _, target_job_dir, _ = setup_job_directory(
        cluster, slurm_task, task_defaults
    )

    # Shared packaging (single-packaging assertion happens in
    # _check_phase2_scope above). resolve_packaging_config picks up the
    # task's packaging config by default.
    packaging_strategy = prepare_packaging_strategy(
        cluster, slurm_task, packaging_config=None
    )

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

    script = render_parallel_script(
        spec=spec,
        packaging_strategy=packaging_strategy,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
        cluster=cluster,
        task_defaults=effective_sbatch_options,
        sbatch_overrides={},
        callbacks=cluster.callbacks,
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

    # Build per-peer Job objects. Every peer shares the base Slurm job id —
    # the allocation is one sbatch — but each peer points at its own result
    # file. We derive each peer's ``pre_submission_id`` with the helper so
    # ``Job._result_filename`` lands on ``slurm_job_<base>_peer_<name>_result.pkl``.
    peer_jobs: Dict[str, "Job"] = {}
    for peer in spec.peers:
        peer_pre_id = peer_pre_submission_id(pre_submission_id, peer.resolved_name)
        if not isinstance(peer.task, BoundTask):
            raise RuntimeError(
                f"Peer {peer.resolved_name!r} did not normalise to a BoundTask. "
                "Internal error."
            )
        peer_slurm_task = peer.task.task
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

    # Start the poller for each per-peer Job so existing status-polling
    # infrastructure fires ``on_completed_ctx`` once per peer. They share a
    # job id so the backend query is redundant on multi-peer jobs — we
    # accept that cost in Phase 2 and revisit in Phase 11.
    for job in peer_jobs.values():
        cluster._maybe_start_job_poller(job)

    return parallel_job


__all__ = ["submit_parallel_spec"]


# Silence unused-import lints for types that appear only in annotations when
# TYPE_CHECKING is False.
_unused: Tuple[Any, ...] = (List,)
