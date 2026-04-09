"""Job submission helpers extracted from ``cluster.py``.

This is an internal module.  The public API surface remains on
:class:`~slurm.cluster.Cluster`.
"""

import logging
import os
import re
import sys
import time
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from .callbacks import (
    PackagingBeginContext,
    PackagingEndContext,
    SubmitBeginContext,
    SubmitEndContext,
)
from .errors import PackagingError, SubmissionError
from ._packaging_resolver import resolve_packaging_config  # noqa: F401
from .job import Job
from .packaging import get_packaging_strategy
from .task import SlurmTask, _NamedCallable, normalize_sbatch_options
from .validation import validate_account, validate_partition

if TYPE_CHECKING:
    from .cluster import Cluster

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _generate_timestamp_id() -> Tuple[str, str]:
    """Generate timestamp and unique ID for hierarchical directory structure.

    Returns:
        tuple: (timestamp, unique_id) where timestamp is YYYYMMDD_HHMMSS format
            and unique_id is an 8-character hex string.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return timestamp, unique_id


def _sanitize_task_name(name: str) -> str:
    """Sanitize task name for filesystem use.

    Args:
        name: Raw task name.

    Returns:
        Sanitized task name safe for filesystem paths.

    Examples:
        >>> _sanitize_task_name("Train Model")
        'train_model'
        >>> _sanitize_task_name("model:v2")
        'model_v2'
    """
    name = name.lower()
    name = re.sub(r"[^\w\-]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name or "task"


# ---------------------------------------------------------------------------
# Submission pipeline functions
# ---------------------------------------------------------------------------


def prepare_packaging_strategy(
    cluster: "Cluster",
    task_func: SlurmTask,
    packaging_config: Optional[Dict[str, Any]],
) -> Any:
    """Prepare the packaging strategy for a task submission.

    Resolves the effective packaging config via :func:`resolve_packaging_config`,
    then instantiates, prepares, and returns the strategy object.
    """
    effective_packaging_config = resolve_packaging_config(
        cluster, task_func, explicit_config=packaging_config
    )

    logger.debug("Effective packaging config: %s", effective_packaging_config)

    packaging_start = time.time()
    begin_ctx = PackagingBeginContext(
        task=task_func,
        packaging_config=effective_packaging_config,
        cluster=cluster,
        timestamp=packaging_start,
    )

    cluster._dispatch_callbacks("on_begin_package_ctx", begin_ctx)

    strategy = get_packaging_strategy(effective_packaging_config)
    logger.debug("[%s] Using packaging strategy: %s", "n/a", type(strategy).__name__)
    try:
        result = strategy.prepare(task=task_func, cluster=cluster)
        logger.debug("[%s] Packaging prepared: %s", "n/a", result)
    except Exception as exc:
        logger.error("[%s] Packaging preparation failed: %s", "n/a", exc)
        traceback.print_exc(file=sys.stderr)
        pkg_type = (
            effective_packaging_config.get("type")
            if effective_packaging_config
            else "none"
        )
        raise PackagingError(
            f"Packaging preparation failed for task '{getattr(task_func, '__name__', 'unknown')}'\n"
            f"Packaging type: {pkg_type}\n"
            f"Original error: {exc}"
        ) from exc

    packaging_end = time.time()
    end_ctx = PackagingEndContext(
        task=task_func,
        packaging_result=strategy,
        cluster=cluster,
        timestamp=packaging_end,
        duration=packaging_end - packaging_start,
    )

    cluster._dispatch_callbacks("on_end_package_ctx", end_ctx)

    return strategy


def setup_job_directory(
    cluster: "Cluster", task_func: SlurmTask, task_defaults: Dict[str, Any]
) -> Tuple[str, str, str, str]:
    """Setup job directory structure and return identifiers.

    Returns:
        Tuple of (pre_submission_id, sanitized_task_name, target_job_dir, timestamp)
    """
    timestamp, unique_id = _generate_timestamp_id()
    pre_submission_id = f"{timestamp}_{unique_id}"

    task_name = task_defaults.get("job_name", task_func.func.__name__)
    sanitized_name = _sanitize_task_name(task_name)

    resolved_job_base_dir = getattr(cluster.backend, "job_base_dir", None)
    if resolved_job_base_dir is None:
        import tempfile

        resolved_job_base_dir = os.path.join(tempfile.gettempdir(), "slurm_jobs")
        try:
            os.makedirs(resolved_job_base_dir, exist_ok=True)
        except (OSError, IOError) as e:
            logger.debug(f"Failed to create job base directory: {e}")

    from .context import _get_active_context
    from .workflow import WorkflowContext

    ctx = _get_active_context()
    if isinstance(ctx, WorkflowContext):
        target_job_dir = os.path.join(
            str(ctx.workflow_job_dir),
            "tasks",
            sanitized_name,
            f"{timestamp}_{unique_id}",
        )
    else:
        target_job_dir = os.path.join(
            resolved_job_base_dir,
            sanitized_name,
            f"{timestamp}_{unique_id}",
        )
    logger.debug(
        "[%s] Target job directory path: %s", pre_submission_id, target_job_dir
    )

    return pre_submission_id, sanitized_name, target_job_dir, timestamp


def merge_sbatch_options(
    cluster: "Cluster",
    task_defaults: Dict[str, Any],
    submit_overrides: Dict[str, Any],
    pre_submission_id: str,
    target_job_dir: str,
) -> Tuple[Dict[str, Any], str, str]:
    """Merge SBATCH options with proper precedence.

    Precedence order (lowest to highest):
    1. Cluster defaults (default_account, default_partition)
    2. Slurmfile [submit] defaults
    3. Task decorator defaults
    4. Runtime overrides

    Returns:
        Tuple of (effective_sbatch_options, stdout_path, stderr_path)
    """
    effective_sbatch_options: Dict[str, Any] = {}

    if cluster.default_account:
        effective_sbatch_options["account"] = cluster.default_account
    if cluster.default_partition:
        effective_sbatch_options["partition"] = cluster.default_partition

    slurmfile_submit_defaults = getattr(cluster, "submit_defaults", None)
    if slurmfile_submit_defaults and isinstance(slurmfile_submit_defaults, dict):
        effective_sbatch_options.update(
            normalize_sbatch_options(slurmfile_submit_defaults)
        )

    effective_sbatch_options.update(task_defaults)
    effective_sbatch_options.update(submit_overrides)

    validate_account(effective_sbatch_options.get("account"))
    validate_partition(effective_sbatch_options.get("partition"))

    stdout_path = effective_sbatch_options.get("output")
    if not stdout_path:
        stdout_path = f"{target_job_dir}/slurm_{pre_submission_id}.out"

    stderr_path = effective_sbatch_options.get("error")
    if not stderr_path:
        stderr_path = f"{target_job_dir}/slurm_{pre_submission_id}.err"

    return effective_sbatch_options, stdout_path, stderr_path


def render_and_submit_to_backend(
    cluster: "Cluster",
    task_func: SlurmTask,
    func_to_render: "_NamedCallable",
    args: tuple,
    kwargs: dict,
    task_defaults: Dict[str, Any],
    submit_overrides: Dict[str, Any],
    packaging_strategy: Any,
    pre_submission_id: str,
    target_job_dir: str,
    effective_sbatch_options: Dict[str, Any],
) -> str:
    """Render job script and submit to backend.

    Returns:
        The job ID as a string.
    """
    from .rendering import RenderContext, render_job_script

    submit_begin_ctx = SubmitBeginContext(
        task=task_func,
        sbatch_options=dict(effective_sbatch_options),
        pre_submission_id=pre_submission_id,
        target_job_dir=target_job_dir,
        cluster=cluster,
        packaging_strategy=packaging_strategy,
        backend_type=cluster.backend_type,
    )

    cluster._dispatch_callbacks("on_begin_submit_job_ctx", submit_begin_ctx)

    script = render_job_script(
        RenderContext(
            task_func=func_to_render,
            task_args=args,
            task_kwargs=kwargs,
            task_definition=task_defaults,
            sbatch_overrides=dict(submit_overrides),
            packaging_strategy=packaging_strategy,
            target_job_dir=target_job_dir,
            pre_submission_id=pre_submission_id,
            callbacks=cluster.callbacks,
            cluster=cluster,
        )
    )
    logger.debug(
        "[%s] --- RENDERED SBATCH SCRIPT ---\n%s\n[%s] --- END RENDERED SCRIPT ---",
        pre_submission_id,
        script,
        pre_submission_id,
    )

    submit_account = effective_sbatch_options.get("account")
    submit_partition = effective_sbatch_options.get("partition")
    logger.debug("[%s] submit_account: %s", pre_submission_id, submit_account)
    logger.debug("[%s] submit_partition: %s", pre_submission_id, submit_partition)
    submission_message = (
        f"[{pre_submission_id}] Submitting job via {cluster.backend_type} backend"
    )
    try:
        logger.debug(submission_message)
        job_submission_result = cluster.backend.submit_job(
            script,
            target_job_dir=target_job_dir,
            pre_submission_id=pre_submission_id,
            account=submit_account,
            partition=submit_partition,
        )
    except Exception as e:
        raise SubmissionError(
            f"Failed to submit job via backend '{cluster.backend_type}': {e}",
            script=script,
            metadata={
                "backend": cluster.backend_type,
                "account": submit_account,
                "partition": submit_partition,
                "pre_submission_id": pre_submission_id,
                "target_job_dir": target_job_dir,
            },
        ) from e

    if isinstance(job_submission_result, str):
        job_id = job_submission_result
    elif isinstance(job_submission_result, tuple) and len(job_submission_result) == 2:
        job_id, _ = job_submission_result
    else:
        raise TypeError(
            f"Unexpected return type from backend.submit_job: {type(job_submission_result)}"
        )

    return job_id


def create_job_object(
    cluster: "Cluster",
    job_id: str,
    task_func: SlurmTask,
    args: tuple,
    kwargs: dict,
    target_job_dir: str,
    pre_submission_id: str,
    effective_sbatch_options: Dict[str, Any],
    stdout_path: str,
    stderr_path: str,
) -> Job:
    """Create a Job object from submission results."""
    return Job(
        id=job_id,
        cluster=cluster,
        task_func=task_func,
        args=args,
        kwargs=kwargs,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
        sbatch_options=dict(effective_sbatch_options),
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        backend=cluster.backend,
        on_completed=cluster._emit_completed_context,
    )


def finalize_job_submission(
    cluster: "Cluster",
    job: Job,
    job_id: str,
    pre_submission_id: str,
    target_job_dir: str,
    effective_sbatch_options: Dict[str, Any],
) -> Job:
    """Finalize job submission with callbacks and polling.

    Returns:
        The Job object.
    """
    submit_end_ctx = SubmitEndContext(
        job=job,
        job_id=str(job_id),
        pre_submission_id=pre_submission_id,
        target_job_dir=target_job_dir,
        sbatch_options=dict(effective_sbatch_options),
        cluster=cluster,
        backend_type=cluster.backend_type,
    )

    cluster._dispatch_callbacks("on_end_submit_job_ctx", submit_end_ctx)
    cluster._maybe_start_job_poller(job)

    return job
