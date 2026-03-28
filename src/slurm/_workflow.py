"""Workflow submission and Slurmfile management extracted from ``cluster.py``.

This is an internal module.  The public API surface remains on
:class:`~slurm.cluster.Cluster`.
"""

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .callbacks import WorkflowTaskSubmitContext
from .job import Job
from .task import SlurmTask

if TYPE_CHECKING:
    from .cluster import Cluster

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SubmittableWorkflow
# ---------------------------------------------------------------------------


class SubmittableWorkflow:
    """Wrapper for workflow submissions that supports pre-building dependent task containers.

    This class is returned by `cluster.submit()` when submitting a workflow, allowing
    users to specify dependent tasks that need their containers built before the workflow runs.

    When a workflow contains child tasks that use different container configurations,
    those containers must be built before the workflow starts. This class provides
    a fluent interface via `with_dependencies()` to specify which tasks need
    pre-building.

    Attributes:
        _cluster: The Cluster instance for submission.
        _submitter: The underlying submitter callable.
        _task_func: The workflow SlurmTask being submitted.
        _packaging_config: Packaging configuration override.
        _dependent_tasks: List of tasks with containers to pre-build.
        _prebuilt_images: Dict mapping task qualified names to pre-built image references.

    Examples:
        Basic workflow submission (no dependencies):

            >>> submitter = cluster.submit(my_workflow)
            >>> job = submitter(input_data)

        Pre-build containers for child tasks:

            >>> job = cluster.submit(my_workflow).with_dependencies([
            ...     task_a,
            ...     task_b.with_options(packaging_container_tag="v2"),
            ... ])(input_data)
    """

    def __init__(
        self,
        cluster: "Cluster",
        submitter: Callable[..., Job],
        task_func: "SlurmTask",
        packaging_config: Optional[Dict[str, Any]],
        container_dependencies: Optional[List["SlurmTask"]] = None,
    ):
        self._cluster = cluster
        self._submitter = submitter
        self._task_func = task_func
        self._packaging_config = packaging_config
        self._dependent_tasks: List["SlurmTask"] = container_dependencies or []
        self._prebuilt_images: Dict[str, str] = {}

    def with_dependencies(self, tasks: List["SlurmTask"]) -> "SubmittableWorkflow":
        """Specify dependent tasks that need their containers pre-built.

        When the workflow is submitted, containers for these tasks will be built
        and pushed before the workflow starts. The workflow's child tasks can then
        use these pre-built images instead of inheriting the parent's container.

        Args:
            tasks: List of SlurmTask instances that will be submitted by the workflow.
                   These can include tasks with overrides (e.g., task.with_options(...)).

        Returns:
            Self for method chaining.

        Example:
            >>> job = cluster.submit(my_workflow).with_dependencies([
            ...     task_a,
            ...     task_b.with_options(packaging_container_tag="v2"),
            ... ])(input_data)
        """
        self._dependent_tasks = list(tasks)
        return self

    def _build_dependency_containers(self) -> Dict[str, str]:
        """Build containers for all dependent tasks.

        Returns:
            Dict mapping task function qualified names to their pre-built image references.
        """
        prebuilt_images: Dict[str, str] = {}

        logger.info(
            f"Building dependency containers for {len(self._dependent_tasks)} tasks"
        )

        for task in self._dependent_tasks:
            if not isinstance(task, SlurmTask):
                logger.warning(f"Skipping non-SlurmTask dependency: {type(task)}")
                continue

            task_packaging = getattr(task, "packaging", None)
            logger.debug(f"Task packaging config: {task_packaging}")
            if not task_packaging:
                task_packaging = {}

            effective_packaging = dict(task_packaging)

            pkg_type = effective_packaging.get("type", "auto")
            logger.debug(f"Packaging type: {pkg_type}")

            if pkg_type in ("auto", "inherit", None):
                if self._cluster.packaging_defaults:
                    effective_packaging = dict(self._cluster.packaging_defaults)
                    effective_packaging.update(task_packaging)

            if effective_packaging.get("type") != "container":
                logger.debug(
                    f"Skipping non-container packaging: {effective_packaging.get('type')}"
                )
                continue

            func = task.func
            task_name = f"{func.__module__}.{func.__qualname__}"

            logger.info(f"Building container for dependent task: {task_name}")
            logger.debug(f"Effective packaging: {effective_packaging}")

            packaging_strategy = self._cluster._prepare_packaging_strategy(
                task, effective_packaging
            )

            if packaging_strategy and hasattr(packaging_strategy, "_image_reference"):
                image_ref = packaging_strategy._image_reference
                if image_ref:
                    prebuilt_images[task_name] = image_ref
                    logger.info(f"Pre-built image for {task_name}: {image_ref}")
                else:
                    logger.warning(f"No image reference set for {task_name}")
            else:
                logger.warning("Packaging strategy has no _image_reference attribute")

        logger.info(
            f"Built {len(prebuilt_images)} pre-built images: {list(prebuilt_images.keys())}"
        )
        return prebuilt_images

    def __call__(self, *args, **kwargs) -> Job:
        """Submit the workflow, building dependency containers first if specified."""
        if self._dependent_tasks:
            self._prebuilt_images = self._build_dependency_containers()

        if self._prebuilt_images:
            if not hasattr(self._cluster, "_prebuilt_dependency_images"):
                self._cluster._prebuilt_dependency_images = {}
            self._cluster._prebuilt_dependency_images.update(self._prebuilt_images)

        return self._submitter(*args, **kwargs)


# ---------------------------------------------------------------------------
# Workflow Slurmfile rendering
# ---------------------------------------------------------------------------


def render_workflow_slurmfile(cluster: "Cluster", env_name: str) -> str:
    """Render a minimal Slurmfile for nested workflow execution.

    This is used as a fallback when the Cluster was created without a Slurmfile
    (e.g. via ``Cluster.from_args()``), but workflow jobs still need to recreate a
    Cluster instance on the runner side.
    """
    env_name = env_name or "default"
    backend_config = dict(cluster._backend_kwargs)
    job_base_dir = backend_config.pop("job_base_dir", None)
    if job_base_dir is None:
        job_base_dir = getattr(cluster.backend, "job_base_dir", None) or "~/slurm_jobs"

    lines: list[str] = []
    lines.append(f"[{env_name}.cluster]")
    lines.append(f'backend = "{cluster.backend_type}"')
    lines.append(f'job_base_dir = "{job_base_dir}"')
    lines.append("")

    if cluster.backend_type == "ssh":
        lines.append(f"[{env_name}.cluster.backend_config]")
        for key in sorted(backend_config.keys()):
            value = backend_config[key]
            if value is None:
                continue
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (int, float)):
                rendered = str(value)
            else:
                rendered = str(value).replace('"', '\\"')
                rendered = f'"{rendered}"'
            lines.append(f"{key} = {rendered}")
        lines.append("")

    from .decorators import _parse_packaging_config

    packaging_config = _parse_packaging_config(cluster.default_packaging or "auto", {})
    packaging_config = packaging_config or {}
    packaging_config.update(cluster.default_packaging_kwargs)
    if packaging_config:
        lines.append(f"[{env_name}.packaging]")
        for key, value in sorted(packaging_config.items()):
            if value is None:
                continue
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (int, float)):
                rendered = str(value)
            else:
                rendered = str(value).replace('"', '\\"')
                rendered = f'"{rendered}"'
            lines.append(f"{key} = {rendered}")
        lines.append("")

    submit_defaults: dict[str, Any] = {}
    if cluster.default_account:
        submit_defaults["account"] = cluster.default_account
    if cluster.default_partition:
        submit_defaults["partition"] = cluster.default_partition
    if submit_defaults:
        lines.append(f"[{env_name}.submit]")
        for key, value in sorted(submit_defaults.items()):
            rendered = str(value).replace('"', '\\"')
            lines.append(f'{key} = "{rendered}"')
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Workflow metadata and Slurmfile upload
# ---------------------------------------------------------------------------


def write_job_metadata(
    cluster: "Cluster",
    job_id: str,
    pre_submission_id: str,
    sanitized_task_name: str,
    timestamp: str,
    target_job_dir: str,
    task_func: SlurmTask,
) -> None:
    """Write job metadata file and emit workflow callbacks."""
    is_workflow = getattr(task_func, "_is_workflow", False)
    logger.debug(
        "[%s] is_workflow=%s (from task_func attribute)",
        pre_submission_id,
        is_workflow,
    )
    metadata = {
        "job_id": job_id,
        "pre_submission_id": pre_submission_id,
        "task_name": sanitized_task_name,
        "timestamp": timestamp,
        "submitted_at": time.time(),
        "status": "PENDING",
        "is_workflow": is_workflow,
    }

    from .context import _get_active_context
    from .workflow import WorkflowContext

    ctx = _get_active_context()
    if isinstance(ctx, WorkflowContext):
        metadata["parent_workflow"] = ctx.workflow_job_id

        logger.debug("Calling on_workflow_task_submitted callbacks...")
        try:
            from pathlib import Path as PathType

            submit_ctx = WorkflowTaskSubmitContext(
                parent_workflow_id=ctx.workflow_job_id,
                parent_workflow_dir=ctx.workflow_job_dir,
                parent_workflow_name=task_func.func.__name__
                if hasattr(task_func, "func")
                else str(ctx.workflow_job_id).split("_")[0],
                child_job_id=job_id,
                child_job_dir=PathType(target_job_dir),
                child_task_name=sanitized_task_name,
                child_is_workflow=is_workflow,
                timestamp=time.time(),
                cluster=cluster,
            )
            cluster._dispatch_callbacks("on_workflow_task_submitted_ctx", submit_ctx)
        except Exception as e:
            logger.warning(f"Error calling workflow task submitted callbacks: {e}")
    else:
        metadata["parent_workflow"] = None

    metadata_path = os.path.join(target_job_dir, "metadata.json")
    try:
        cluster.backend.write_file(metadata_path, json.dumps(metadata, indent=2))
    except Exception as exc:
        logger.warning("[%s] Failed to write metadata.json: %s", pre_submission_id, exc)


def handle_workflow_slurmfile(
    cluster: "Cluster",
    task_func: SlurmTask,
    pre_submission_id: str,
    target_job_dir: str,
) -> None:
    """Handle workflow Slurmfile upload for nested workflow execution."""
    is_workflow = getattr(task_func, "_is_workflow", False)
    logger.debug(
        "[%s] Checking workflow Slurmfile upload: is_workflow=%s",
        pre_submission_id,
        is_workflow,
    )
    if is_workflow:
        slurmfile_path = getattr(cluster, "slurmfile_path", None)
        logger.debug(
            "[%s] slurmfile_path=%s, exists=%s",
            pre_submission_id,
            slurmfile_path,
            os.path.exists(slurmfile_path) if slurmfile_path else False,
        )

        env_name = getattr(cluster, "env_name", None) or "default"
        try:
            if slurmfile_path and os.path.exists(slurmfile_path):
                with open(slurmfile_path, "r", encoding="utf-8") as f:
                    slurmfile_content = f.read()
            else:
                slurmfile_content = render_workflow_slurmfile(cluster, env_name)

            try:
                result = cluster.backend.execute_command("hostname")
                internal_hostname = result.strip() or "localhost"
            except Exception:
                internal_hostname = "localhost"

            import tomlkit

            doc = tomlkit.parse(slurmfile_content)
            env_table = doc.setdefault(env_name, tomlkit.table(is_super_table=True))
            cluster_table = env_table.setdefault(
                "cluster", tomlkit.table(is_super_table=True)
            )
            backend_cfg = cluster_table.setdefault("backend_config", tomlkit.table())
            backend_cfg["hostname"] = internal_hostname
            backend_cfg["port"] = 22
            modified_content = tomlkit.dumps(doc)

            logger.debug(
                "[%s] Prepared Slurmfile for workflow: hostname=%s, port=22",
                pre_submission_id,
                internal_hostname,
            )

            remote_slurmfile_path = os.path.join(target_job_dir, "Slurmfile.toml")
            cluster.backend.write_file(remote_slurmfile_path, modified_content)
            logger.debug(
                "[%s] Uploaded workflow Slurmfile to %s",
                pre_submission_id,
                remote_slurmfile_path,
            )
        except Exception as exc:
            logger.warning(
                "[%s] Failed to upload workflow Slurmfile: %s",
                pre_submission_id,
                exc,
            )
