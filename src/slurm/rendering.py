"""
This module provides functions for rendering Slurm job scripts from task definitions.
"""

import base64
import logging
import pickle
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple, Callable, List, Optional, TYPE_CHECKING

from ._serialization import dumps_pickled
from .task import WorkflowTask, _NamedCallable
import inspect
import sys
import traceback
from .packaging.base import PackagingStrategy
import pathlib
import shlex
import os
import importlib
from .callbacks import BaseCallback
from .validation import validate_job_name

if TYPE_CHECKING:
    from .cluster import Cluster

logger = logging.getLogger(__name__)

# Define the expected output filename for results
RESULT_FILENAME = "result.pkl"
ARGS_FILENAME = "task_args.pkl"
KWARGS_FILENAME = "task_kwargs.pkl"
CALLBACKS_FILENAME = "callbacks.pkl"


def _get_importable_module_name(func: Callable) -> str:
    """Attempts to find the importable module path for a function.

    Special handling for __main__ module: derives module name from file path
    relative to sys.path entries to enable remote imports.
    """
    module_name = func.__module__
    if module_name == "__main__":
        try:
            filepath = inspect.getfile(func)
            abs_filepath = pathlib.Path(filepath).resolve()

            # Build candidate module names from all sys.path parents; prefer dotted package paths
            candidates = []  # (score, derived_module_name)
            for p in sys.path:
                try:
                    abs_sys_path = pathlib.Path(p).resolve()
                    if not abs_filepath.is_relative_to(abs_sys_path):
                        continue
                    relative_path = abs_filepath.relative_to(abs_sys_path)
                    module_parts = list(relative_path.parts)
                    if not module_parts:
                        continue
                    if module_parts[-1].endswith(".py"):
                        module_parts[-1] = module_parts[-1][:-3]
                    if module_parts[-1] == "__init__":
                        module_parts.pop()
                    derived_module_name = ".".join(module_parts)
                    dot_count = derived_module_name.count(".")
                    depth = len(module_parts)
                    score = (dot_count, -depth)
                    candidates.append((score, derived_module_name))
                except (ValueError, OSError, TypeError):
                    continue

            if candidates:
                candidates.sort(reverse=True)
                chosen = candidates[0][1]
                logger.debug(
                    "Derived module name '%s' for function in '__main__' from path %s",
                    chosen,
                    filepath,
                )
                return chosen
            else:
                logger.warning(
                    "Could not determine module path for function in '__main__' from path %s",
                    filepath,
                )

        except (TypeError, OSError) as e:
            logger.warning(
                "Could not inspect function file path for __main__ module: %s",
                e,
            )

        return module_name
    else:
        return module_name


@dataclass
class RenderContext:
    """Structured input for :func:`render_job_script`.

    Groups the parameters needed to render an sbatch script so that
    callers pass a single object instead of 12 positional arguments.
    """

    task_func: _NamedCallable
    task_args: Tuple[Any, ...]
    task_kwargs: Dict[str, Any]
    task_definition: Dict[str, Any]
    sbatch_overrides: Dict[str, Any]
    packaging_strategy: PackagingStrategy
    target_job_dir: str
    pre_submission_id: str
    callbacks: List[BaseCallback] = field(default_factory=list)
    cluster: Optional["Cluster"] = None
    is_array_job: bool = False
    array_items_file: Optional[str] = None


def _resolve_output_paths(
    sbatch_params: Dict[str, Any],
    target_job_dir: str,
    pre_submission_id: str,
    is_array_job: bool,
) -> Tuple[str, str]:
    """Set default stdout/stderr paths in *sbatch_params* if not already set.

    Returns ``(stdout_path, stderr_path)`` for use by later rendering stages.
    """
    suffix = "_%a" if is_array_job else ""

    stdout_path = sbatch_params.get("output")
    if not stdout_path:
        stdout_path = f"{target_job_dir}/slurm_{pre_submission_id}{suffix}.out"
        sbatch_params["output"] = stdout_path

    stderr_path = sbatch_params.get("error")
    if not stderr_path:
        stderr_path = f"{target_job_dir}/slurm_{pre_submission_id}{suffix}.err"
        sbatch_params["error"] = stderr_path

    return stdout_path, stderr_path


def _emit_sbatch_directives(
    sbatch_params: Dict[str, Any],
    task_func: _NamedCallable,
    packaging_strategy: PackagingStrategy,
) -> List[str]:
    """Emit ``#SBATCH`` directive lines from *sbatch_params*."""
    lines: List[str] = ["#!/bin/bash"]

    job_name = sbatch_params.pop("job_name", None)
    if not job_name:
        job_name = task_func.__name__
    validate_job_name(job_name)
    lines.append(f"#SBATCH --job-name={shlex.quote(job_name)}")

    for key, value in sbatch_params.items():
        if key == "job_name":
            continue
        flag = key.replace("_", "-")
        value_to_emit = value
        if isinstance(value, str) and key in {"output", "error"}:
            value_to_emit = shlex.quote(value)
        if value_to_emit is None:
            lines.append(f"#SBATCH --{flag}")
        else:
            lines.append(f"#SBATCH --{flag}={value_to_emit}")

    resolved_digest = getattr(packaging_strategy, "_resolved_digest", None)
    if resolved_digest:
        lines.append(f"# Container digest: {resolved_digest}")

    return lines


def _emit_environment_exports(
    target_job_dir: str,
    task_func: _NamedCallable,
    packaging_strategy: PackagingStrategy,
    cluster: Optional["Cluster"],
) -> List[str]:
    """Emit shell export lines for JOB_DIR, Slurmfile path, packaging config, etc."""
    import json

    lines: List[str] = []
    lines.append(f'echo "Target Job Directory (from Python): {target_job_dir}"')
    lines.append(f"export JOB_DIR={shlex.quote(target_job_dir)}")
    lines.append("export SLURM_JOBS_DIR=$(dirname $(dirname $JOB_DIR))")

    if cluster is not None:
        slurmfile_path = cluster.slurmfile_path
        env_name = cluster.env_name
        is_workflow = isinstance(task_func, WorkflowTask)
        if is_workflow or slurmfile_path:
            remote_slurmfile_path = f"{target_job_dir}/Slurmfile.toml"
            lines.append(
                f"export SLURM_SDK_SLURMFILE={shlex.quote(remote_slurmfile_path)}"
            )
        if env_name:
            lines.append(f"export SLURM_SDK_ENV={shlex.quote(env_name)}")

    if packaging_strategy is not None:
        packaging_config = getattr(packaging_strategy, "config", {})
        if packaging_config:
            config_to_export = dict(packaging_config)
            config_to_export["type"] = (
                type(packaging_strategy)
                .__name__.replace("PackagingStrategy", "")
                .lower()
            )

            if hasattr(packaging_strategy, "_image_reference"):
                config_to_export["image"] = packaging_strategy._image_reference
                for key in ["dockerfile", "context", "registry", "name", "tag"]:
                    config_to_export.pop(key, None)
                config_to_export["push"] = False

            config_json = json.dumps(config_to_export)
            config_b64 = base64.b64encode(config_json.encode()).decode()
            lines.append(f"export SLURM_SDK_PACKAGING_CONFIG={shlex.quote(config_b64)}")

    if cluster is not None:
        prebuilt_images = cluster._prebuilt_dependency_images
        if prebuilt_images:
            images_json = json.dumps(prebuilt_images)
            images_b64 = base64.b64encode(images_json.encode()).decode()
            lines.append(f"export SLURM_SDK_PREBUILT_IMAGES={shlex.quote(images_b64)}")

    return lines


def _emit_packaging_setup(
    packaging_strategy: PackagingStrategy,
    task_func: _NamedCallable,
    pre_submission_id: str,
) -> List[str]:
    """Emit packaging setup commands (virtual env activation, container pull, etc.)."""
    lines: List[str] = []
    try:
        setup_commands = packaging_strategy.generate_setup_commands(
            task=task_func,
            job_id=pre_submission_id,
            job_dir="$JOB_DIR",
        )
        if setup_commands:
            lines.extend(setup_commands)
            lines.append("echo 'Packaging setup commands executed.'")
        else:
            lines.append("echo 'No packaging setup commands generated.'")
    except Exception as e:
        logger.error("Error generating packaging setup commands: %s", e)
        traceback.print_exc(file=sys.stderr)
        lines.append("echo 'ERROR: Failed to generate packaging setup commands!' >&2")
        lines.append("exit 1")
    return lines


def _serialize_runner_inputs(
    task_func: _NamedCallable,
    task_args: Tuple[Any, ...],
    task_kwargs: Dict[str, Any],
    callbacks: List[BaseCallback],
    is_array_job: bool,
    pre_submission_id: str,
) -> Tuple[List[str], str, Optional[str], Optional[str], str, str]:
    """Pickle task arguments, callbacks, and sys.path into base64 heredocs.

    Returns ``(script_lines, pickled_sys_path, args_file, kwargs_file,
    result_file, callbacks_file)``.
    """
    if is_array_job:
        args_file: Optional[str] = None
        kwargs_file: Optional[str] = None
        result_file = f"slurm_job_{pre_submission_id}_%a_{RESULT_FILENAME}"
    else:
        args_file = f"slurm_job_{pre_submission_id}_{ARGS_FILENAME}"
        kwargs_file = f"slurm_job_{pre_submission_id}_{KWARGS_FILENAME}"
        result_file = f"slurm_job_{pre_submission_id}_{RESULT_FILENAME}"

    callbacks_file = f"slurm_job_{pre_submission_id}_{CALLBACKS_FILENAME}"

    try:
        if not is_array_job:
            pickled_args = base64.b64encode(dumps_pickled(task_args)).decode()
            pickled_kwargs = base64.b64encode(dumps_pickled(task_kwargs)).decode()
        pickled_sys_path = base64.b64encode(dumps_pickled(sys.path)).decode()

        picklable_callbacks: List[BaseCallback] = []
        for cb in callbacks or []:
            needs_runner = True
            if hasattr(cb, "requires_runner_transport"):
                try:
                    needs_runner = cb.requires_runner_transport()
                except Exception as _cb_err:  # pragma: no cover - defensive
                    logger.debug(
                        "Callback %s failed requires_runner_transport check: %s",
                        type(cb).__name__,
                        _cb_err,
                    )
            if not needs_runner:
                continue
            if getattr(cb, "requires_pickling", True) is False:
                logger.debug(
                    "Skipping callback %s: requires_pickling=False",
                    type(cb).__name__,
                )
                continue
            try:
                pickle.dumps(cb)
                picklable_callbacks.append(cb)
            except Exception as _cb_err:
                logger.debug(
                    "Skipping non-picklable callback %s: %s", type(cb).__name__, _cb_err
                )

        pickled_callbacks = (
            base64.b64encode(dumps_pickled(picklable_callbacks)).decode()
            if picklable_callbacks
            else ""
        )

    except Exception as e:
        raise RuntimeError(
            f"Failed to serialize task arguments for cluster execution.\n\n"
            f"Error: {e}\n\n"
            "This usually means one or more of your task arguments cannot be pickled.\n"
            "Common non-picklable objects include:\n"
            "  - Open file handles or database connections\n"
            "  - Lambda functions or local functions\n"
            "  - Objects with __getstate__ that raises errors\n"
            "  - Thread locks or multiprocessing primitives\n\n"
            "To fix:\n"
            "  1. Pass file paths instead of open file objects\n"
            "  2. Use module-level functions instead of lambdas\n"
            "  3. Ensure all arguments are standard Python types or pickle-compatible objects"
        ) from e

    lines: List[str] = []
    if not is_array_job:
        assert args_file is not None
        assert kwargs_file is not None
        lines.append(f'base64 -d > "{args_file}" << "BASE64_ARGS"')
        lines.append(pickled_args)
        lines.append("BASE64_ARGS")
        lines.append(f'base64 -d > "{kwargs_file}" << "BASE64_KWARGS"')
        lines.append(pickled_kwargs)
        lines.append("BASE64_KWARGS")

    if pickled_callbacks:
        lines.append(f'base64 -d > "{callbacks_file}" << "BASE64_CBS"')
        lines.append(pickled_callbacks)
        lines.append("BASE64_CBS")
    else:
        lines.append(f'touch "{callbacks_file}"')

    return lines, pickled_sys_path, args_file, kwargs_file, result_file, callbacks_file


def _emit_python_path_setup() -> List[str]:
    """Emit PYTHONPATH, PY_EXEC, and PYTHONUNBUFFERED exports."""
    lines: List[str] = []
    lines.append("echo 'Executing Python task via packaged runner...' ")
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
    if pythonpath_contrib:
        lines.append(
            f"export PYTHONPATH={shlex.quote(pythonpath_contrib)}:${{PYTHONPATH:-}}"
        )

    lines.append("PY_EXEC_RESOLVED=${PY_EXEC:-python}")
    lines.append("export PY_EXEC_RESOLVED")
    lines.append("export PYTHONUNBUFFERED=1")
    return lines


def _escape_quotes(value: str) -> str:
    return value.replace('"', '\\"')


def _build_runner_command(
    *,
    module_name: str,
    func_name: str,
    pre_submission_id: str,
    target_job_dir: str,
    is_array_job: bool,
    array_items_file: Optional[str],
    args_file: Optional[str],
    kwargs_file: Optional[str],
    result_file: str,
    callbacks_file: str,
    pickled_sys_path: str,
    stdout_path: str,
    stderr_path: str,
) -> Tuple[List[str], str]:
    """Build the ``python -m slurm.runner`` command line.

    Returns ``(setup_lines, runner_command)`` where *setup_lines* are bash
    variable assignments for array jobs and *runner_command* is the final
    command string.
    """
    setup_lines: List[str] = []

    if is_array_job:
        setup_lines.append("")
        setup_lines.append("# Set up filenames for array job (expand array task ID)")
        setup_lines.append(
            f'RESULT_FILE="slurm_job_{pre_submission_id}_${{SLURM_ARRAY_TASK_ID}}_{RESULT_FILENAME}"'
        )
        if stdout_path and "%a" in stdout_path:
            expanded_stdout = stdout_path.replace("%a", "${SLURM_ARRAY_TASK_ID}")
            setup_lines.append(f'STDOUT_PATH="{expanded_stdout}"')
        if stderr_path and "%a" in stderr_path:
            expanded_stderr = stderr_path.replace("%a", "${SLURM_ARRAY_TASK_ID}")
            setup_lines.append(f'STDERR_PATH="{expanded_stderr}"')

    runner_parts = [
        '"$PY_EXEC_RESOLVED"',
        "-m slurm.runner",
        f'--module "{_escape_quotes(module_name)}"',
        f'--function "{_escape_quotes(func_name)}"',
    ]

    if is_array_job:
        assert array_items_file is not None
        runner_parts.append('--array-index "$SLURM_ARRAY_TASK_ID"')
        runner_parts.append(f'--array-items-file "{_escape_quotes(array_items_file)}"')
    else:
        assert args_file is not None
        assert kwargs_file is not None
        runner_parts.append(f'--args-file "{_escape_quotes(args_file)}"')
        runner_parts.append(f'--kwargs-file "{_escape_quotes(kwargs_file)}"')

    if is_array_job:
        runner_parts.append('--output-file "$RESULT_FILE"')
    else:
        runner_parts.append(f'--output-file "{_escape_quotes(result_file)}"')

    runner_parts.extend(
        [
            f'--callbacks-file "{_escape_quotes(callbacks_file)}"',
            f'--sys-path "{_escape_quotes(pickled_sys_path)}"',
        ]
    )

    if target_job_dir:
        runner_parts.append(f'--job-dir "{_escape_quotes(target_job_dir)}"')
    if stdout_path:
        if is_array_job and "%a" in stdout_path:
            runner_parts.append('--stdout-path "$STDOUT_PATH"')
        else:
            runner_parts.append(f'--stdout-path "{_escape_quotes(stdout_path)}"')
    if stderr_path:
        if is_array_job and "%a" in stderr_path:
            runner_parts.append('--stderr-path "$STDERR_PATH"')
        else:
            runner_parts.append(f'--stderr-path "{_escape_quotes(stderr_path)}"')
    runner_parts.append(f'--pre-submission-id "{_escape_quotes(pre_submission_id)}"')

    return setup_lines, " ".join(runner_parts)


def _emit_execution_and_cleanup(
    runner_command: str,
    packaging_strategy: PackagingStrategy,
    task_func: _NamedCallable,
    pre_submission_id: str,
    is_array_job: bool,
) -> List[str]:
    """Wrap the runner command with the packaging strategy and emit cleanup."""
    lines: List[str] = []

    try:
        wrapped_command = packaging_strategy.wrap_execution_command(
            command=runner_command,
            task=task_func,
            job_id=pre_submission_id,
            job_dir='"$JOB_DIR"',
        )
        if (
            hasattr(packaging_strategy, "_image_reference")
            and packaging_strategy._image_reference
        ):
            lines.append(
                f"echo 'Executing with container image: {packaging_strategy._image_reference}'"
            )
        lines.append(wrapped_command)
    except Exception as e:
        logger.error("Error wrapping execution command: %s", e)
        traceback.print_exc(file=sys.stderr)
        lines.append("echo 'ERROR: Failed to wrap execution command!' >&2")
        lines.append("exit 1")

    lines.append("EXECUTION_STATUS=$?")
    lines.append("")

    if not is_array_job:
        try:
            cleanup_commands = packaging_strategy.generate_cleanup_commands(
                task=task_func,
                job_id=pre_submission_id,
                job_dir="$JOB_DIR",
            )
            if cleanup_commands:
                lines.extend(cleanup_commands)
                lines.append("echo 'Packaging cleanup commands executed.'")
            else:
                lines.append("echo 'No packaging cleanup commands generated.'")
        except Exception as e:
            logger.error("Error generating packaging cleanup commands: %s", e)
            traceback.print_exc(file=sys.stderr)
            lines.append(
                "echo 'WARNING: Failed to generate packaging cleanup commands!' >&2"
            )

    lines.append("")
    lines.append('echo "Job finished with status: $EXECUTION_STATUS"')
    lines.append("exit $EXECUTION_STATUS")
    return lines


def render_job_script(ctx: RenderContext) -> str:
    """Render a SLURM sbatch script from a :class:`RenderContext`."""
    sbatch_params = dict(ctx.task_definition)
    sbatch_params.update(ctx.sbatch_overrides)

    stdout_path, stderr_path = _resolve_output_paths(
        sbatch_params, ctx.target_job_dir, ctx.pre_submission_id, ctx.is_array_job
    )

    script_lines = _emit_sbatch_directives(
        sbatch_params, ctx.task_func, ctx.packaging_strategy
    )

    script_lines.append("")
    script_lines.extend(
        _emit_environment_exports(
            ctx.target_job_dir, ctx.task_func, ctx.packaging_strategy, ctx.cluster
        )
    )

    script_lines.append("")
    script_lines.extend(_job_directory_setup_lines())
    script_lines.append("")
    script_lines.extend(
        _emit_packaging_setup(
            ctx.packaging_strategy, ctx.task_func, ctx.pre_submission_id
        )
    )
    script_lines.append("")

    (
        serialization_lines,
        pickled_sys_path,
        args_file,
        kwargs_file,
        result_file,
        callbacks_file,
    ) = _serialize_runner_inputs(
        ctx.task_func,
        ctx.task_args,
        ctx.task_kwargs,
        ctx.callbacks,
        ctx.is_array_job,
        ctx.pre_submission_id,
    )
    script_lines.extend(serialization_lines)
    script_lines.append("")

    script_lines.extend(_emit_python_path_setup())

    runner_setup, runner_command = _build_runner_command(
        module_name=_get_importable_module_name(ctx.task_func),
        func_name=ctx.task_func.__name__,
        pre_submission_id=ctx.pre_submission_id,
        target_job_dir=ctx.target_job_dir,
        is_array_job=ctx.is_array_job,
        array_items_file=ctx.array_items_file,
        args_file=args_file,
        kwargs_file=kwargs_file,
        result_file=result_file,
        callbacks_file=callbacks_file,
        pickled_sys_path=pickled_sys_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    script_lines.extend(runner_setup)

    script_lines.extend(
        _emit_execution_and_cleanup(
            runner_command,
            ctx.packaging_strategy,
            ctx.task_func,
            ctx.pre_submission_id,
            ctx.is_array_job,
        )
    )

    final_script = "\n".join(
        line.rstrip("\r") for line in "\n".join(script_lines).splitlines()
    )
    return final_script


def _job_directory_setup_lines() -> List[str]:
    """Emit the standard job directory preamble."""
    return [
        'echo "SLURM Job ID: ${SLURM_JOB_ID:-}"',
        'echo "Running on host: $(hostname)"',
        'echo "Initial working directory: $(pwd)"',
        'echo "Job output directory JOB_DIR: $JOB_DIR"',
        "",
        "# Change to job directory",
        "cd $JOB_DIR || { echo 'ERROR: Failed to cd to $JOB_DIR' >&2; exit 1; }",
        'echo "Changed to working directory: $(pwd)"',
    ]
