"""
This module provides functions for rendering Slurm job scripts from task definitions.
"""

import logging
import pickle
import base64
from typing import Any, Dict, Tuple, Callable, List, Optional, TYPE_CHECKING
import inspect
import sys
import traceback
from .packaging.base import PackagingStrategy
from .task import normalize_sbatch_options
import pathlib
import shlex
import os
import importlib
from .callbacks.callbacks import BaseCallback

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


def render_job_script(
    task_func: Callable[..., Any],
    task_args: Tuple[Any, ...],
    task_kwargs: Dict[str, Any],
    task_definition: Dict[str, Any],
    sbatch_overrides: Dict[str, Any],
    packaging_strategy: PackagingStrategy,
    target_job_dir: str,
    pre_submission_id: str,
    callbacks: List[BaseCallback],
    cluster: Optional["Cluster"] = None,
) -> str:
    """
    Renders the SLURM sbatch script using an explicit target directory path.

    Args:
        cluster: Optional cluster instance for workflow support (provides Slurmfile path and env name).
    """

    sbatch_params = normalize_sbatch_options(task_definition)
    overrides = normalize_sbatch_options(sbatch_overrides)
    sbatch_params.update(overrides)

    stdout_path = sbatch_params.get("output")
    if not stdout_path:
        stdout_path = f"{target_job_dir}/slurm_{pre_submission_id}.out"
        sbatch_params["output"] = stdout_path

    stderr_path = sbatch_params.get("error")
    if not stderr_path:
        stderr_path = f"{target_job_dir}/slurm_{pre_submission_id}.err"
        sbatch_params["error"] = stderr_path

    script_lines = ["#!/bin/bash"]

    job_name = sbatch_params.pop("job_name", None)
    if not job_name:
        job_name = task_func.__name__
    script_lines.append(f"#SBATCH --job-name={job_name}")

    output_path = sbatch_params.get("output")
    error_path = sbatch_params.get("error")
    if not output_path:
        output_path = f"{shlex.quote(target_job_dir)}/slurm_{pre_submission_id}.out"
        sbatch_params["output"] = output_path
    if not error_path:
        error_path = f"{shlex.quote(target_job_dir)}/slurm_{pre_submission_id}.err"
        sbatch_params["error"] = error_path

    # Emit remaining directives in insertion order
    for key, value in sbatch_params.items():
        if key == "job_name":
            continue
        flag = key.replace("_", "-")
        value_to_emit = value
        if isinstance(value, str) and key in {"output", "error"}:
            value_to_emit = shlex.quote(value)
        if value_to_emit is None:
            script_lines.append(f"#SBATCH --{flag}")
        else:
            script_lines.append(f"#SBATCH --{flag}={value_to_emit}")

    script_lines.append("")
    script_lines.append(f'echo "Target Job Directory (from Python): {target_job_dir}"')
    script_lines.append(f"export JOB_DIR={shlex.quote(target_job_dir)}")

    # Export cluster configuration for workflow support
    if cluster is not None:
        slurmfile_path = getattr(cluster, "slurmfile_path", None)
        env_name = getattr(cluster, "env_name", None)
        # The Slurmfile will be uploaded to the job directory as "Slurmfile.toml"
        if slurmfile_path:
            remote_slurmfile_path = f"{target_job_dir}/Slurmfile.toml"
            script_lines.append(
                f"export SLURM_SDK_SLURMFILE={shlex.quote(remote_slurmfile_path)}"
            )
        if env_name:
            script_lines.append(f"export SLURM_SDK_ENV={shlex.quote(env_name)}")

    script_lines.append("")
    script_lines.append('echo "SLURM Job ID: ${SLURM_JOB_ID:-}"')
    script_lines.append('echo "Running on host: $(hostname)"')
    script_lines.append('echo "Working directory: $(pwd)"')
    script_lines.append('echo "Job output directory JOB_DIR: $JOB_DIR"')
    script_lines.append("")

    try:
        setup_commands = packaging_strategy.generate_setup_commands(
            task=task_func,
            job_id=pre_submission_id,
            job_dir='"$JOB_DIR"',
        )
        if setup_commands:
            script_lines.extend(setup_commands)
            script_lines.append("echo 'Packaging setup commands executed.'")
        else:
            script_lines.append("echo 'No packaging setup commands generated.'")
    except Exception as e:
        logger.error("Error generating packaging setup commands: %s", e)
        traceback.print_exc(file=sys.stderr)
        script_lines.append(
            "echo 'ERROR: Failed to generate packaging setup commands!' >&2"
        )
        script_lines.append("exit 1")

    script_lines.append("")

    module_name = _get_importable_module_name(task_func)
    func_name = task_func.__name__

    args_file_path_str = f"slurm_job_{pre_submission_id}_{ARGS_FILENAME}"
    kwargs_file_path_str = f"slurm_job_{pre_submission_id}_{KWARGS_FILENAME}"
    result_file_path_str = f"slurm_job_{pre_submission_id}_{RESULT_FILENAME}"
    callbacks_file_path_str = f"slurm_job_{pre_submission_id}_{CALLBACKS_FILENAME}"

    try:
        pickled_args = base64.b64encode(pickle.dumps(task_args)).decode()
        pickled_kwargs = base64.b64encode(pickle.dumps(task_kwargs)).decode()
        pickled_sys_path = base64.b64encode(pickle.dumps(sys.path)).decode()

        picklable_callbacks = []
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
            base64.b64encode(pickle.dumps(picklable_callbacks)).decode()
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

    script_lines.append(f'base64 -d > "{args_file_path_str}" << "BASE64_ARGS"')
    script_lines.append(pickled_args)
    script_lines.append("BASE64_ARGS")
    script_lines.append(f'base64 -d > "{kwargs_file_path_str}" << "BASE64_KWARGS"')
    script_lines.append(pickled_kwargs)
    script_lines.append("BASE64_KWARGS")
    if pickled_callbacks:
        script_lines.append(f'base64 -d > "{callbacks_file_path_str}" << "BASE64_CBS"')
        script_lines.append(pickled_callbacks)
        script_lines.append("BASE64_CBS")
    else:
        script_lines.append(f'touch "{callbacks_file_path_str}"')
    script_lines.append("")

    script_lines.append("echo 'Executing Python task via packaged runner...' ")
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
        script_lines.append(
            f"export PYTHONPATH={shlex.quote(pythonpath_contrib)}:${{PYTHONPATH:-}}"
        )

    script_lines.append("PY_EXEC_RESOLVED=${PY_EXEC:-python}")
    script_lines.append("export PY_EXEC_RESOLVED")

    # Construct the runner command that executes the user's task function.
    def _escape_quotes(value: str) -> str:
        return value.replace('"', '\\"')

    runner_parts = [
        '"$PY_EXEC_RESOLVED"',
        "-m slurm.runner",
        f'--module "{_escape_quotes(module_name)}"',
        f'--function "{_escape_quotes(func_name)}"',
        f'--args-file "{_escape_quotes(args_file_path_str)}"',
        f'--kwargs-file "{_escape_quotes(kwargs_file_path_str)}"',
        f'--output-file "{_escape_quotes(result_file_path_str)}"',
        f'--callbacks-file "{_escape_quotes(callbacks_file_path_str)}"',
        f'--sys-path "{_escape_quotes(pickled_sys_path)}"',
    ]

    if target_job_dir:
        runner_parts.append(f'--job-dir "{_escape_quotes(target_job_dir)}"')
    if stdout_path:
        runner_parts.append(f'--stdout-path "{_escape_quotes(stdout_path)}"')
    if stderr_path:
        runner_parts.append(f'--stderr-path "{_escape_quotes(stderr_path)}"')
    runner_parts.append(f'--pre-submission-id "{_escape_quotes(pre_submission_id)}"')

    runner_command = " ".join(runner_parts)

    try:
        runner_command = packaging_strategy.wrap_execution_command(
            command=runner_command,
            task=task_func,
            job_id=pre_submission_id,
            job_dir='"$JOB_DIR"',
        )
    except Exception as e:
        logger.error("Error wrapping execution command: %s", e)
        traceback.print_exc(file=sys.stderr)
        script_lines.append("echo 'ERROR: Failed to wrap execution command!' >&2")
        script_lines.append("exit 1")
    else:
        script_lines.append(runner_command)
    script_lines.append("EXECUTION_STATUS=$?")
    script_lines.append("")

    try:
        cleanup_commands = packaging_strategy.generate_cleanup_commands(
            task=task_func,
            job_id=pre_submission_id,
            job_dir='"$JOB_DIR"',
        )
        if cleanup_commands:
            script_lines.extend(cleanup_commands)
            script_lines.append("echo 'Packaging cleanup commands executed.'")
        else:
            script_lines.append("echo 'No packaging cleanup commands generated.'")
    except Exception as e:
        logger.error("Error generating packaging cleanup commands: %s", e)
        traceback.print_exc(file=sys.stderr)
        script_lines.append(
            "echo 'WARNING: Failed to generate packaging cleanup commands!' >&2"
        )

    script_lines.append("")

    script_lines.append('echo "Job finished with status: $EXECUTION_STATUS"')
    script_lines.append("exit $EXECUTION_STATUS")

    final_script = "\n".join(
        line.rstrip("\r") for line in "\n".join(script_lines).splitlines()
    )
    return final_script
