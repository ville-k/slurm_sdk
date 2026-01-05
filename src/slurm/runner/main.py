"""Main orchestration module for the Slurm runner.

This module provides the main entry point for executing tasks submitted
to Slurm, using the modular components extracted from the original runner.
"""

import importlib
import logging
import os
import socket
import sys
import time
from typing import Any, List, Optional

from slurm.callbacks.callbacks import (
    BaseCallback,
    RunBeginContext,
    RunEndContext,
    WorkflowCallbackContext,
)
from slurm.runtime import (
    JobContext,
    _bind_job_context,
    _function_wants_job_context,
)

from .argument_loader import (
    RunnerArgs,
)
from .callbacks import run_callbacks
from .context_manager import (
    _bind_workflow_context,
    _function_wants_workflow_context,
)
from .result_saver import write_environment_metadata
from .workflow_builder import (
    WorkflowSetupResult,
    setup_workflow_execution,
)

logger = logging.getLogger("slurm.runner")


def get_job_id_from_env() -> Optional[str]:
    """Get the job ID from environment variables.

    Handles both regular jobs and array jobs.

    Returns:
        Job ID string or None.
    """
    array_job_id = os.environ.get("SLURM_ARRAY_JOB_ID")
    array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")

    if array_job_id and array_task_id:
        return f"{array_job_id}_{array_task_id}"
    else:
        return os.environ.get("SLURM_JOB_ID")


def get_environment_snapshot() -> dict:
    """Capture a snapshot of relevant environment variables.

    Returns:
        Dict of environment variable names to values.
    """
    snapshot_keys = [
        "SLURM_JOB_ID",
        "SLURM_JOB_NAME",
        "SLURM_CLUSTER_NAME",
        "SLURM_SUBMIT_DIR",
        "SLURM_ARRAY_TASK_ID",
        "JOB_DIR",
    ]
    return {key: os.environ[key] for key in snapshot_keys if key in os.environ}


def load_function(module_name: str, function_name: str) -> Any:
    """Load a function from a module.

    Args:
        module_name: Fully qualified module name.
        function_name: Name of the function to load.

    Returns:
        The loaded function.

    Raises:
        ImportError: If module cannot be imported.
        AttributeError: If function not found in module.
    """
    logger.debug("Importing module: %s", module_name)
    module = importlib.import_module(module_name)
    logger.debug("Getting function: %s", function_name)
    return getattr(module, function_name)


def execute_task(
    func: Any,
    task_args: tuple,
    task_kwargs: dict,
) -> Any:
    """Execute the task function with arguments.

    Args:
        func: The function to execute.
        task_args: Positional arguments.
        task_kwargs: Keyword arguments.

    Returns:
        The result of the function execution.
    """
    logger.info("Executing task...")
    result = func(*task_args, **task_kwargs)
    logger.info("Task execution complete")
    return result


def handle_job_context_injection(
    func: Any,
    task_args: tuple,
    task_kwargs: dict,
    job_context: JobContext,
    module_name: str,
    function_name: str,
) -> tuple[tuple, dict]:
    """Handle JobContext injection if function expects it.

    Args:
        func: The function to check.
        task_args: Current positional arguments.
        task_kwargs: Current keyword arguments.
        job_context: The JobContext to potentially inject.
        module_name: Module name for logging.
        function_name: Function name for logging.

    Returns:
        Tuple of (possibly modified args, possibly modified kwargs).
    """
    if _function_wants_job_context(func):
        task_args, task_kwargs, injected = _bind_job_context(
            func, task_args, task_kwargs, job_context
        )
        if injected:
            logger.debug("Injected JobContext into %s.%s", module_name, function_name)
        else:
            logger.debug(
                "JobContext requested by %s.%s but argument already provided",
                module_name,
                function_name,
            )
    return task_args, task_kwargs


def handle_workflow_context_injection(
    func: Any,
    task_args: tuple,
    task_kwargs: dict,
    args: RunnerArgs,
    job_id: Optional[str],
    job_dir: Optional[str],
    callbacks: List[BaseCallback],
) -> tuple[tuple, dict, Optional[WorkflowSetupResult]]:
    """Handle WorkflowContext injection if function expects it.

    Args:
        func: The function to check.
        task_args: Current positional arguments.
        task_kwargs: Current keyword arguments.
        args: Parsed runner arguments.
        job_id: The job ID.
        job_dir: The job directory.
        callbacks: Callbacks for workflow events.

    Returns:
        Tuple of (args, kwargs, workflow_setup_result or None).
    """
    if not _function_wants_workflow_context(func):
        return task_args, task_kwargs, None

    # Set up workflow execution environment
    setup_result = setup_workflow_execution(job_id, job_dir)

    # Bind workflow context to function
    task_args, task_kwargs, injected = _bind_workflow_context(
        func, task_args, task_kwargs, setup_result.workflow_context
    )

    if injected:
        logger.debug("Injected WorkflowContext into %s.%s", args.module, args.function)
    else:
        logger.debug(
            "WorkflowContext requested by %s.%s but argument already provided",
            args.module,
            args.function,
        )

    # Write environment metadata for child tasks
    logger.info("Writing environment metadata...")
    write_environment_metadata(
        job_dir=job_dir or str(setup_result.workflow_context.workflow_job_dir),
        packaging_type=setup_result.parent_packaging_type,
        job_id=job_id,
        workflow_name=args.function,
        pre_submission_id=args.pre_submission_id,
    )
    logger.info("Environment metadata written")

    # Emit workflow begin callback
    try:
        workflow_begin_ctx = WorkflowCallbackContext(
            workflow_job_id=job_id or "unknown",
            workflow_job_dir=setup_result.workflow_context.workflow_job_dir,
            workflow_name=args.function,
            workflow_context=setup_result.workflow_context,
            timestamp=time.time(),
            cluster=None,
        )
        run_callbacks(callbacks, "on_workflow_begin_ctx", workflow_begin_ctx)
    except Exception as e:
        logger.warning(f"Callback on_workflow_begin_ctx failed: {e}")

    return task_args, task_kwargs, setup_result


def run_task_with_callbacks(
    func: Any,
    task_args: tuple,
    task_kwargs: dict,
    args: RunnerArgs,
    callbacks: List[BaseCallback],
    job_id: Optional[str],
    job_dir: Optional[str],
    job_context: JobContext,
    environment_snapshot: dict,
) -> Any:
    """Execute task with before/after callbacks.

    Args:
        func: The function to execute.
        task_args: Positional arguments.
        task_kwargs: Keyword arguments.
        args: Parsed runner arguments.
        callbacks: List of callbacks.
        job_id: Job ID.
        job_dir: Job directory.
        job_context: JobContext instance.
        environment_snapshot: Environment snapshot dict.

    Returns:
        The result of the task execution.
    """
    run_start_time = time.time()
    hostname = socket.gethostname()
    python_executable = sys.executable
    python_version = sys.version
    working_directory = os.getcwd()

    # Call begin callbacks
    try:
        begin_ctx = RunBeginContext(
            module=args.module,
            function=args.function,
            args_file=args.args_file,
            kwargs_file=args.kwargs_file,
            output_file=args.output_file,
            job_id=job_id,
            job_dir=job_dir,
            hostname=hostname,
            python_executable=python_executable,
            python_version=python_version,
            working_directory=working_directory,
            environment_snapshot=environment_snapshot,
            start_time=run_start_time,
            job_context=job_context,
        )
        run_callbacks(callbacks, "on_begin_run_job_ctx", begin_ctx)
    except Exception as e:
        logger.warning(f"Callback on_begin_run_job_ctx failed: {e}")

    # Execute task
    result = execute_task(func, task_args, task_kwargs)

    # Call end callbacks
    run_end_time = time.time()
    try:
        end_ctx = RunEndContext(
            module=args.module,
            function=args.function,
            args_file=args.args_file,
            kwargs_file=args.kwargs_file,
            output_file=args.output_file,
            job_id=job_id,
            job_dir=job_dir,
            hostname=hostname,
            python_executable=python_executable,
            python_version=python_version,
            working_directory=working_directory,
            environment_snapshot=environment_snapshot,
            start_time=run_start_time,
            end_time=run_end_time,
            duration=run_end_time - run_start_time,
            result=result,
            exception=None,
            job_context=job_context,
        )
        run_callbacks(callbacks, "on_end_run_job_ctx", end_ctx)
    except Exception as e:
        logger.warning(f"Callback on_end_run_job_ctx failed: {e}")

    return result


__all__ = [
    "get_job_id_from_env",
    "get_environment_snapshot",
    "load_function",
    "execute_task",
    "handle_job_context_injection",
    "handle_workflow_context_injection",
    "run_task_with_callbacks",
]
