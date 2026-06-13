"""Main orchestration module for the Slurm runner.

This module provides the main entry point for executing tasks submitted
to Slurm, using the modular components extracted from the original runner.
"""

import importlib
import logging
import os
import platform
import socket
import sys
import time
import traceback
from typing import Any, List, Optional

from slurm.callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    RunBeginContext,
    RunEndContext,
    WorkflowCallbackContext,
)
from slurm.runtime import (
    JobContext,
    _bind_job_context,
    _function_wants_job_context,
    current_job_context,
    install_shutdown_handler,
    resolve_current_hostname,
)
from slurm.workflow import WorkflowContext

from .initialization import (
    RunnerArgs,
    configure_logging,
    load_callbacks,
    load_task_arguments,
    log_startup_info,
    parse_args,
    resolve_replica_output_file,
    restore_sys_path,
)
from .callbacks import run_callbacks
from .context_manager import (
    bind_workflow_context,
    function_wants_workflow_context,
)
from .placeholder import resolve_placeholder
from .result_saver import save_result, update_job_metadata, write_environment_metadata
from .workflow_builder import (
    WorkflowSetupResult,
    setup_workflow_execution,
    teardown_workflow_execution,
)

logger = logging.getLogger("slurm.runner")


def _publish_initial_hostinfo(job_context: JobContext) -> None:
    """Record the peer's actual hostname + step id in the registry.

    Bootstrap seeds every peer entry with empty ``hostname`` / ``step_id``
    because it runs *before* Slurm picks which node each unpinned srun
    step lands on. The runner is the first process that knows the answer
    (it's running on the chosen node), so we publish as soon as the
    runner has a registry path and a peer identity.

    For pinned peers the values are already correct after bootstrap;
    :func:`update_peer_hostinfo` elides the write in that case.

    Under local-mode (no srun) ``SLURM_STEP_ID`` is absent and the
    helper stores ``None`` — still useful since the hostname publish is
    what ``ctx.peers[...].first.hostname`` depends on.
    """
    if job_context.peer_name is None or job_context._registry_path is None:
        return

    hostname = resolve_current_hostname(job_context)
    if hostname is None:
        return
    step_id = os.environ.get("SLURM_STEP_ID") or None
    try:
        from slurm.parallel.registry import update_peer_hostinfo

        replica = job_context.replica_index or 0
        update_peer_hostinfo(
            job_context._registry_path,
            job_context.peer_name,
            replica,
            hostname=hostname,
            step_id=step_id,
        )
    except (FileNotFoundError, KeyError, IndexError, OSError) as exc:
        logger.debug("Could not publish initial hostinfo: %s", exc)


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
    if not function_wants_workflow_context(func):
        return task_args, task_kwargs, None

    # Set up workflow execution environment
    setup_result = setup_workflow_execution(job_id, job_dir)

    # Bind workflow context to function
    task_args, task_kwargs, injected = bind_workflow_context(
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


def main():
    """Main entry point for the Slurm task runner.

    This function orchestrates task execution using the modular components
    in the slurm.runner package.
    """
    # Parse command-line arguments
    args = parse_args()

    # Replica dispatch: per-task rewrite of ``--output-file`` so each replica
    # writes to its own pickle. See ``apply_replica_output_suffix`` for the
    # mechanics; in singleton / non-peer runs this is a no-op.
    args.output_file = resolve_replica_output_file(args)

    # Configure logging
    configure_logging(args.loglevel)
    log_startup_info(args)

    # Install SIGTERM handler from the main thread so ``ctx.shutdown_requested``
    # flips when the supervisor (or operator) signals the runner. This must
    # happen in ``main()`` because Python only allows ``signal.signal`` from
    # the main thread.
    try:
        install_shutdown_handler()
    except ValueError:
        # Not on the main thread (e.g. runner invoked under a test harness).
        logger.debug("Could not install SIGTERM handler — not on main thread")

    # Get runtime context
    job_context: JobContext = current_job_context()

    # Publish the peer's actual hostname + step id. Bootstrap cannot know
    # these for unpinned peers until the ``srun`` step lands somewhere;
    # this runner invocation is the first process that does.
    _publish_initial_hostinfo(job_context)
    job_id = get_job_id_from_env()
    job_dir = args.job_dir or os.environ.get("JOB_DIR")
    stdout_path = args.stdout_path or os.environ.get("SLURM_STDOUT")
    stderr_path = args.stderr_path or os.environ.get("SLURM_STDERR")
    environment_snapshot = get_environment_snapshot()
    run_start_time = time.time()

    # Restore sys.path if provided
    if args.sys_path:
        restore_sys_path(args.sys_path)

    # Track state for cleanup
    callbacks: List[BaseCallback] = []
    workflow_setup_result = None

    try:
        # Load task arguments and callbacks
        task_args, task_kwargs = load_task_arguments(args, job_dir)
        callbacks = load_callbacks(args.callbacks_file)

        logger.debug("Deserialized Args: %s", task_args)
        logger.debug("Deserialized Kwargs: %s", task_kwargs)

        # Resolve JobResultPlaceholder objects
        task_args = resolve_placeholder(task_args)
        task_kwargs = resolve_placeholder(task_kwargs)

        logger.debug("Resolved Args: %s", task_args)
        logger.debug("Resolved Kwargs: %s", task_kwargs)

        # Load the function to execute
        func = load_function(args.module, args.function)

        # Unwrap @task decorated functions for direct execution
        if hasattr(func, "unwrapped"):
            logger.debug("Unwrapping @task decorated function for execution")
            func = func.unwrapped

        # Handle context injection
        if _function_wants_job_context(func):
            # JobContext injection
            task_args, task_kwargs = handle_job_context_injection(
                func, task_args, task_kwargs, job_context, args.module, args.function
            )
        elif function_wants_workflow_context(func):
            # WorkflowContext injection - sets up cluster and context
            task_args, task_kwargs, workflow_setup_result = (
                handle_workflow_context_injection(
                    func, task_args, task_kwargs, args, job_id, job_dir, callbacks
                )
            )

        # Call begin callbacks
        _call_begin_callback(
            callbacks,
            args,
            job_id,
            job_dir,
            environment_snapshot,
            run_start_time,
            job_context,
        )

        # Execute the task
        logger.info("Executing task")
        logger.info(f"Task args: {task_args}")
        logger.info(f"Task kwargs: {task_kwargs}")

        task_result = None
        task_exception = None
        try:
            logger.info("About to execute function...")
            result = execute_task(func, task_args, task_kwargs)
            logger.info(f"Function returned: {result}")
            task_result = result
        except Exception as e:
            task_exception = e
            logger.error(f"Function raised exception: {e}", exc_info=True)
            raise
        finally:
            # Emit workflow end event if workflow context was activated
            if workflow_setup_result is not None:
                _emit_workflow_end_callback(
                    callbacks,
                    job_id,
                    job_dir,
                    args.function,
                    workflow_setup_result.workflow_context,
                    task_result,
                    task_exception,
                )
                # Teardown workflow execution (deactivate context, cleanup SSH)
                teardown_workflow_execution(workflow_setup_result)

        logger.info("Task execution complete")
        end_time = time.time()

        # Save result and metadata
        save_result(args.output_file, result)
        update_job_metadata(args.output_file, job_id or "unknown", end_time)

        # Call success callbacks
        _call_success_callbacks(
            callbacks,
            args,
            job_id,
            job_dir,
            stdout_path,
            stderr_path,
            run_start_time,
            end_time,
            job_context,
        )

        logger.info("=" * 70)
        logger.info("RUNNER EXITING SUCCESSFULLY")
        logger.info("=" * 70)
        sys.exit(0)

    except Exception as e:
        logger.error("Error during task execution: %s", e)
        error_traceback = traceback.format_exc()
        sys.stderr.write(error_traceback)
        sys.stderr.flush()

        end_time = time.time()

        # Call failure callbacks
        _call_failure_callbacks(
            callbacks,
            args,
            job_id,
            job_dir,
            stdout_path,
            stderr_path,
            run_start_time,
            end_time,
            job_context,
            e,
            error_traceback,
        )

        logger.error("=" * 70)
        logger.error("RUNNER EXITING WITH FAILURE")
        logger.error("=" * 70)
        sys.exit(1)


def _call_begin_callback(
    callbacks: List[BaseCallback],
    args: RunnerArgs,
    job_id: Optional[str],
    job_dir: Optional[str],
    environment_snapshot: dict,
    start_time: float,
    job_context: JobContext,
) -> None:
    """Call callbacks for task execution beginning."""
    hostname = socket.gethostname()
    python_executable = sys.executable
    python_version = sys.version
    working_directory = os.getcwd()

    logger.debug("Calling on_begin_run_job callbacks...")
    try:
        ctx = RunBeginContext(
            module=args.module,
            function=args.function,
            args_file=args.args_file or "",
            kwargs_file=args.kwargs_file or "",
            output_file=args.output_file,
            job_id=job_id,
            job_dir=job_dir,
            hostname=hostname,
            python_executable=python_executable,
            python_version=python_version,
            working_directory=working_directory,
            environment_snapshot=environment_snapshot,
            start_time=start_time,
            job_context=job_context,
        )
        run_callbacks(callbacks, "on_begin_run_job_ctx", ctx)
    except Exception as e:
        logger.warning(f"Callback on_begin_run_job_ctx failed: {e}")


def _emit_workflow_end_callback(
    callbacks: List[BaseCallback],
    job_id: Optional[str],
    job_dir: Optional[str],
    workflow_name: str,
    workflow_context: "WorkflowContext",
    result: Optional[Any],
    exception: Optional[Exception],
) -> None:
    """Emit workflow end callback event."""
    from pathlib import Path

    logger.debug("Calling on_workflow_end callbacks...")
    try:
        workflow_end_ctx = WorkflowCallbackContext(
            workflow_job_id=job_id or "unknown",
            workflow_job_dir=Path(job_dir) if job_dir else Path.cwd(),
            workflow_name=workflow_name,
            workflow_context=workflow_context,
            timestamp=time.time(),
            result=result,
            exception=exception,
            cluster=None,
        )
        run_callbacks(callbacks, "on_workflow_end_ctx", workflow_end_ctx)
    except Exception as e:
        logger.warning(f"Error calling workflow end callbacks: {e}")


def _call_success_callbacks(
    callbacks: List[BaseCallback],
    args: RunnerArgs,
    job_id: Optional[str],
    job_dir: Optional[str],
    stdout_path: Optional[str],
    stderr_path: Optional[str],
    start_time: float,
    end_time: float,
    job_context: JobContext,
) -> None:
    """Call callbacks for successful task execution."""
    hostname = socket.gethostname()
    python_version = sys.version

    logger.debug("Calling on_end_run_job callbacks (success)...")
    try:
        run_callbacks(
            callbacks,
            "on_end_run_job_ctx",
            RunEndContext(
                status="success",
                output_file=args.output_file,
                job_id=job_id,
                job_dir=job_dir,
                hostname=hostname,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                job_context=job_context,
            ),
        )
    except Exception as e:
        logger.warning(f"Callback on_end_run_job_ctx (success) failed: {e}")

    status_payload = {
        "JobState": "COMPLETED",
        "ExitCode": "0:0",
    }
    if args.pre_submission_id:
        status_payload["PreSubmissionId"] = args.pre_submission_id
    status_payload["Hostname"] = hostname
    status_payload["PythonVersion"] = python_version
    status_payload["Platform"] = platform.platform()

    try:
        run_callbacks(
            callbacks,
            "on_completed_ctx",
            CompletedContext(
                job=None,
                job_id=job_id,
                job_dir=job_dir,
                job_state="COMPLETED",
                exit_code="0:0",
                reason=None,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                status=status_payload,
                result_path=args.output_file,
                emitted_by=ExecutionLocus.RUNNER,
                job_context=job_context,
            ),
        )
    except Exception as e:
        logger.warning(f"Callback on_completed_ctx (success) failed: {e}")


def _call_failure_callbacks(
    callbacks: List[BaseCallback],
    args: RunnerArgs,
    job_id: Optional[str],
    job_dir: Optional[str],
    stdout_path: Optional[str],
    stderr_path: Optional[str],
    start_time: float,
    end_time: float,
    job_context: JobContext,
    exception: Exception,
    error_traceback: str,
) -> None:
    """Call callbacks for failed task execution."""
    hostname = socket.gethostname()
    python_version = sys.version

    logger.debug("Calling on_end_run_job callbacks (failure)...")
    try:
        run_callbacks(
            callbacks,
            "on_end_run_job_ctx",
            RunEndContext(
                status="failure",
                error_type=type(exception).__name__,
                error_message=str(exception),
                traceback=error_traceback,
                job_id=job_id,
                job_dir=job_dir,
                hostname=hostname,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                job_context=job_context,
            ),
        )
    except Exception as exc:
        logger.warning(f"Callback on_end_run_job_ctx (failure) failed: {exc}")

    status_payload = {
        "JobState": "FAILED",
        "ExitCode": "1:0",
        "ErrorType": type(exception).__name__,
        "ErrorMessage": str(exception),
    }
    if args.pre_submission_id:
        status_payload["PreSubmissionId"] = args.pre_submission_id
    status_payload["Hostname"] = hostname
    status_payload["PythonVersion"] = python_version
    status_payload["Platform"] = platform.platform()

    try:
        run_callbacks(
            callbacks,
            "on_completed_ctx",
            CompletedContext(
                job=None,
                job_id=job_id,
                job_dir=job_dir,
                job_state="FAILED",
                exit_code="1:0",
                reason=str(exception),
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                status=status_payload,
                error_type=type(exception).__name__,
                error_message=str(exception),
                traceback=error_traceback,
                result_path=args.output_file,
                emitted_by=ExecutionLocus.RUNNER,
                job_context=job_context,
            ),
        )
    except Exception as exc:
        logger.warning(f"Callback on_completed_ctx (failure) failed: {exc}")


__all__ = [
    "main",
    "get_job_id_from_env",
    "get_environment_snapshot",
    "load_function",
    "execute_task",
    "handle_job_context_injection",
    "handle_workflow_context_injection",
]
