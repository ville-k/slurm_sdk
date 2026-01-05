"""
Internal runner script executed by Slurm jobs to run Python tasks.
Handles deserialization of the function and arguments, execution,
and serialization of the result.

This module provides backwards-compatible exports while the main logic
has been refactored into the slurm.runner package.
"""

import json
import logging
import os
import platform
import sys
import time
import traceback
from datetime import datetime
from typing import List, Optional

from slurm.callbacks.callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    RunBeginContext,
    RunEndContext,
    WorkflowCallbackContext,
)
from slurm.runtime import (
    JobContext,
    current_job_context,
)
from slurm.workflow import WorkflowContext

# Import from new modular runner package
from slurm.runner.argument_loader import (
    RunnerArgs,
    configure_logging,
    load_callbacks,
    load_task_arguments,
    log_startup_info,
    parse_args,
    restore_sys_path,
)
from slurm.runner.callbacks import run_callbacks

# Note: _function_wants_workflow_context and _bind_workflow_context are defined
# locally in this file for backwards compatibility. The runner.context_manager
# module also exports them.
from slurm.runner.main import (
    execute_task,
    get_environment_snapshot,
    get_job_id_from_env,
    handle_job_context_injection,
    handle_workflow_context_injection,
    load_function,
)
from slurm.runner.placeholder import resolve_placeholder
from slurm.runner.result_saver import save_result, update_job_metadata
from slurm.runner.workflow_builder import teardown_workflow_execution

logger = logging.getLogger("slurm.runner")


def _run_callbacks(callbacks: List[BaseCallback], method_name: str, *args, **kwargs):
    """Helper function to run a specific method on a list of callbacks, catching errors."""
    for callback in callbacks:
        if hasattr(
            callback, "should_run_on_runner"
        ) and not callback.should_run_on_runner(method_name):
            continue
        try:
            method = getattr(callback, method_name)
            method(*args, **kwargs)
        except Exception as e:
            logger.warning(
                f"Runner: Error executing callback {type(callback).__name__}.{method_name}: {e}"
            )


def _function_wants_workflow_context(func):
    """Check if function expects a WorkflowContext parameter."""
    import inspect

    try:
        sig = inspect.signature(func)
        for param in sig.parameters.values():
            # Check by annotation
            annotation = param.annotation
            if annotation != inspect.Parameter.empty:
                if annotation is WorkflowContext:
                    return True
                # Check string annotations
                if isinstance(annotation, str) and "WorkflowContext" in annotation:
                    return True
                # Check name attribute
                if (
                    hasattr(annotation, "__name__")
                    and annotation.__name__ == "WorkflowContext"
                ):
                    return True
            # Check by parameter name
            if param.name in ("ctx", "context", "workflow_context"):
                return True
        return False
    except Exception:
        return False


def _bind_workflow_context(func, args, kwargs, workflow_context):
    """Inject workflow_context into function if it expects it.

    Returns (args, kwargs, injected).
    """
    import inspect

    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # Find the parameter that wants WorkflowContext
        target_param = None
        for param in params:
            annotation = param.annotation
            # Check by annotation
            if annotation != inspect.Parameter.empty:
                if annotation is WorkflowContext:
                    target_param = param
                    break
                if isinstance(annotation, str) and "WorkflowContext" in annotation:
                    target_param = param
                    break
                if (
                    hasattr(annotation, "__name__")
                    and annotation.__name__ == "WorkflowContext"
                ):
                    target_param = param
                    break
            # Check by parameter name
            if param.name in ("ctx", "context", "workflow_context"):
                target_param = param
                break

        if target_param is None:
            return args, kwargs, False

        param_name = target_param.name

        # If already provided, don't inject
        if param_name in kwargs:
            return args, kwargs, False

        # Inject as keyword argument
        new_kwargs = dict(kwargs)
        new_kwargs[param_name] = workflow_context
        return args, new_kwargs, True

    except Exception as e:
        logger.warning(f"Error binding workflow context: {e}")
        return args, kwargs, False


def _write_environment_metadata(
    job_dir: str,
    packaging_type: str,
    job_id: Optional[str] = None,
    workflow_name: Optional[str] = None,
    pre_submission_id: Optional[str] = None,
) -> None:
    """
    Write environment metadata for child tasks to inherit.

    This metadata file allows child tasks using InheritPackagingStrategy
    to discover and activate the parent workflow's execution environment.

    Args:
        job_dir: The workflow job directory
        packaging_type: The type of packaging used (wheel, container, etc.)
        job_id: The SLURM job ID
        workflow_name: The name of the workflow function
        pre_submission_id: The pre-submission ID
    """
    from pathlib import Path

    try:
        metadata_path = Path(job_dir) / ".slurm_environment.json"

        # Detect current environment details
        venv_path = os.environ.get("VIRTUAL_ENV")
        python_executable = sys.executable
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        container_image = os.environ.get("SINGULARITY_NAME") or os.environ.get(
            "SLURM_CONTAINER_IMAGE"
        )

        # Build metadata structure
        metadata = {
            "version": "1.0",
            "packaging_type": packaging_type,
            "environment": {
                "venv_path": venv_path,
                "python_executable": python_executable,
                "python_version": python_version,
                "container_image": container_image,
                "activated": bool(venv_path or container_image),
            },
            "shared_paths": {
                "job_dir": job_dir,
                "shared_dir": str(Path(job_dir) / "shared"),
                "tasks_dir": str(Path(job_dir) / "tasks"),
            },
            "parent_job": {
                "slurm_job_id": job_id or "unknown",
                "pre_submission_id": pre_submission_id or "unknown",
                "workflow_name": workflow_name or "unknown",
            },
            "created_at": datetime.utcnow().isoformat() + "Z",
        }

        # Write metadata file
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Wrote environment metadata to {metadata_path}")
        logger.debug(f"Metadata: {json.dumps(metadata, indent=2)}")

    except Exception as e:
        logger.warning(f"Failed to write environment metadata: {e}")
        # Non-fatal - child tasks will fall back to other strategies


def main():
    """Main entry point for the Slurm task runner.

    This function orchestrates task execution using the modular components
    extracted into the slurm.runner package.
    """

    from slurm.runtime import _function_wants_job_context

    # Parse command-line arguments
    args = parse_args()

    # Configure logging
    configure_logging(args.loglevel)
    log_startup_info(args)

    # Get runtime context
    job_context: JobContext = current_job_context()
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
        elif _function_wants_workflow_context(func):
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
        update_job_metadata(args.output_file, job_id, end_time)

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
    import socket

    hostname = socket.gethostname()
    python_executable = sys.executable
    python_version = sys.version
    working_directory = os.getcwd()

    logger.debug("Calling on_begin_run_job callbacks...")
    try:
        ctx = RunBeginContext(
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
    result: Optional[any],
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
    import socket

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
    import socket

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


if __name__ == "__main__":
    main()
