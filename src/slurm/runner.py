"""
Internal runner script executed by Slurm jobs to run Python tasks.
Handles deserialization of the function and arguments, execution,
and serialization of the result.
"""

import argparse
import importlib
import logging
import os
import pickle
import sys
import traceback
import time
import socket
import platform
from typing import List

from slurm.callbacks.callbacks import (
    BaseCallback,
    CompletedContext,
    ExecutionLocus,
    RunBeginContext,
    RunEndContext,
)
from slurm.runtime import (
    JobContext,
    _bind_job_context,
    current_job_context,
    _function_wants_job_context,
)

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


def main():
    parser = argparse.ArgumentParser(description="Slurm Task Runner")
    parser.add_argument(
        "--module", required=True, help="Module containing the task function"
    )
    parser.add_argument("--function", required=True, help="Name of the task function")
    parser.add_argument(
        "--args-file", required=True, help="Path to the pickled args tuple file"
    )
    parser.add_argument(
        "--kwargs-file", required=True, help="Path to the pickled kwargs dict file"
    )
    parser.add_argument(
        "--output-file", required=True, help="Path to save the pickled result"
    )
    parser.add_argument(
        "--sys-path", help="Original sys.path pickled and base64 encoded"
    )
    parser.add_argument("--loglevel", help="Log level", default="INFO")
    parser.add_argument(
        "--callbacks-file",
        required=True,
        help="Path to the pickled callbacks list file",
    )
    parser.add_argument("--job-dir", help="Resolved job directory on the runner")
    parser.add_argument("--stdout-path", help="Scheduler stdout path for the job")
    parser.add_argument("--stderr-path", help="Scheduler stderr path for the job")
    parser.add_argument(
        "--pre-submission-id",
        help="SDK pre-submission identifier associated with this run",
    )
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    logger.info("Starting task execution")
    logger.debug("Module=%s, Function=%s", args.module, args.function)
    logger.debug("Args file=%s", args.args_file)
    logger.debug("Kwargs file=%s", args.kwargs_file)
    logger.debug("Output file=%s", args.output_file)
    logger.debug("Callbacks file=%s", args.callbacks_file)
    logger.debug("Log level=%s", args.loglevel)

    job_context: JobContext = current_job_context()

    job_id_env = os.environ.get("SLURM_JOB_ID")
    job_dir_env = os.environ.get("JOB_DIR")
    job_dir = args.job_dir or job_dir_env
    stdout_path = args.stdout_path or os.environ.get("SLURM_STDOUT")
    stderr_path = args.stderr_path or os.environ.get("SLURM_STDERR")
    hostname = socket.gethostname()
    python_executable = sys.executable
    python_version = sys.version
    working_directory = os.getcwd()
    env_snapshot_keys = [
        "SLURM_JOB_ID",
        "SLURM_JOB_NAME",
        "SLURM_CLUSTER_NAME",
        "SLURM_SUBMIT_DIR",
        "JOB_DIR",
    ]
    environment_snapshot = {
        key: os.environ[key] for key in env_snapshot_keys if key in os.environ
    }
    run_start_time = time.time()

    # Optional: Restore sys.path if provided - helps find user modules
    if args.sys_path:
        import base64

        original_sys_path = pickle.loads(base64.b64decode(args.sys_path.encode()))
        # Prepend original paths to ensure user modules are found first
        sys.path = original_sys_path + [
            p for p in sys.path if p not in original_sys_path
        ]
        logger.debug("Updated sys.path: %s", sys.path)

    callbacks: List[BaseCallback] = []
    try:
        # Load arguments
        with open(args.args_file, "rb") as f:
            task_args = pickle.load(f)
        with open(args.kwargs_file, "rb") as f:
            task_kwargs = pickle.load(f)
        # Load callbacks
        try:
            with open(args.callbacks_file, "rb") as f:
                # Handle empty file case (created by rendering script if no callbacks)
                content = f.read()
                if content:
                    callbacks = pickle.loads(content)
                    logger.debug("Deserialized %d callbacks.", len(callbacks))
                else:
                    logger.debug("No callbacks provided (empty callbacks file).")
        except FileNotFoundError:
            logger.warning(
                "Runner: Callbacks file not found at %s", args.callbacks_file
            )
        except (pickle.UnpicklingError, EOFError) as e:
            logger.error(
                "Runner: Error deserializing callbacks from %s: %s",
                args.callbacks_file,
                e,
            )

        logger.debug("Deserialized Args: %s", task_args)
        logger.debug("Deserialized Kwargs: %s", task_kwargs)

        logger.debug("Calling on_begin_run_job callbacks...")
        try:
            run_start_time = time.time()
            ctx = RunBeginContext(
                module=args.module,
                function=args.function,
                args_file=args.args_file,
                kwargs_file=args.kwargs_file,
                output_file=args.output_file,
                job_id=job_id_env,
                job_dir=job_dir,
                hostname=hostname,
                python_executable=python_executable,
                python_version=python_version,
                working_directory=working_directory,
                environment_snapshot=environment_snapshot,
                start_time=run_start_time,
                job_context=job_context,
            )
            _run_callbacks(callbacks, "on_begin_run_job_ctx", ctx)
        except Exception:
            pass

        logger.debug("Importing module %s...", args.module)
        module = importlib.import_module(args.module)
        logger.debug("Getting function %s from module...", args.function)
        func = getattr(module, args.function)

        if _function_wants_job_context(func):
            task_args, task_kwargs, injected = _bind_job_context(
                func, task_args, task_kwargs, job_context
            )
            if injected:
                logger.debug(
                    "Injected JobContext into %s.%s",
                    args.module,
                    args.function,
                )
            else:
                logger.debug(
                    "JobContext requested by %s.%s but argument already provided",
                    args.module,
                    args.function,
                )

        logger.info("Executing task")
        result = func(*task_args, **task_kwargs)
        logger.info("Task execution complete")

        end_time = time.time()

        logger.debug("Saving result to %s...", args.output_file)

        output_dir = os.path.dirname(args.output_file)
        if output_dir:
            logger.debug("Ensuring output directory exists: %s", output_dir)
            os.makedirs(output_dir, exist_ok=True)

        with open(args.output_file, "wb") as f:
            pickle.dump(result, f)

        logger.debug("Result saved successfully.")

        logger.debug("Calling on_end_run_job callbacks (success)...")
        try:
            _run_callbacks(
                callbacks,
                "on_end_run_job_ctx",
                RunEndContext(
                    status="success",
                    output_file=args.output_file,
                    job_id=job_id_env,
                    job_dir=job_dir,
                    hostname=hostname,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    start_time=run_start_time,
                    end_time=end_time,
                    duration=end_time - run_start_time,
                    job_context=job_context,
                ),
            )
        except Exception:
            pass

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
            _run_callbacks(
                callbacks,
                "on_completed_ctx",
                CompletedContext(
                    job=None,
                    job_id=job_id_env,
                    job_dir=job_dir,
                    job_state="COMPLETED",
                    exit_code="0:0",
                    reason=None,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    start_time=run_start_time,
                    end_time=end_time,
                    duration=end_time - run_start_time,
                    status=status_payload,
                    result_path=args.output_file,
                    emitted_by=ExecutionLocus.RUNNER,
                    job_context=job_context,
                ),
            )
        except Exception:
            pass
        sys.exit(0)
    except Exception as e:
        logger.error("Error during task execution: %s", e)
        error_traceback = traceback.format_exc()
        sys.stderr.write(error_traceback)
        sys.stderr.flush()

        end_time = time.time()

        logger.debug("Calling on_end_run_job callbacks (failure)...")
        try:
            _run_callbacks(
                callbacks,
                "on_end_run_job_ctx",
                RunEndContext(
                    status="failure",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback=error_traceback,
                    job_id=job_id_env,
                    job_dir=job_dir,
                    hostname=hostname,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    start_time=run_start_time,
                    end_time=end_time,
                    duration=end_time - run_start_time,
                    job_context=job_context,
                ),
            )
        except Exception:
            pass

        status_payload = {
            "JobState": "FAILED",
            "ExitCode": "1:0",
            "ErrorType": type(e).__name__,
            "ErrorMessage": str(e),
        }
        if args.pre_submission_id:
            status_payload["PreSubmissionId"] = args.pre_submission_id
        status_payload["Hostname"] = hostname
        status_payload["PythonVersion"] = python_version
        status_payload["Platform"] = platform.platform()

        try:
            _run_callbacks(
                callbacks,
                "on_completed_ctx",
                CompletedContext(
                    job=None,
                    job_id=job_id_env,
                    job_dir=job_dir,
                    job_state="FAILED",
                    exit_code="1:0",
                    reason=str(e),
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    start_time=run_start_time,
                    end_time=end_time,
                    duration=end_time - run_start_time,
                    status=status_payload,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback=error_traceback,
                    result_path=args.output_file,
                    emitted_by=ExecutionLocus.RUNNER,
                    job_context=job_context,
                ),
            )
        except Exception:
            pass

        sys.exit(1)


if __name__ == "__main__":
    main()
