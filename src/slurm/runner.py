"""
Internal runner script executed by Slurm jobs to run Python tasks.
Handles deserialization of the function and arguments, execution,
and serialization of the result.
"""

import argparse
import importlib
import json
import logging
import os
import pickle
import platform
import socket
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

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
    _bind_job_context,
    _function_wants_job_context,
    current_job_context,
)
from slurm.workflow import WorkflowContext

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
    parser = argparse.ArgumentParser(description="Slurm Task Runner")
    parser.add_argument(
        "--module", required=True, help="Module containing the task function"
    )
    parser.add_argument("--function", required=True, help="Name of the task function")

    # Regular job arguments (mutually exclusive with array job arguments)
    parser.add_argument(
        "--args-file", help="Path to the pickled args tuple file (regular jobs)"
    )
    parser.add_argument(
        "--kwargs-file", help="Path to the pickled kwargs dict file (regular jobs)"
    )

    # Array job arguments (mutually exclusive with regular job arguments)
    parser.add_argument(
        "--array-index", type=int, help="Array task index for native SLURM arrays"
    )
    parser.add_argument(
        "--array-items-file", help="Path to pickled array items file (array jobs)"
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
    logger.info("=" * 70)
    logger.info("RUNNER STARTING")
    logger.info(f"Module: {args.module}, Function: {args.function}")
    logger.info(f"Job Dir: {args.job_dir}")
    logger.info("=" * 70)

    # Determine if this is an array job
    is_array_job = args.array_index is not None
    array_task_id_env = os.environ.get("SLURM_ARRAY_TASK_ID")

    if is_array_job:
        logger.info("Starting array task execution (index=%s)", args.array_index)
        logger.debug("Module=%s, Function=%s", args.module, args.function)
        logger.debug("Array index=%s", args.array_index)
        logger.debug("Array items file=%s", args.array_items_file)
        logger.debug("Output file=%s", args.output_file)
        logger.debug("Callbacks file=%s", args.callbacks_file)
        logger.debug("SLURM_ARRAY_TASK_ID=%s", array_task_id_env)
    else:
        logger.info("Starting task execution")
        logger.debug("Module=%s, Function=%s", args.module, args.function)
        logger.debug("Args file=%s", args.args_file)
        logger.debug("Kwargs file=%s", args.kwargs_file)
        logger.debug("Output file=%s", args.output_file)
        logger.debug("Callbacks file=%s", args.callbacks_file)
    logger.debug("Log level=%s", args.loglevel)

    job_context: JobContext = current_job_context()

    # Get job ID, constructing full ID for native array jobs
    # For array jobs: SLURM_ARRAY_JOB_ID=base_id, SLURM_ARRAY_TASK_ID=index
    # We need to construct "base_id_index" to match what Job.id contains
    array_job_id = os.environ.get("SLURM_ARRAY_JOB_ID")
    array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_job_id and array_task_id:
        job_id_env = f"{array_job_id}_{array_task_id}"
    else:
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
        "SLURM_ARRAY_TASK_ID",  # Add array task ID to snapshot
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
        # Load arguments - different logic for array jobs vs regular jobs
        if is_array_job:
            # Array job: load item from array items file by index
            from slurm.array_items import load_array_item

            # Resolve array items file path
            # If it's a relative path, resolve it relative to job_dir
            array_items_path = args.array_items_file
            if not os.path.isabs(array_items_path):
                # Relative path - resolve relative to job directory
                if job_dir:
                    array_items_path = os.path.join(job_dir, array_items_path)
                else:
                    # Fallback to current directory
                    array_items_path = os.path.abspath(array_items_path)

            logger.debug(
                "Loading array item at index %s from %s",
                args.array_index,
                array_items_path,
            )
            logger.debug("Resolved array items path: %s", array_items_path)
            item = load_array_item(array_items_path, args.array_index)

            # Unpack item based on type
            if isinstance(item, dict):
                # Dict: unpack as kwargs
                task_args = ()
                task_kwargs = item
                logger.debug("Loaded dict item as kwargs: %s keys", len(task_kwargs))
            elif isinstance(item, tuple):
                # Tuple: unpack as args
                task_args = item
                task_kwargs = {}
                logger.debug("Loaded tuple item as args: %s elements", len(task_args))
            else:
                # Single value: pass as first arg
                task_args = (item,)
                task_kwargs = {}
                logger.debug("Loaded single item as first arg")
        else:
            # Regular job: load from args/kwargs files
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

        # Resolve JobResultPlaceholder objects by loading their results
        from slurm.task import JobResultPlaceholder

        def resolve_placeholder(value):
            """Recursively resolve JobResultPlaceholder objects."""
            if isinstance(value, JobResultPlaceholder):
                # Load the result from the job directory
                # The job directory follows the pattern: {base}/{task_name}/{timestamp}_{job_id}/
                # We need to find it based on the job_id
                logger.debug(
                    "Resolving JobResultPlaceholder for job_id=%s", value.job_id
                )

                # For now, we'll search for the result file based on job_id
                # This is a simplified approach - in production, you'd want proper metadata tracking
                job_base_dir = os.environ.get(
                    "SLURM_JOBS_DIR", os.path.expanduser("~/slurm_jobs")
                )

                # Search for the result file
                # Pattern: {job_base_dir}/**/*_{job_id}/slurm_job_*_result.pkl
                import glob

                # Search for metadata.json files first (more efficient)
                search_pattern = f"{job_base_dir}/**/metadata.json"
                for metadata_path in glob.glob(search_pattern, recursive=True):
                    try:
                        import json

                        with open(metadata_path, "r") as f:
                            metadata_map = json.load(f)

                        # Check if this metadata contains our job_id
                        if value.job_id in metadata_map:
                            result_dir = os.path.dirname(metadata_path)
                            result_filename = metadata_map[value.job_id]["result_file"]
                            result_path = os.path.join(result_dir, result_filename)

                            logger.debug("Found result file: %s", result_path)
                            with open(result_path, "rb") as f:
                                return pickle.load(f)
                    except Exception as e:
                        logger.warning(
                            "Error reading metadata from %s: %s", metadata_path, e
                        )

                raise FileNotFoundError(
                    f"Could not find result file for job_id={value.job_id}"
                )
            elif isinstance(value, (list, tuple)):
                return type(value)(resolve_placeholder(item) for item in value)
            elif isinstance(value, dict):
                return {k: resolve_placeholder(v) for k, v in value.items()}
            else:
                return value

        # Resolve placeholders in args and kwargs
        task_args = resolve_placeholder(task_args)
        task_kwargs = resolve_placeholder(task_kwargs)

        logger.debug("Resolved Args: %s", task_args)
        logger.debug("Resolved Kwargs: %s", task_kwargs)

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
        except Exception as e:
            logger.warning(f"Callback on_begin_run_job_ctx failed: {e}")

        logger.debug("Importing module %s...", args.module)
        module = importlib.import_module(args.module)
        logger.debug("Getting function %s from module...", args.function)
        func = getattr(module, args.function)

        # If this is a @task decorated function, unwrap it for direct execution
        # The decorator prevents direct calls outside a Cluster context, but we're
        # running inside a SLURM job so we need the underlying function
        if hasattr(func, "unwrapped"):
            logger.debug("Unwrapping @task decorated function for execution")
            func = func.unwrapped

        # Track whether we activated a workflow context
        workflow_context_token = None

        # Check if function wants JobContext
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
        # Check if function wants WorkflowContext (for @workflow functions)
        elif _function_wants_workflow_context(func):
            # Build WorkflowContext for workflow orchestrators
            from pathlib import Path

            from slurm.cluster import Cluster

            # We need to recreate the cluster connection for the workflow
            # The workflow needs to be able to submit jobs
            workflow_job_dir = Path(job_dir) if job_dir else Path.cwd()
            shared_dir = workflow_job_dir / "shared"

            # Create a cluster instance for the workflow to use
            # Try to load from environment variables set by the job script
            slurmfile_path = os.environ.get("SLURM_SDK_SLURMFILE")
            env_name = os.environ.get("SLURM_SDK_ENV")
            packaging_config_b64 = os.environ.get("SLURM_SDK_PACKAGING_CONFIG")
            container_image_env = os.environ.get("CONTAINER_IMAGE")

            # Decode parent packaging configuration if available
            parent_packaging_config: Optional[Dict[str, Any]] = None
            if packaging_config_b64:
                try:
                    import base64

                    config_json = base64.b64decode(packaging_config_b64).decode()
                    parent_packaging_config = json.loads(config_json)
                    logger.debug(
                        f"Loaded parent packaging config from env: {parent_packaging_config}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to decode parent packaging config: {e}")

            # Fallback: when running inside a containerized workflow job, the sbatch
            # script exports CONTAINER_IMAGE. Prefer it to avoid losing the registry
            # prefix for nested submissions.
            if container_image_env:
                if parent_packaging_config is None:
                    parent_packaging_config = {
                        "type": "container",
                        "image": container_image_env,
                        "push": False,
                    }
                elif parent_packaging_config.get(
                    "type"
                ) == "container" and not parent_packaging_config.get("image"):
                    parent_packaging_config = dict(parent_packaging_config)
                    parent_packaging_config["image"] = container_image_env
                    parent_packaging_config["push"] = False

            # Decode pre-built dependency images if available
            prebuilt_images: Optional[Dict[str, str]] = None
            prebuilt_images_b64 = os.environ.get("SLURM_SDK_PREBUILT_IMAGES")
            if prebuilt_images_b64:
                try:
                    import base64

                    images_json = base64.b64decode(prebuilt_images_b64).decode()
                    prebuilt_images = json.loads(images_json)
                    logger.debug(
                        f"Loaded pre-built dependency images from env: {prebuilt_images}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to decode pre-built images: {e}")

            cluster = None
            parent_packaging_type: Optional[str] = None
            if slurmfile_path:
                try:
                    logger.info(
                        f"Loading cluster from SLURM_SDK_SLURMFILE={slurmfile_path}, env={env_name}"
                    )
                    # Check if Slurmfile actually exists before trying to load it
                    if not os.path.exists(slurmfile_path):
                        logger.warning(f"Slurmfile does not exist at: {slurmfile_path}")
                        logger.info("Checking SLURM_SDK_SLURMFILE env var...")
                        slurmfile_path = os.environ.get("SLURM_SDK_SLURMFILE")
                        if slurmfile_path and os.path.exists(slurmfile_path):
                            logger.info(
                                f"Found Slurmfile at SLURM_SDK_SLURMFILE: {slurmfile_path}"
                            )
                        else:
                            logger.warning(
                                "Could not find Slurmfile, skipping cluster creation"
                            )
                            slurmfile_path = None

                    if slurmfile_path:
                        logger.info("Calling Cluster.from_env()...")
                        # Use a timeout for cluster initialization
                        import signal

                        def timeout_handler(signum, frame):
                            raise TimeoutError(
                                "Cluster.from_env() timed out after 30 seconds"
                            )

                        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(30)  # 30 second timeout
                        try:
                            cluster = Cluster.from_env(slurmfile_path, env=env_name)
                            signal.alarm(0)  # Cancel alarm
                            logger.info("Cluster loaded successfully")
                        except TimeoutError as e:
                            logger.error(f"Cluster initialization timed out: {e}")
                            cluster = None
                        except Exception as e:
                            logger.error(
                                f"Error initializing cluster from {slurmfile_path}: {e}",
                                exc_info=True,
                            )
                            cluster = None
                        finally:
                            signal.signal(signal.SIGALRM, old_handler)

                    # Use parent packaging config from env var if available,
                    # otherwise fall back to cluster.packaging_defaults
                    effective_packaging_config = (
                        parent_packaging_config or cluster.packaging_defaults
                    )
                    if effective_packaging_config:
                        parent_packaging_type = effective_packaging_config.get("type")

                    # For nested workflow tasks, reuse the parent workflow's container image
                    # Remove dockerfile/context to prevent rebuilding; set explicit image reference
                    if (
                        effective_packaging_config
                        and effective_packaging_config.get("type") == "container"
                    ):
                        pkg = dict(effective_packaging_config)

                        # Construct the full image reference if not already present
                        if not pkg.get("image"):
                            registry = pkg.get("registry", "").rstrip("/")
                            name = pkg.get("name", "")
                            tag = pkg.get("tag", "latest")

                            if registry and name:
                                image_ref = f"{registry}/{name.lstrip('/')}:{tag}"
                            elif name:
                                image_ref = f"{name}:{tag}"
                            else:
                                image_ref = None

                            if image_ref:
                                pkg["image"] = image_ref
                                logger.debug(
                                    f"Constructed image reference: {image_ref}"
                                )
                            elif container_image_env:
                                pkg["image"] = container_image_env
                                logger.debug(
                                    "Using CONTAINER_IMAGE for nested tasks: %s",
                                    container_image_env,
                                )

                        # Remove build-time fields so child tasks use the image directly
                        for key in ["dockerfile", "context", "registry", "name", "tag"]:
                            pkg.pop(key, None)
                        pkg["push"] = False
                        cluster.packaging_defaults = pkg
                        logger.debug(
                            "Configured nested tasks to reuse parent container image"
                        )
                    else:
                        # If not using containers, use 'inherit' packaging
                        # Child tasks will read .slurm_environment.json to activate parent's venv
                        cluster.packaging_defaults = {
                            "type": "inherit",
                            "parent_job_dir": job_dir,
                        }
                        logger.info(
                            f"Configured child tasks to inherit environment from {job_dir}"
                        )

                    # Store pre-built dependency images on cluster
                    if prebuilt_images:
                        cluster._prebuilt_dependency_images = prebuilt_images
                        logger.debug(
                            f"Stored {len(prebuilt_images)} pre-built dependency images on cluster"
                        )
                except Exception as e:
                    logger.warning(f"Could not load cluster from {slurmfile_path}: {e}")

            # Fallback: try without path
            if cluster is None:
                try:
                    logger.debug("Trying to load cluster from discovered Slurmfile")
                    cluster = Cluster.from_env(env=env_name)
                    if cluster.packaging_defaults and parent_packaging_type is None:
                        parent_packaging_type = cluster.packaging_defaults.get("type")

                    # For nested workflow tasks, reuse the parent workflow's container image
                    if (
                        cluster.packaging_defaults
                        and cluster.packaging_defaults.get("type") == "container"
                    ):
                        pkg = dict(cluster.packaging_defaults)

                        # Construct the full image reference if not already present
                        if not pkg.get("image"):
                            registry = pkg.get("registry", "").rstrip("/")
                            name = pkg.get("name", "")
                            tag = pkg.get("tag", "latest")

                            if registry and name:
                                image_ref = f"{registry}/{name.lstrip('/')}:{tag}"
                            elif name:
                                image_ref = f"{name}:{tag}"
                            else:
                                image_ref = None

                            if image_ref:
                                pkg["image"] = image_ref
                                logger.debug(
                                    f"Constructed image reference: {image_ref}"
                                )
                            elif container_image_env:
                                pkg["image"] = container_image_env
                                logger.debug(
                                    "Using CONTAINER_IMAGE for nested tasks: %s",
                                    container_image_env,
                                )

                        # Remove build-time fields so child tasks use the image directly
                        for key in ["dockerfile", "context", "registry", "name", "tag"]:
                            pkg.pop(key, None)
                        pkg["push"] = False
                        cluster.packaging_defaults = pkg
                        logger.debug(
                            "Configured nested tasks to reuse parent container image"
                        )
                    else:
                        # Non-container packaging: use inherit strategy
                        cluster.packaging_defaults = {
                            "type": "inherit",
                            "parent_job_dir": job_dir,
                        }
                        logger.info(
                            f"Configured child tasks to inherit environment from {job_dir}"
                        )
                except Exception as e:
                    logger.warning(f"Could not load cluster from Slurmfile: {e}")

            if cluster is None:
                logger.error(
                    "CRITICAL: Could not create Cluster instance for workflow. "
                    "This is required for workflows that submit child tasks. "
                    "Possible causes: Slurmfile not found, connection timeout, or invalid environment. "
                    "Workflow execution will fail if it attempts to submit any tasks."
                )

            logger.info("Creating WorkflowContext...")
            workflow_context = WorkflowContext(
                cluster=cluster,
                workflow_job_id=job_id_env or "unknown",
                workflow_job_dir=workflow_job_dir,
                shared_dir=shared_dir,
                local_mode=False,
            )
            logger.info("WorkflowContext created successfully")

            logger.info("Binding workflow context to function...")
            task_args, task_kwargs, injected = _bind_workflow_context(
                func, task_args, task_kwargs, workflow_context
            )
            logger.info(f"Workflow context bound (injected={injected})")
            if injected:
                logger.debug(
                    "Injected WorkflowContext into %s.%s",
                    args.module,
                    args.function,
                )
            else:
                logger.debug(
                    "WorkflowContext requested by %s.%s but argument already provided",
                    args.module,
                    args.function,
                )

            # Write environment metadata for child tasks to inherit
            # This must happen BEFORE child tasks are submitted
            # The packaging_type should reflect the PARENT's actual environment (wheel/container),
            # not what children will use (inherit)
            logger.info("Determining parent packaging type...")
            if parent_packaging_type not in {"wheel", "container"}:
                parent_packaging_type = (
                    "wheel"
                    if os.environ.get("VIRTUAL_ENV")
                    else "container"
                    if os.environ.get("SINGULARITY_NAME")
                    or os.environ.get("SLURM_CONTAINER_IMAGE")
                    else "none"
                )
            logger.info(f"Parent packaging type: {parent_packaging_type}")
            logger.info("Writing environment metadata...")
            _write_environment_metadata(
                job_dir=str(workflow_job_dir),
                packaging_type=parent_packaging_type,
                job_id=job_id_env,
                workflow_name=args.function,
                pre_submission_id=args.pre_submission_id,
            )
            logger.info("Environment metadata written")

            # Activate the cluster context for the workflow execution
            # This allows tasks called within the workflow to submit jobs
            from slurm.context import _set_active_context

            logger.debug("Activating cluster context for workflow execution")
            workflow_context_token = _set_active_context(workflow_context)

            # Emit workflow begin event after context is set up
            logger.debug("Calling on_workflow_begin callbacks...")
            try:
                from pathlib import Path

                workflow_begin_ctx = WorkflowCallbackContext(
                    workflow_job_id=job_id_env or "unknown",
                    workflow_job_dir=Path(job_dir) if job_dir else Path.cwd(),
                    workflow_name=args.function,
                    workflow_context=workflow_context,
                    timestamp=time.time(),
                    cluster=None,  # Cluster not serializable/available in runner
                )
                _run_callbacks(callbacks, "on_workflow_begin_ctx", workflow_begin_ctx)
            except Exception as e:
                logger.warning(f"Error calling workflow begin callbacks: {e}")

        logger.info("Executing task")
        logger.info(f"Task args: {task_args}")
        logger.info(f"Task kwargs: {task_kwargs}")

        # Execute and track result/exception for workflow end event
        task_result = None
        task_exception = None
        try:
            logger.info("About to execute function...")
            result = func(*task_args, **task_kwargs)
            logger.info(f"Function returned: {result}")
            task_result = result
        except Exception as e:
            task_exception = e
            logger.error(f"Function raised exception: {e}", exc_info=True)
            raise
        finally:
            # Emit workflow end event if we activated workflow context
            if workflow_context_token is not None:
                logger.debug("Calling on_workflow_end callbacks...")
                try:
                    from pathlib import Path

                    workflow_end_ctx = WorkflowCallbackContext(
                        workflow_job_id=job_id_env or "unknown",
                        workflow_job_dir=Path(job_dir) if job_dir else Path.cwd(),
                        workflow_name=args.function,
                        workflow_context=workflow_context,
                        timestamp=time.time(),
                        result=task_result,
                        exception=task_exception,
                        cluster=None,
                    )
                    _run_callbacks(callbacks, "on_workflow_end_ctx", workflow_end_ctx)
                except Exception as e:
                    logger.warning(f"Error calling workflow end callbacks: {e}")

            # Deactivate cluster context if it was activated for a workflow
            if workflow_context_token is not None:
                from slurm.context import _reset_active_context

                _reset_active_context(workflow_context_token)
                logger.debug("Deactivated cluster context after workflow execution")

                # Explicitly close SSH connections to prevent hanging
                # The cluster's SSH backend holds connections that can prevent process exit
                # Use a timeout to prevent the close operation from hanging indefinitely
                if cluster is not None and hasattr(cluster, "backend"):
                    try:
                        # Manually trigger SSH backend cleanup with timeout
                        if (
                            hasattr(cluster.backend, "client")
                            and cluster.backend.client
                        ):
                            logger.debug(
                                "Closing SSH connections from workflow cluster"
                            )
                            # Close SFTP connection with timeout
                            if (
                                hasattr(cluster.backend, "sftp")
                                and cluster.backend.sftp
                            ):
                                try:
                                    import threading

                                    def close_sftp():
                                        try:
                                            cluster.backend.sftp.close()
                                        except Exception:
                                            pass

                                    sftp_thread = threading.Thread(
                                        target=close_sftp, daemon=True
                                    )
                                    sftp_thread.start()
                                    sftp_thread.join(timeout=2.0)  # 2 second timeout
                                except Exception as e:
                                    logger.debug(f"Error closing SFTP: {e}")
                            # Close SSH client with timeout
                            try:
                                import threading

                                def close_ssh():
                                    try:
                                        cluster.backend.client.close()
                                    except Exception:
                                        pass

                                ssh_thread = threading.Thread(
                                    target=close_ssh, daemon=True
                                )
                                ssh_thread.start()
                                ssh_thread.join(timeout=2.0)  # 2 second timeout
                            except Exception as e:
                                logger.debug(f"Error closing SSH client: {e}")
                            logger.debug("SSH connections closed (or timeout)")
                    except Exception as e:
                        logger.warning(f"Error cleaning up cluster backend: {e}")
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

        # Create metadata.json for JobResultPlaceholder resolution
        # Array jobs share a directory, so use file locking to prevent concurrent write conflicts
        metadata_path = os.path.join(output_dir or ".", "metadata.json")
        try:
            import fcntl

            lock_file = metadata_path + ".lock"
            max_retries = 10
            retry_delay = 0.1

            for attempt in range(max_retries):
                try:
                    lock_fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY, 0o644)
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                    try:
                        metadata_map = {}
                        if os.path.exists(metadata_path):
                            try:
                                with open(metadata_path, "r") as f:
                                    metadata_map = json.load(f)
                            except Exception as read_err:
                                logger.warning(
                                    "Could not read metadata, starting fresh: %s",
                                    read_err,
                                )

                        metadata_map[job_id_env] = {
                            "result_file": os.path.basename(args.output_file),
                            "timestamp": end_time,
                        }

                        # Atomic write: temp file + rename prevents partial reads
                        temp_path = metadata_path + ".tmp"
                        with open(temp_path, "w") as f:
                            json.dump(metadata_map, f, indent=2)
                        os.rename(temp_path, metadata_path)

                        logger.debug(
                            "Metadata saved to %s (job_id=%s)",
                            metadata_path,
                            job_id_env,
                        )
                    finally:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                        os.close(lock_fd)
                    break

                except (IOError, OSError) as lock_err:
                    if attempt < max_retries - 1:
                        logger.debug(
                            "Metadata lock busy, retrying in %s seconds...", retry_delay
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        raise Exception(
                            f"Could not acquire metadata lock after {max_retries} attempts"
                        ) from lock_err

        except Exception as e:
            logger.warning("Failed to save metadata: %s", e)

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
        except Exception as e:
            logger.warning(f"Callback on_completed_ctx (success) failed: {e}")
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
        except Exception as exc:
            logger.warning(f"Callback on_end_run_job_ctx (failure) failed: {exc}")

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
        except Exception as exc:
            logger.warning(f"Callback on_completed_ctx (failure) failed: {exc}")

        logger.error("=" * 70)
        logger.error("RUNNER EXITING WITH FAILURE")
        logger.error("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
