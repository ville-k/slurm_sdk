"""Workflow context building utilities for the runner.

This module handles creating WorkflowContext instances for workflow functions,
including cluster initialization and configuration.
"""

import base64
import json
import logging
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("slurm.runner")

# Default timeout for cluster initialization in seconds
DEFAULT_CLUSTER_TIMEOUT = 30


@dataclass
class ClusterConfig:
    """Configuration for cluster creation extracted from environment.

    This dataclass holds all the configuration needed to create a Cluster
    instance for workflow execution.
    """

    slurmfile_path: Optional[str] = None
    env_name: Optional[str] = None
    parent_packaging_config: Optional[Dict[str, Any]] = None
    container_image: Optional[str] = None
    prebuilt_images: Optional[Dict[str, str]] = None
    job_dir: Optional[str] = None


def extract_cluster_config_from_env(job_dir: Optional[str] = None) -> ClusterConfig:
    """Extract cluster configuration from environment variables.

    Reads various SLURM_SDK_* environment variables to build a ClusterConfig
    that can be used to initialize a cluster for workflow execution.

    Args:
        job_dir: The job directory path.

    Returns:
        ClusterConfig with extracted configuration.
    """
    slurmfile_path = os.environ.get("SLURM_SDK_SLURMFILE")
    env_name = os.environ.get("SLURM_SDK_ENV")
    packaging_config_b64 = os.environ.get("SLURM_SDK_PACKAGING_CONFIG")
    container_image = os.environ.get("CONTAINER_IMAGE")
    prebuilt_images_b64 = os.environ.get("SLURM_SDK_PREBUILT_IMAGES")

    # Decode parent packaging configuration
    parent_packaging_config: Optional[Dict[str, Any]] = None
    if packaging_config_b64:
        try:
            config_json = base64.b64decode(packaging_config_b64).decode()
            parent_packaging_config = json.loads(config_json)
            logger.debug(
                f"Loaded parent packaging config from env: {parent_packaging_config}"
            )
        except Exception as e:
            logger.warning(f"Failed to decode parent packaging config: {e}")

    # Apply container image fallback
    if container_image:
        if parent_packaging_config is None:
            parent_packaging_config = {
                "type": "container",
                "image": container_image,
                "push": False,
            }
        elif parent_packaging_config.get(
            "type"
        ) == "container" and not parent_packaging_config.get("image"):
            parent_packaging_config = dict(parent_packaging_config)
            parent_packaging_config["image"] = container_image
            parent_packaging_config["push"] = False

    # Decode pre-built dependency images
    prebuilt_images: Optional[Dict[str, str]] = None
    if prebuilt_images_b64:
        try:
            images_json = base64.b64decode(prebuilt_images_b64).decode()
            prebuilt_images = json.loads(images_json)
            logger.debug(
                f"Loaded pre-built dependency images from env: {prebuilt_images}"
            )
        except Exception as e:
            logger.warning(f"Failed to decode pre-built images: {e}")

    return ClusterConfig(
        slurmfile_path=slurmfile_path,
        env_name=env_name,
        parent_packaging_config=parent_packaging_config,
        container_image=container_image,
        prebuilt_images=prebuilt_images,
        job_dir=job_dir,
    )


def create_cluster_from_config(
    config: ClusterConfig,
    timeout: int = DEFAULT_CLUSTER_TIMEOUT,
) -> tuple[Optional[Any], Optional[str]]:
    """Create a Cluster instance from configuration.

    Attempts to create a Cluster using the provided configuration,
    with timeout handling to prevent hanging on connection issues.

    Args:
        config: The cluster configuration.
        timeout: Timeout in seconds for cluster initialization.

    Returns:
        Tuple of (cluster, parent_packaging_type) where cluster may be None
        if creation failed.
    """

    cluster = None
    parent_packaging_type: Optional[str] = None

    # Try with explicit slurmfile path first
    if config.slurmfile_path:
        cluster, parent_packaging_type = _create_cluster_with_path(config, timeout)

    # Fallback: try without path
    if cluster is None:
        cluster, parent_packaging_type = _create_cluster_from_discovery(config, timeout)

    if cluster is None:
        logger.error(
            "CRITICAL: Could not create Cluster instance for workflow. "
            "This is required for workflows that submit child tasks. "
            "Possible causes: Slurmfile not found, connection timeout, or invalid environment. "
            "Workflow execution will fail if it attempts to submit any tasks."
        )

    return cluster, parent_packaging_type


def _create_cluster_with_path(
    config: ClusterConfig,
    timeout: int,
) -> tuple[Optional[Any], Optional[str]]:
    """Create cluster using explicit slurmfile path.

    Args:
        config: The cluster configuration.
        timeout: Timeout in seconds.

    Returns:
        Tuple of (cluster, parent_packaging_type).
    """

    slurmfile_path = config.slurmfile_path
    parent_packaging_type: Optional[str] = None

    try:
        logger.info(
            f"Loading cluster from SLURM_SDK_SLURMFILE={slurmfile_path}, env={config.env_name}"
        )

        # Check if Slurmfile exists
        if not os.path.exists(slurmfile_path):
            logger.warning(f"Slurmfile does not exist at: {slurmfile_path}")
            # Try env var again as fallback
            slurmfile_path = os.environ.get("SLURM_SDK_SLURMFILE")
            if slurmfile_path and os.path.exists(slurmfile_path):
                logger.info(f"Found Slurmfile at SLURM_SDK_SLURMFILE: {slurmfile_path}")
            else:
                logger.warning("Could not find Slurmfile, skipping cluster creation")
                return None, None

        logger.info("Calling Cluster.from_env()...")
        cluster = _create_cluster_with_timeout(slurmfile_path, config.env_name, timeout)

        if cluster is None:
            return None, None

        logger.info("Cluster loaded successfully")

        # Configure packaging
        cluster, parent_packaging_type = _configure_cluster_packaging(cluster, config)

        return cluster, parent_packaging_type

    except Exception as e:
        logger.warning(f"Could not load cluster from {slurmfile_path}: {e}")
        return None, None


def _create_cluster_from_discovery(
    config: ClusterConfig,
    timeout: int,
) -> tuple[Optional[Any], Optional[str]]:
    """Create cluster using automatic discovery.

    Args:
        config: The cluster configuration.
        timeout: Timeout in seconds.

    Returns:
        Tuple of (cluster, parent_packaging_type).
    """

    try:
        logger.debug("Trying to load cluster from discovered Slurmfile")
        cluster = _create_cluster_with_timeout(None, config.env_name, timeout)

        if cluster is None:
            return None, None

        # Configure packaging
        cluster, parent_packaging_type = _configure_cluster_packaging(cluster, config)

        return cluster, parent_packaging_type

    except Exception as e:
        logger.warning(f"Could not load cluster from Slurmfile: {e}")
        return None, None


def _create_cluster_with_timeout(
    slurmfile_path: Optional[str],
    env_name: Optional[str],
    timeout: int,
) -> Optional[Any]:
    """Create a Cluster with timeout handling.

    Args:
        slurmfile_path: Path to the Slurmfile (or None for discovery).
        env_name: Environment name.
        timeout: Timeout in seconds.

    Returns:
        Cluster instance or None if creation failed/timed out.
    """
    from slurm.cluster import Cluster

    def timeout_handler(signum, frame):
        raise TimeoutError(f"Cluster.from_env() timed out after {timeout} seconds")

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    try:
        if slurmfile_path:
            cluster = Cluster.from_env(slurmfile_path, env=env_name)
        else:
            cluster = Cluster.from_env(env=env_name)
        signal.alarm(0)  # Cancel alarm
        return cluster
    except TimeoutError as e:
        logger.error(f"Cluster initialization timed out: {e}")
        return None
    except Exception as e:
        logger.error(
            f"Error initializing cluster: {e}",
            exc_info=True,
        )
        return None
    finally:
        signal.signal(signal.SIGALRM, old_handler)


def _configure_cluster_packaging(
    cluster: Any,
    config: ClusterConfig,
) -> tuple[Any, Optional[str]]:
    """Configure cluster packaging for nested workflow tasks.

    Args:
        cluster: The Cluster instance.
        config: The cluster configuration.

    Returns:
        Tuple of (cluster, parent_packaging_type).
    """
    parent_packaging_type: Optional[str] = None

    # Determine effective packaging config
    effective_packaging_config = (
        config.parent_packaging_config or cluster.packaging_defaults
    )

    if effective_packaging_config:
        parent_packaging_type = effective_packaging_config.get("type")

    # Configure packaging for nested tasks
    if (
        effective_packaging_config
        and effective_packaging_config.get("type") == "container"
    ):
        pkg = dict(effective_packaging_config)

        # Construct full image reference if not present
        if not pkg.get("image"):
            image_ref = _construct_image_reference(pkg, config.container_image)
            if image_ref:
                pkg["image"] = image_ref
                logger.debug(f"Constructed image reference: {image_ref}")

        # Remove build-time fields so child tasks use the image directly
        for key in ["dockerfile", "context", "registry", "name", "tag"]:
            pkg.pop(key, None)
        pkg["push"] = False
        cluster.packaging_defaults = pkg
        logger.debug("Configured nested tasks to reuse parent container image")
    else:
        # Non-container packaging: use inherit strategy
        cluster.packaging_defaults = {
            "type": "inherit",
            "parent_job_dir": config.job_dir,
        }
        logger.info(
            f"Configured child tasks to inherit environment from {config.job_dir}"
        )

    # Store pre-built dependency images on cluster
    if config.prebuilt_images:
        cluster._prebuilt_dependency_images = config.prebuilt_images
        logger.debug(
            f"Stored {len(config.prebuilt_images)} pre-built dependency images on cluster"
        )

    return cluster, parent_packaging_type


def _construct_image_reference(
    pkg_config: Dict[str, Any],
    fallback_image: Optional[str] = None,
) -> Optional[str]:
    """Construct a full container image reference from config.

    Args:
        pkg_config: The packaging configuration dict.
        fallback_image: Fallback image to use if construction fails.

    Returns:
        Image reference string or None.
    """
    registry = pkg_config.get("registry", "").rstrip("/")
    name = pkg_config.get("name", "")
    tag = pkg_config.get("tag", "latest")

    if registry and name:
        return f"{registry}/{name.lstrip('/')}:{tag}"
    elif name:
        return f"{name}:{tag}"
    elif fallback_image:
        logger.debug(f"Using fallback image for nested tasks: {fallback_image}")
        return fallback_image
    return None


def create_workflow_context(
    cluster: Optional[Any],
    job_id: Optional[str],
    job_dir: Optional[str],
) -> Any:
    """Create a WorkflowContext for workflow execution.

    Args:
        cluster: The Cluster instance (may be None if creation failed).
        job_id: The SLURM job ID.
        job_dir: The job directory path.

    Returns:
        WorkflowContext instance.
    """
    from slurm.workflow import WorkflowContext

    workflow_job_dir = Path(job_dir) if job_dir else Path.cwd()
    shared_dir = workflow_job_dir / "shared"

    logger.info("Creating WorkflowContext...")
    workflow_context = WorkflowContext(
        cluster=cluster,
        workflow_job_id=job_id or "unknown",
        workflow_job_dir=workflow_job_dir,
        shared_dir=shared_dir,
        local_mode=False,
    )
    logger.info("WorkflowContext created successfully")

    return workflow_context


def determine_parent_packaging_type(
    parent_packaging_type: Optional[str] = None,
) -> str:
    """Determine the parent's packaging type from environment.

    This is used to record the actual execution environment of the parent
    workflow, not what children will use (which is typically 'inherit').

    Args:
        parent_packaging_type: Previously determined type (from cluster config).

    Returns:
        Packaging type string: 'wheel', 'container', or 'none'.
    """
    if parent_packaging_type in {"wheel", "container"}:
        return parent_packaging_type

    # Detect from environment
    if os.environ.get("VIRTUAL_ENV"):
        return "wheel"
    elif os.environ.get("SINGULARITY_NAME") or os.environ.get("SLURM_CONTAINER_IMAGE"):
        return "container"
    else:
        return "none"


def activate_workflow_context(workflow_context: Any) -> Any:
    """Activate the workflow context for task submission.

    This allows tasks called within the workflow to submit jobs
    using the workflow's cluster.

    Args:
        workflow_context: The WorkflowContext to activate.

    Returns:
        Context token for later deactivation.
    """
    from slurm.context import _set_active_context

    logger.debug("Activating cluster context for workflow execution")
    return _set_active_context(workflow_context)


@dataclass
class WorkflowSetupResult:
    """Result of setting up workflow execution environment.

    Contains all the components needed for workflow execution.
    """

    cluster: Optional[Any]
    workflow_context: Any
    parent_packaging_type: str
    context_token: Any


def setup_workflow_execution(
    job_id: Optional[str],
    job_dir: Optional[str],
    timeout: int = DEFAULT_CLUSTER_TIMEOUT,
) -> WorkflowSetupResult:
    """Set up the complete workflow execution environment.

    This is the main orchestration function that:
    1. Extracts cluster configuration from environment
    2. Creates the cluster instance
    3. Creates the WorkflowContext
    4. Activates the context for task submission

    Args:
        job_id: The SLURM job ID.
        job_dir: The job directory path.
        timeout: Timeout for cluster initialization.

    Returns:
        WorkflowSetupResult with all components.
    """
    # Extract configuration from environment
    config = extract_cluster_config_from_env(job_dir=job_dir)

    # Create cluster
    cluster, parent_packaging_type = create_cluster_from_config(config, timeout)

    # Determine final packaging type
    final_packaging_type = determine_parent_packaging_type(parent_packaging_type)
    logger.info(f"Parent packaging type: {final_packaging_type}")

    # Create WorkflowContext
    workflow_context = create_workflow_context(cluster, job_id, job_dir)

    # Activate context
    context_token = activate_workflow_context(workflow_context)

    return WorkflowSetupResult(
        cluster=cluster,
        workflow_context=workflow_context,
        parent_packaging_type=final_packaging_type,
        context_token=context_token,
    )


def cleanup_cluster_connections(
    cluster: Optional[Any],
    timeout: float = 2.0,
) -> None:
    """Clean up SSH connections from cluster to prevent hanging.

    The cluster's SSH backend holds connections that can prevent process exit.
    This function closes them with a timeout to prevent indefinite blocking.

    Args:
        cluster: The Cluster instance (may be None).
        timeout: Timeout in seconds for each close operation.
    """
    import threading

    if cluster is None or not hasattr(cluster, "backend"):
        return

    try:
        backend = cluster.backend
        if not hasattr(backend, "client") or not backend.client:
            return

        logger.debug("Closing SSH connections from workflow cluster")

        # Close SFTP connection with timeout
        if hasattr(backend, "sftp") and backend.sftp:
            try:

                def close_sftp():
                    try:
                        backend.sftp.close()
                    except Exception:
                        pass

                sftp_thread = threading.Thread(target=close_sftp, daemon=True)
                sftp_thread.start()
                sftp_thread.join(timeout=timeout)
            except Exception as e:
                logger.debug(f"Error closing SFTP: {e}")

        # Close SSH client with timeout
        try:

            def close_ssh():
                try:
                    backend.client.close()
                except Exception:
                    pass

            ssh_thread = threading.Thread(target=close_ssh, daemon=True)
            ssh_thread.start()
            ssh_thread.join(timeout=timeout)
        except Exception as e:
            logger.debug(f"Error closing SSH client: {e}")

        logger.debug("SSH connections closed (or timeout)")

    except Exception as e:
        logger.warning(f"Error cleaning up cluster backend: {e}")


def deactivate_workflow_context(context_token: Any) -> None:
    """Deactivate the workflow context.

    Args:
        context_token: The token returned from activate_workflow_context.
    """
    from slurm.context import _reset_active_context

    if context_token is not None:
        try:
            _reset_active_context(context_token)
            logger.debug("Workflow context deactivated")
        except Exception as e:
            logger.debug(f"Error deactivating workflow context: {e}")


def teardown_workflow_execution(
    setup_result: WorkflowSetupResult,
    connection_timeout: float = 2.0,
) -> None:
    """Tear down the workflow execution environment.

    Cleans up all resources created during workflow setup.

    Args:
        setup_result: The result from setup_workflow_execution.
        connection_timeout: Timeout for closing SSH connections.
    """
    # Deactivate context first
    deactivate_workflow_context(setup_result.context_token)

    # Clean up SSH connections
    cleanup_cluster_connections(setup_result.cluster, timeout=connection_timeout)

    logger.info("Workflow execution cleanup complete")


__all__ = [
    "ClusterConfig",
    "extract_cluster_config_from_env",
    "create_cluster_from_config",
    "create_workflow_context",
    "determine_parent_packaging_type",
    "activate_workflow_context",
    "setup_workflow_execution",
    "WorkflowSetupResult",
    "cleanup_cluster_connections",
    "deactivate_workflow_context",
    "teardown_workflow_execution",
    "DEFAULT_CLUSTER_TIMEOUT",
]
