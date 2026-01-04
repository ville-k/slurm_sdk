"""
Inherit packaging strategy: child tasks inherit parent workflow's environment.

Design goals:
- Explicit inheritance via metadata file
- Cross-node compatibility (shared filesystem)
- Clear error messages for debugging
- Works with both wheel and container packaging
"""

import json
import logging
import shlex
from pathlib import Path

# Avoid circular imports for type hints
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from ..errors import PackagingError
from .base import PackagingStrategy

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..task import SlurmTask


logger = logging.getLogger(__name__)

METADATA_FILENAME = ".slurm_environment.json"


class InheritPackagingStrategy(PackagingStrategy):
    """
    Strategy that inherits execution environment from parent workflow.

    This strategy assumes:
    1. A parent workflow has created a .slurm_environment.json metadata file
    2. The job directory is accessible via shared filesystem
    3. The parent environment (venv or container) is still available
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the inherit packaging strategy.

        Args:
            config: Configuration dictionary
                - parent_job_dir: Path to parent workflow's job directory (required)
        """
        super().__init__(config)
        self.parent_job_dir = self.config.get("parent_job_dir")
        self.parent_metadata: Optional[Dict[str, Any]] = None

        if not self.parent_job_dir:
            raise PackagingError(
                "InheritPackagingStrategy requires 'parent_job_dir' in config.\n"
                "This is typically set automatically by the workflow runner."
            )

    def prepare(
        self, task: Union["SlurmTask", Callable], cluster: "Cluster"
    ) -> Dict[str, Any]:
        """
        Load parent environment metadata.

        Args:
            task: The task to package
            cluster: The cluster to submit to

        Returns:
            Dict with status and parent metadata information
        """
        metadata_path = Path(self.parent_job_dir) / METADATA_FILENAME

        logger.debug(f"Loading parent environment metadata from {metadata_path}")

        metadata_content: Optional[str] = None

        try:
            metadata_content = metadata_path.read_text(encoding="utf-8")
            logger.debug(
                "Loaded parent environment metadata from local filesystem: %s",
                metadata_path,
            )
        except Exception as exc:
            logger.debug(
                "Local metadata read failed (%s). Attempting backend fetch.",
                exc,
            )

        if metadata_content is None:
            try:
                metadata_content = cluster.backend.read_file(str(metadata_path))
                logger.debug(
                    "Loaded parent environment metadata via backend read: %s",
                    metadata_path,
                )
            except FileNotFoundError:
                raise PackagingError(
                    f"Parent environment metadata not found at {metadata_path}.\n"
                    f"\n"
                    f"This usually means:\n"
                    f"  1. The parent workflow job failed before creating metadata\n"
                    f"  2. The parent_job_dir path is incorrect: {self.parent_job_dir}\n"
                    f"  3. The shared filesystem is not mounted correctly\n"
                    f"\n"
                    f"Expected metadata at: {metadata_path}"
                ) from None
            except Exception as exc:
                raise PackagingError(
                    f"Unexpected error reading metadata from {metadata_path}: {exc}"
                ) from exc

        try:
            self.parent_metadata = json.loads(metadata_content)
        except json.JSONDecodeError as exc:
            raise PackagingError(
                f"Invalid metadata JSON in {metadata_path}: {exc}\n"
                f"The metadata file may be corrupted."
            ) from exc

        logger.debug(
            f"Loaded metadata: packaging_type={self.parent_metadata.get('packaging_type')}"
        )

        # Validate schema
        required_keys = ["packaging_type", "environment"]
        missing_keys = [k for k in required_keys if k not in self.parent_metadata]

        if missing_keys:
            raise PackagingError(
                f"Invalid metadata schema. Missing keys: {missing_keys}"
            )

        # Validate version if present
        metadata_version = self.parent_metadata.get("version", "1.0")
        if metadata_version != "1.0":
            logger.warning(f"Metadata version {metadata_version} may not be compatible")

        return {
            "status": "success",
            "message": f"Will inherit from parent: {self.parent_job_dir}",
            "parent_metadata": self.parent_metadata,
            "parent_packaging_type": self.parent_metadata["packaging_type"],
        }

    def generate_setup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate commands to activate parent environment.

        Args:
            task: The task to set up
            job_id: The SLURM job ID
            job_dir: The job directory

        Returns:
            List of shell commands for environment setup
        """
        if not self.parent_metadata:
            raise RuntimeError(
                "prepare() must be called before generate_setup_commands(). "
                "This is a bug in the SDK - please report it."
            )

        commands = []
        env = self.parent_metadata["environment"]
        packaging_type = self.parent_metadata["packaging_type"]
        parent_info = self.parent_metadata.get("parent_job", {})

        commands.append(
            f"echo '--- Inheriting Parent Environment (packaging_type={packaging_type}) ---'"
        )
        commands.append(f"echo 'Parent job dir: {self.parent_job_dir}'")

        if parent_info:
            workflow_name = parent_info.get("workflow_name", "unknown")
            slurm_job_id = parent_info.get("slurm_job_id", "unknown")
            commands.append(
                f"echo 'Parent workflow: {workflow_name} (SLURM job {slurm_job_id})'"
            )

        if packaging_type == "wheel":
            venv_path = env.get("venv_path")
            python_exec = env.get("python_executable")

            if not venv_path or not python_exec:
                raise PackagingError(
                    "Invalid metadata: wheel packaging requires venv_path and python_executable"
                )

            commands.append(f"echo 'Parent venv: {venv_path}'")
            commands.append("echo 'Activating parent venv...'")
            commands.append(f"source {shlex.quote(venv_path)}/bin/activate")
            commands.append(f"export PY_EXEC={shlex.quote(python_exec)}")
            commands.append('echo "Activated venv: $VIRTUAL_ENV"')
            commands.append('echo "Python executable: $PY_EXEC"')

        elif packaging_type == "container":
            # When inheriting from container, we're already running in the container
            # Just set the Python executable
            python_exec = env.get("python_executable")

            if not python_exec:
                # Fallback to 'python' if not specified
                python_exec = "python"
                logger.warning(
                    "Container metadata missing python_executable, using 'python'"
                )

            commands.append(
                f"echo 'Parent container image: {env.get('container_image', 'unknown')}'"
            )
            commands.append(f"export PY_EXEC={shlex.quote(python_exec)}")
            commands.append('echo "Python executable: $PY_EXEC"')

        else:
            raise PackagingError(
                f"Cannot inherit from unsupported packaging type: {packaging_type}\n"
                f"Supported types: wheel, container"
            )

        commands.append("echo '--- Environment Inheritance Complete ---'")

        return commands

    def generate_cleanup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Generate cleanup commands.

        For inherited environments, we don't own the venv/container,
        so we just deactivate (if applicable) without removing anything.

        Args:
            task: The task to clean up after
            job_id: The SLURM job ID
            job_dir: The job directory

        Returns:
            List of shell commands for cleanup
        """
        if not self.parent_metadata:
            logger.warning(
                "Warning: Cannot generate cleanup commands, metadata missing."
            )
            return []

        commands = []
        packaging_type = self.parent_metadata["packaging_type"]

        commands.append(
            f"echo '--- Cleaning Up Inherited Environment (packaging_type={packaging_type}) ---'"
        )

        if packaging_type == "wheel":
            # Deactivate venv if active, but don't delete it (parent owns it)
            commands.append("echo 'Deactivating virtualenv (if active)...'")
            commands.append("type deactivate > /dev/null 2>&1 && deactivate || true")
            commands.append(
                "echo 'Note: Parent venv not removed (owned by parent workflow)'"
            )

        elif packaging_type == "container":
            # Nothing to clean up for containers
            commands.append("echo 'No cleanup needed for container environment'")

        commands.append("echo '--- Cleanup Complete ---'")

        return commands
