"""
Wheel packaging strategy: builds a wheel and installs it on the target.

Design goals:
- Minimal configuration; sensible defaults
- Clear control flow
- Concise logging (debug for details)
"""

import logging
import os
import subprocess
import tempfile
import pathlib
from typing import Any, Dict, List, Optional, Union, Callable
import shlex
import uuid

from .base import PackagingStrategy

# Import the backend class needed for the isinstance check
from ..api.ssh import SSHCommandBackend
from ..errors import PackagingError

# Avoid circular imports for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..task import SlurmTask


logger = logging.getLogger(__name__)


class WheelPackagingStrategy(PackagingStrategy):
    """
    Strategy that builds a wheel from the project and installs it on the cluster.

    This strategy assumes:
    1. The project uses pyproject.toml for packaging
    2. Python 3 with venv support is available on the cluster
    3. The cluster has access to PyPI or a specified package index
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the wheel packaging strategy.

        Args:
            config: Configuration (all optional)
                - build_tool: "uv" (default) or fallback to pip automatically
                - extra_index_url: URL to a private PyPI server
                - extras: List of extras to install
                - dependencies: List of additional dependencies to install
                - python_executable: Interpreter to use for venv (default: "python")
        """
        super().__init__(config)
        self.build_tool = self.config.get("build_tool", "uv")
        self.extra_index_url = self.config.get("extra_index_url")
        self.extras = self.config.get("extras", [])
        self.dependencies = self.config.get("dependencies", [])
        self.python_executable = self.config.get("python_executable", "python3")
        self.upgrade_pip: bool = bool(self.config.get("upgrade_pip", False))
        self.wheel_path: Optional[str] = None
        self.last_prepare_result: Optional[Dict[str, Any]] = None

    def prepare(
        self, task: Union["SlurmTask", Callable], cluster: "Cluster"
    ) -> Dict[str, Any]:
        """
        Build a wheel from the project.

        Args:
            task: The task to package
            cluster: The cluster to submit to

        Returns:
            Dict with status and wheel information
        """
        # Create a temporary directory for the wheel
        temp_dir = tempfile.mkdtemp(prefix="slurm_wheel_")

        # Locate project root and build wheel
        project_root = self._find_project_root()
        if not project_root:
            raise PackagingError(
                "Could not find pyproject.toml in any parent directory.\n"
                f"Searched from: {pathlib.Path.cwd()}\n"
                "For wheel packaging, your project must have a pyproject.toml file.\n"
                "See: https://packaging.python.org/tutorials/packaging-projects/"
            )

        wheel_path = self._build_wheel(project_root, temp_dir)

        # Store the wheel path for later use
        self.wheel_path = wheel_path

        # Verify the wheel exists
        if not os.path.exists(wheel_path):
            raise PackagingError(f"Built wheel file does not exist: {wheel_path}")

        logger.debug("Wheel built")

        # Ensure it potentially uploads the wheel if using SSH backend
        if isinstance(cluster.backend, SSHCommandBackend):
            # Upload the wheel to a temporary location in the remote job base
            try:
                remote_base = cluster.backend.get_remote_upload_base_path()
            except Exception as e_path:
                raise PackagingError(
                    "Could not determine remote upload base path"
                ) from e_path

            upload_id = uuid.uuid4().hex[:8]
            wheel_filename = os.path.basename(wheel_path)
            remote_upload_path = (
                f"{remote_base}/upload_{upload_id}_{wheel_filename}".replace("\\", "/")
            )

            logger.info("Uploading wheel to remote")
            try:
                cluster.backend.upload_file(wheel_path, remote_upload_path)
            except Exception as e_upload:
                raise PackagingError(
                    "Failed to upload wheel to remote cluster"
                ) from e_upload

            prepare_result = {
                "status": "success",
                "message": f"Built wheel at {wheel_path}",
                "wheel_path": wheel_path,
                "wheel_filename": wheel_filename,
                "project_root": project_root,
                "temp_dir": temp_dir,
                "remote_upload_path": remote_upload_path,
            }
        else:
            # Local backend doesn't need remote path
            prepare_result = {
                "status": "success",
                "message": f"Built wheel at {wheel_path}",
                "wheel_path": wheel_path,
                "wheel_filename": os.path.basename(wheel_path),
                "project_root": project_root,
                "temp_dir": temp_dir,
            }

        self.last_prepare_result = prepare_result  # Store result
        return self.last_prepare_result

    def generate_setup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """Generates commands to setup environment within the specified job_dir."""
        if not self.last_prepare_result or "wheel_path" not in self.last_prepare_result:
            raise RuntimeError(
                "Wheel strategy 'prepare' must be called before generating setup commands."
            )

        if job_dir is None:
            raise ValueError("job_dir must be provided to generate_setup_commands")

        remote_upload_path = self.last_prepare_result.get(
            "remote_upload_path"
        )  # Path where wheel was *initially* uploaded (in base dir)
        original_wheel_filename = self.last_prepare_result.get("wheel_filename")
        if not original_wheel_filename:
            raise RuntimeError("Missing wheel filename in prepare result.")

        # Build base paths
        venv_dir_name = f".slurm_venv_{job_id if job_id else 'job'}"
        venv_path = f"{job_dir}/{venv_dir_name}"
        target_wheel_path = f"{job_dir}/{original_wheel_filename}"

        # Options
        extra_index_cmd = ""
        if self.extra_index_url:
            extra_index_cmd = f"--extra-index-url {shlex.quote(self.extra_index_url)}"
        extras_str = f"[{','.join(self.extras)}]" if self.extras else ""
        deps_str = " ".join(shlex.quote(dep) for dep in self.dependencies)

        # Interpreter
        selected_python = self.python_executable or "python"
        version_cfg = self.config.get("python_version")
        if isinstance(version_cfg, str) and version_cfg.strip():
            selected_python = f"python{version_cfg.strip()}"

        # Build command list
        commands = [
            f"echo '--- Setting up Wheel Environment (Job ID: {job_id}) ---'",
            "echo 'Working directory: $(pwd)'",
            f"echo 'Job directory: {job_dir}'",
            f"echo 'Using Python interpreter: {selected_python}'",
            f"PY_EXEC={shlex.quote(selected_python)}",
            'command -v "$PY_EXEC" >/dev/null 2>&1 || { echo "ERROR: Python interpreter not found: $PY_EXEC" >&2; exit 1; }',
            '"$PY_EXEC" --version || { echo "ERROR: Failed to run $PY_EXEC --version" >&2; exit 1; }',
        ]

        # Move uploaded wheel into JOB_DIR if we uploaded via SSH
        if remote_upload_path:
            commands.append(
                f"echo 'Moving uploaded wheel {remote_upload_path} to {target_wheel_path}'"
            )
            commands.append(f"mv {shlex.quote(remote_upload_path)} {target_wheel_path}")
            install_wheel_path = target_wheel_path
        else:
            # Local install directly from the built wheel path
            install_wheel_path = shlex.quote(self.last_prepare_result["wheel_path"])  # type: ignore[index]

        # Venv creation and activation (use explicit interpreter)
        commands.append(f"echo 'Creating venv at {venv_path}'")
        commands.append(f'"$PY_EXEC" -m venv {venv_path}')
        commands.append(f"echo 'Activating venv: {venv_path}/bin/activate'")
        commands.append(f"source {venv_path}/bin/activate")
        commands.append(f"PY_EXEC={venv_path}/bin/python")
        commands.append(f'echo "Updated PY_EXEC to venv python: $PY_EXEC"')

        # Installation
        commands.append("echo 'Installing wheel and dependencies using pip...'")
        if self.upgrade_pip:
            commands.append('"$PY_EXEC" -m pip install --upgrade pip')
        install_cmd = f'"$PY_EXEC" -m pip install {extra_index_cmd} {install_wheel_path}{extras_str} {deps_str}'
        commands.append(install_cmd.strip())

        commands.append('echo "Installation complete. Python: $(which \\"$PY_EXEC\\")"')
        commands.append("echo '--- Environment Setup Complete ---'")

        return commands

    def generate_cleanup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        """Generates commands to deactivate and remove the virtual environment and wheel from job_dir."""
        if not self.last_prepare_result:
            logger.warning(
                "Warning: Cannot generate cleanup commands, prepare result missing."
            )
            return []

        if job_dir is None:
            logger.warning(
                "Warning: job_dir not provided for cleanup, skipping environment removal."
            )
            return [
                f"echo '--- Skipping Wheel Environment Cleanup (Job ID: {job_id}) --- '",
                "echo 'Warning: job_dir was not provided.'",
            ]

        venv_dir_name = f".slurm_venv_{job_id if job_id else 'job'}"
        venv_path = f"{job_dir}/{venv_dir_name}"

        original_wheel_filename = self.last_prepare_result.get("wheel_filename")
        target_wheel_path = (
            f"{job_dir}/{original_wheel_filename}" if original_wheel_filename else ""
        )

        commands = [
            f"echo '--- Cleaning Up Wheel Environment (Job ID: {job_id}) ---'",
            f"echo 'Job directory: {job_dir}'",
            "echo 'Deactivating virtualenv (if active)...'",
            "type deactivate > /dev/null 2>&1 && deactivate",
            f"echo 'Removing venv: {venv_path}'",
            f"rm -rf {venv_path}",
        ]

        if target_wheel_path:
            commands.append(f"echo 'Removing target wheel: {target_wheel_path}'")
            commands.append(f"rm -f {target_wheel_path}")

        # Assume mv removed the original upload path
        commands.append("echo '--- Cleanup Complete ---'")

        return commands

    def _find_project_root(self) -> Optional[pathlib.Path]:
        """
        Find the project root by looking for pyproject.toml in parent directories.

        Returns:
            Path to the project root, or None if not found
        """
        current_dir = pathlib.Path.cwd()

        # Look for pyproject.toml in current and parent directories
        while current_dir != current_dir.parent:
            pyproject_path = current_dir / "pyproject.toml"
            if pyproject_path.exists():
                return current_dir
            current_dir = current_dir.parent

        return None

    def _build_wheel(self, project_root: pathlib.Path, output_dir: str) -> str:
        """Build with uv when possible; otherwise fallback to pip."""
        if self.build_tool == "uv":
            built = self._build_with_uv(project_root, output_dir)
            if built:
                return built
        return self._build_with_pip(project_root, output_dir)

    def _build_with_uv(self, project_root: pathlib.Path, output_dir: str) -> str:
        """
        Build a wheel using uv.

        Args:
            project_root: Path to the project root
            output_dir: Directory to output the wheel to

        Returns:
            Path to the built wheel
        """
        # Check if uv is installed
        try:
            version_result = subprocess.run(
                ["uv", "--version"], check=True, capture_output=True, text=True
            )
            logger.debug("Using uv version: %s", version_result.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning(
                "Warning: uv is not installed or not working properly. Falling back to pip."
            )
            return self._build_with_pip(project_root, output_dir)

        # Build the wheel using the correct uv build command
        cmd = ["uv", "build", "--wheel", "-o", output_dir, str(project_root)]
        logger.debug("Building wheel with command: %s", " ".join(cmd))

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.debug("uv build output: %s", result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("Error running uv build: %s", e)
            logger.error("stdout: %s", e.stdout)
            logger.error("stderr: %s", e.stderr)
            logger.warning("Falling back to pip...")
            return self._build_with_pip(project_root, output_dir)
        except Exception as e:  # Broader safety net for mocked or unexpected failures
            logger.error("uv build failed unexpectedly: %s", e)
            logger.warning("Falling back to pip...")
            return self._build_with_pip(project_root, output_dir)

        # Find the wheel file in the output directory
        wheels = list(pathlib.Path(output_dir).glob("*.whl"))
        if not wheels:
            logger.warning(
                "No wheel found in %s after build with uv. Falling back to pip...",
                output_dir,
            )
            return self._build_with_pip(project_root, output_dir)

        # Return the path to the wheel
        logger.debug("Successfully built wheel: %s", wheels[0])
        return str(wheels[0])

    def _build_with_pip(self, project_root: pathlib.Path, output_dir: str) -> str:
        """
        Build a wheel using pip.

        Args:
            project_root: Path to the project root
            output_dir: Directory to output the wheel to

        Returns:
            Path to the built wheel
        """
        # Check if pip is installed
        try:
            subprocess.run(["pip", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise PackagingError("Neither uv nor pip is available for building wheels")

        # Build the wheel
        cmd = ["pip", "wheel", "--no-deps", "-w", output_dir, str(project_root)]
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        # Find the wheel file in the output directory
        wheels = list(pathlib.Path(output_dir).glob("*.whl"))
        if not wheels:
            raise PackagingError(f"No wheel found in {output_dir} after build with pip")

        # Return the path to the wheel
        return str(wheels[0])
