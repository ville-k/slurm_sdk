"""
Wheel packaging strategy: builds a wheel and installs it on the target.

Design goals:
- Minimal configuration; sensible defaults
- Clear control flow
- Concise logging (debug for details)
"""

import logging
import os
import subprocess  # nosec B404 - required for pip/uv commands during wheel building
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
                "Failed to prepare wheel: No pyproject.toml found in current directory or any parent directory.\n\n"
                f"Searched from: {pathlib.Path.cwd()}\n\n"
                "The slurm-sdk uses wheel packaging to bundle your code for remote execution.\n"
                "For this to work, your project must have a pyproject.toml file at its root.\n\n"
                "To fix this:\n"
                "  1. Create a pyproject.toml file in your project root\n"
                "  2. Add minimal project configuration (see example below)\n"
                "  3. Or disable packaging with: @task(packaging={'type': 'none'})\n\n"
                "Example minimal pyproject.toml:\n"
                "  [build-system]\n"
                "  requires = ['setuptools>=45', 'wheel']\n"
                "  build-backend = 'setuptools.build_meta'\n\n"
                "  [project]\n"
                "  name = 'your-project-name'\n"
                "  version = '0.1.0'\n\n"
                "Learn more: https://packaging.python.org/tutorials/packaging-projects/"
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
                    "Failed to prepare wheel upload: Could not determine remote upload path on cluster.\n\n"
                    f"Backend: {type(cluster.backend).__name__}\n"
                    f"Error: {e_path}\n\n"
                    "Possible causes:\n"
                    "  1. SSH connection issues (check hostname, username, credentials)\n"
                    "  2. Permission issues on remote filesystem\n"
                    "  3. Remote directory doesn't exist or isn't writable\n\n"
                    "To diagnose:\n"
                    "  1. Verify you can SSH to the cluster manually\n"
                    "  2. Check that job_base_dir is writable: ssh user@host ls -la /path/to/job_base_dir\n"
                    "  3. Try running with LocalBackend first to isolate SSH issues"
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
                    "Failed to upload wheel to remote cluster.\n\n"
                    f"Local wheel: {wheel_path}\n"
                    f"Remote path: {remote_upload_path}\n"
                    f"Error: {e_upload}\n\n"
                    "Possible causes:\n"
                    "  1. Network connectivity issues\n"
                    "  2. SSH authentication failure (check credentials)\n"
                    "  3. Remote directory doesn't exist or isn't writable\n"
                    "  4. Disk space issues on remote cluster\n\n"
                    "To diagnose:\n"
                    "  1. Test SSH connection: ssh {hostname} ls -la\n"
                    "  2. Check disk space on cluster: ssh {hostname} df -h\n"
                    "  3. Verify permissions: ssh {hostname} ls -la {remote_base}\n"
                    "  4. Try uploading manually: scp {wheel_path} {hostname}:{remote_upload_path}".format(
                        hostname=getattr(cluster.backend, "hostname", "cluster"),
                        remote_base=remote_base,
                        wheel_path=wheel_path,
                        remote_upload_path=remote_upload_path,
                    )
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
        """Generates commands to setup environment within the specified job_dir.

        This implementation uses file locking to handle concurrent execution in
        array jobs. Only one array element will perform the setup, while others
        wait and then use the already-setup environment.
        """
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
        # Note: job_dir is "$JOB_DIR" (bash variable), paths will be properly quoted when used
        venv_dir_name = f".slurm_venv_{job_id if job_id else 'job'}"
        venv_path = f"{job_dir}/{venv_dir_name}"
        target_wheel_path = f"{job_dir}/{original_wheel_filename}"
        lock_file = f"{job_dir}/.wheel_setup.lock"
        setup_complete_marker = f"{venv_path}/.setup_complete"

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

        # Build command list with file locking for array job safety
        commands = [
            f"echo '--- Setting up Wheel Environment (Job ID: {job_id}) ---'",
            "echo 'Working directory: $(pwd)'",
            f"echo 'Job directory: {job_dir}'",
        ]

        # Detect if running in an array job
        commands.append("# Detect array job execution")
        commands.append('if [ -n "$SLURM_ARRAY_TASK_ID" ]; then')
        commands.append(
            '  echo "Array task ID: $SLURM_ARRAY_TASK_ID (using file locking)"'
        )
        commands.append("  ARRAY_JOB=1")
        commands.append("else")
        commands.append("  ARRAY_JOB=0")
        commands.append("fi")
        commands.append("")

        # Use file locking to synchronize array elements
        commands.append("# File locking for concurrent array execution")
        commands.append(f'LOCK_FILE="{lock_file}"')
        commands.append(f'VENV_PATH="{venv_path}"')
        commands.append(f'SETUP_MARKER="{setup_complete_marker}"')
        commands.append("")

        # Check if setup is already complete
        commands.append('if [ -f "$SETUP_MARKER" ]; then')
        commands.append(
            '  echo "Environment already set up by another array element, skipping setup"'
        )
        commands.append("  SKIP_SETUP=1")
        commands.append("else")
        commands.append("  SKIP_SETUP=0")
        commands.append("fi")
        commands.append("")

        # If not skipping, acquire lock and set up
        commands.append('if [ "$SKIP_SETUP" = "0" ]; then')
        commands.append("  # Open lock file and acquire exclusive lock")
        commands.append('  exec 200>"$LOCK_FILE"')
        commands.append('  echo "Acquiring setup lock..."')
        commands.append("  flock -x 200")
        commands.append('  echo "Lock acquired"')
        commands.append("")

        # Check again inside the lock (another process may have completed setup)
        commands.append("  # Check again if setup was completed while waiting for lock")
        commands.append('  if [ -f "$SETUP_MARKER" ]; then')
        commands.append(
            '    echo "Setup completed by another process while waiting, using existing environment"'
        )
        commands.append("    flock -u 200")
        commands.append("  else")
        commands.append('    echo "Performing environment setup..."')
        commands.append("")

        # Perform actual setup inside the lock
        commands.append(f'    echo "Using Python interpreter: {selected_python}"')
        commands.append(f"    PY_EXEC={shlex.quote(selected_python)}")
        commands.append(
            '    command -v "$PY_EXEC" >/dev/null 2>&1 || { echo "ERROR: Python interpreter not found: $PY_EXEC" >&2; exit 1; }'
        )
        commands.append(
            '    "$PY_EXEC" --version || { echo "ERROR: Failed to run $PY_EXEC --version" >&2; exit 1; }'
        )
        commands.append("")

        # Move uploaded wheel into JOB_DIR if we uploaded via SSH (with safety check)
        if remote_upload_path:
            commands.append("    # Move wheel file (with concurrent access safety)")
            commands.append(f"    if [ -f {shlex.quote(remote_upload_path)} ]; then")
            commands.append(
                f'      echo "Moving uploaded wheel {remote_upload_path} to {target_wheel_path}"'
            )
            commands.append(
                f'      mv {shlex.quote(remote_upload_path)} "{target_wheel_path}"'
            )
            commands.append(f'    elif [ ! -f "{target_wheel_path}" ]; then')
            commands.append(
                '      echo "ERROR: Wheel file not found at source or target" >&2'
            )
            commands.append("      flock -u 200")
            commands.append("      exit 1")
            commands.append("    else")
            commands.append('      echo "Wheel already at target location"')
            commands.append("    fi")
            install_wheel_path = f'"{target_wheel_path}"'
        else:
            # Local install directly from the built wheel path
            install_wheel_path = shlex.quote(self.last_prepare_result["wheel_path"])  # type: ignore[index]

        # Venv creation and activation (use explicit interpreter)
        commands.append('    echo "Creating venv at $VENV_PATH"')
        commands.append(
            '    "$PY_EXEC" -m venv "$VENV_PATH" || { echo "ERROR: Failed to create venv" >&2; flock -u 200; exit 1; }'
        )
        commands.append('    echo "Activating venv: $VENV_PATH/bin/activate"')
        commands.append(
            '    source "$VENV_PATH/bin/activate" || { echo "ERROR: Failed to activate venv" >&2; flock -u 200; exit 1; }'
        )
        commands.append('    PY_EXEC="$VENV_PATH/bin/python"')
        commands.append('    echo "Updated PY_EXEC to venv python: $PY_EXEC"')
        commands.append("")

        # Installation (use --quiet to avoid output size limits in SLURM)
        commands.append('    echo "Installing wheel and dependencies using pip..."')
        if self.upgrade_pip:
            commands.append(
                '    "$PY_EXEC" -m pip install --quiet --upgrade pip || { echo "ERROR: Failed to upgrade pip" >&2; flock -u 200; exit 1; }'
            )
        install_cmd = f'"$PY_EXEC" -m pip install --quiet {extra_index_cmd} {install_wheel_path}{extras_str} {deps_str}'
        commands.append(
            f'    {install_cmd.strip()} || {{ echo "ERROR: Failed to install wheel" >&2; flock -u 200; exit 1; }}'
        )
        commands.append("")

        # Mark setup as complete
        commands.append(
            '    echo "Installation complete. Python: $(which \\"$PY_EXEC\\")"'
        )
        commands.append(
            '    touch "$SETUP_MARKER" || { echo "ERROR: Failed to create setup marker" >&2; flock -u 200; exit 1; }'
        )
        commands.append('    echo "Setup complete, releasing lock"')
        commands.append("    flock -u 200")
        commands.append("  fi")
        commands.append("fi")
        commands.append("")

        # All processes (whether they did setup or not) activate the venv
        commands.append("# Activate environment (for all array elements)")
        commands.append(f'source "{venv_path}/bin/activate"')
        commands.append(f'PY_EXEC="{venv_path}/bin/python"')
        commands.append('echo "Using venv python: $PY_EXEC"')
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

        uv_error = None
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.debug("uv build output: %s", result.stdout)
        except subprocess.CalledProcessError as e:
            # Store uv error details for potential use later
            uv_error = e
            logger.error(
                "Error running uv build (exit code %d), falling back to pip",
                e.returncode,
            )
            logger.debug("uv stdout: %s", e.stdout)
            logger.debug("uv stderr: %s", e.stderr)
            return self._build_with_pip(project_root, output_dir, uv_error=uv_error)
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

    def _build_with_pip(
        self,
        project_root: pathlib.Path,
        output_dir: str,
        uv_error: Optional[subprocess.CalledProcessError] = None,
    ) -> str:
        """
        Build a wheel using pip.

        Args:
            project_root: Path to the project root
            output_dir: Directory to output the wheel to
            uv_error: Optional error from previous uv build attempt

        Returns:
            Path to the built wheel
        """
        # Check if pip is installed
        try:
            subprocess.run(["pip", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            error_msg = (
                "Failed to build wheel: Neither 'uv' nor 'pip' is available.\n\n"
            )

            if uv_error:
                error_msg += (
                    f"uv build failed with exit code {uv_error.returncode}:\n"
                    f"{uv_error.stderr}\n\n"
                    "And pip is not installed.\n\n"
                )

            error_msg += (
                "The slurm-sdk requires a Python package build tool to package your code for remote execution.\n\n"
                "To fix this, install one of the following:\n"
                "  1. uv (recommended):    pip install uv\n"
                "  2. pip (fallback):      Already included with most Python installations\n\n"
                "If you don't want automatic packaging, use: packaging='none' in your @task decorator."
            )
            raise PackagingError(error_msg)

        # Build the wheel
        cmd = ["pip", "wheel", "--no-deps", "-w", output_dir, str(project_root)]
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.debug("pip wheel output: %s", result.stdout)
        except subprocess.CalledProcessError as e:
            error_msg = "Failed to build wheel.\n\n"

            if uv_error:
                error_msg += (
                    "Both uv and pip builds failed:\n\n"
                    f"1. uv build failed (exit code {uv_error.returncode}):\n"
                    f"   {uv_error.stderr}\n\n"
                    f"2. pip build failed (exit code {e.returncode}):\n"
                    f"   Command: {' '.join(cmd)}\n"
                    f"   Stdout: {e.stdout}\n"
                    f"   Stderr: {e.stderr}\n\n"
                )
            else:
                error_msg += (
                    f"pip build failed:\n\n"
                    f"Command: {' '.join(cmd)}\n"
                    f"Project root: {project_root}\n"
                    f"Exit code: {e.returncode}\n\n"
                    f"Build output (stdout):\n{e.stdout}\n\n"
                    f"Build errors (stderr):\n{e.stderr}\n\n"
                )

            error_msg += (
                "Common causes:\n"
                "  1. Missing or invalid pyproject.toml file\n"
                "  2. Missing build dependencies (setuptools, wheel)\n"
                "  3. Syntax errors in your project configuration\n"
                "  4. Missing required dependencies in your project\n\n"
                "To fix:\n"
                "  1. Ensure pyproject.toml exists in your project root\n"
                "  2. Install build dependencies: pip install setuptools wheel\n"
                f"  3. Try building manually to see full output:\n"
                f"     cd {project_root} && pip wheel --no-deps -w /tmp .\n"
                "  4. Verify your pyproject.toml structure (see example below)\n\n"
                "Example minimal pyproject.toml:\n"
                "  [build-system]\n"
                "  requires = ['setuptools>=45', 'wheel']\n"
                "  build-backend = 'setuptools.build_meta'\n\n"
                "  [project]\n"
                "  name = 'myproject'\n"
                "  version = '0.1.0'\n\n"
                "Learn more: https://packaging.python.org/tutorials/packaging-projects/"
            )
            raise PackagingError(error_msg) from e

        # Find the wheel file in the output directory
        wheels = list(pathlib.Path(output_dir).glob("*.whl"))
        if not wheels:
            raise PackagingError(
                f"Wheel build completed but no .whl file was created.\n\n"
                f"Output directory: {output_dir}\n"
                f"Project root: {project_root}\n\n"
                "This usually means:\n"
                "  1. Your pyproject.toml is missing [project] metadata\n"
                "  2. The build succeeded but produced no distributable package\n"
                "  3. The wheel was created in a different location\n\n"
                "To diagnose:\n"
                f"  1. Check pyproject.toml has [build-system] and [project] sections\n"
                f"  2. Verify project name and version are set\n"
                f"  3. Try building manually: cd {project_root} && pip wheel --no-deps -w /tmp .\n"
                f"  4. Check the manual build output directory\n\n"
                "Example minimal pyproject.toml:\n"
                "  [build-system]\n"
                "  requires = ['setuptools>=45', 'wheel']\n"
                "  build-backend = 'setuptools.build_meta'\n\n"
                "  [project]\n"
                "  name = 'myproject'\n"
                "  version = '0.1.0'\n\n"
                "Learn more: https://packaging.python.org/tutorials/packaging-projects/"
            )

        # Return the path to the wheel
        return str(wheels[0])
