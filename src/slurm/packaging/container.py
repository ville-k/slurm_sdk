"""Container-based packaging strategy for SLURM clusters using Pyxis."""

from __future__ import annotations

import logging
import os
import pathlib
import shlex
import shutil
import subprocess  # nosec B404 - required for docker/podman commands during container building
import uuid
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union, Callable, Tuple

from .base import PackagingStrategy
from ..errors import PackagingError
from ..ui import progress_task

# Avoid circular imports for type hints
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cluster import Cluster
    from ..task import SlurmTask


logger = logging.getLogger(__name__)

_STEP_PROGRESS_RE = re.compile(r"^Step\s+(\d+)/(\d+)\s*:?\s*(.*)$", re.IGNORECASE)
_PUSH_LAYER_RE = re.compile(r"^([0-9a-f]{6,}):\s*(.*)$")


@dataclass
class _MountSpec:
    """Normalized container mount configuration."""

    source: str
    target: str
    options: Optional[str] = None

    def render(
        self,
        *,
        job_dir_expr: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> str:
        """Render the mount string, applying known placeholders if present."""

        def _apply(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None

            replacements = {}
            if job_dir_expr:
                replacements["job_dir"] = job_dir_expr
                replacements["job_dir_expr"] = job_dir_expr
            if job_dir:
                replacements["job_dir_path"] = job_dir

            pattern = re.compile(r"\{([^}]+)\}")

            def _replace(match: re.Match[str]) -> str:
                key = match.group(1)
                return replacements.get(key, match.group(0))

            return pattern.sub(_replace, value)

        rendered_source = _apply(self.source) or self.source
        rendered_target = _apply(self.target) or self.target
        rendered_options = _apply(self.options) if self.options else None

        if rendered_options:
            return f"{rendered_source}:{rendered_target}:{rendered_options}"
        return f"{rendered_source}:{rendered_target}"


@dataclass
class _BuildSecret:
    """Normalized representation of a BuildKit secret."""

    identifier: str
    kind: str  # "env" or "file"
    reference: str
    required: bool = True

    def as_cli_fragment(self) -> Tuple[str, str]:
        """Return the --secret argument fragment (flag, value)."""

        if self.kind == "env":
            return "--secret", f"id={self.identifier},env={self.reference}"
        return "--secret", f"id={self.identifier},src={self.reference}"


class ContainerPackagingStrategy(PackagingStrategy):
    """Builds (optionally) and runs tasks inside container images via Pyxis."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(config)
        # Track whether context was explicitly provided; default to "." for display
        # without forcing builds when unset.
        self._context_provided: bool = "context" in self.config
        self.runtime: Optional[str] = self.config.get("runtime")
        self.dockerfile: Optional[str] = self.config.get("dockerfile")
        if self._context_provided:
            self.context: Optional[str] = self.config.get("context")
        else:
            self.context = "."
        self.image: Optional[str] = self.config.get("image")
        self.image_name: Optional[str] = self.config.get("name")
        # Generate unique UUID-based tag if not provided for reproducibility
        if "tag" in self.config:
            self.tag: str = self.config["tag"]
        else:
            self.tag: str = f"build-{uuid.uuid4().hex[:12]}"
        self.registry: Optional[str] = self.config.get("registry")
        self.platform: str = self.config.get("platform", "linux/amd64")
        self.build_args: Dict[str, Any] = self.config.get("build_args", {})
        self.push: bool = bool(self.config.get("push", True))
        self.use_digest: bool = bool(self.config.get("use_digest", False))
        self.no_cache: bool = bool(self.config.get("no_cache", False))
        self.tls_verify: bool = bool(self.config.get("tls_verify", True))
        self.python_executable: str = self.config.get("python_executable", "python")
        self.modules: List[str] = list(self._as_list(self.config.get("modules", [])))
        self.srun_args: List[str] = list(
            self._as_list(self.config.get("srun_args", []))
        )
        self.mount_job_dir: bool = bool(self.config.get("mount_job_dir", True))
        self._mount_specs: List[_MountSpec] = self._normalize_mounts(
            self.config.get("mounts")
        )
        self.mounts: List[str] = [self._mount_repr(spec) for spec in self._mount_specs]
        self._build_secrets: List[_BuildSecret] = self._normalize_build_secrets(
            self.config.get("build_secrets")
        )
        self.container_workdir: Optional[str] = self.config.get("workdir")

        self._image_reference: Optional[str] = None
        self._runtime_cmd: Optional[str] = None
        self.last_prepare_result: Optional[Dict[str, Any]] = None
        self._active_build_secrets: List[str] = []

    def prepare(
        self, task: Union["SlurmTask", Callable], cluster: "Cluster"
    ) -> Dict[str, Any]:
        console = getattr(cluster, "console", None)

        # Check for RichLoggerCallback to determine verbosity
        self._packaging_callback = None
        self._verbose_packaging = True  # Default to verbose
        callbacks = getattr(cluster, "callbacks", [])
        if callbacks:
            from ..callbacks import RichLoggerCallback

            for callback in callbacks:
                if isinstance(callback, RichLoggerCallback):
                    self._packaging_callback = callback
                    self._verbose_packaging = getattr(callback, "_verbose", True)
                    break

        if self.use_digest:
            logger.debug(
                "Using auto-generated tag %s (will resolve to digest)", self.tag
            )
        else:
            logger.debug("Using auto-generated tag: %s", self.tag)

        image_ref = self._resolve_image_reference(task)

        built_image = False
        push_performed = False

        dockerfile_path, context_path = self._resolve_build_paths()

        # Fast path: if we have a pre-built image and don't need to build/push,
        # skip runtime detection entirely (for nested tasks inside containers)
        needs_runtime = bool(
            dockerfile_path or context_path or self.push or self.use_digest
        )
        if not needs_runtime and self.image:
            # Pre-built image, no runtime needed
            self._image_reference = self._convert_to_enroot_format(image_ref)
            logger.debug(
                "Using pre-built image reference (no runtime needed): %s",
                self._image_reference,
            )
            self._runtime_cmd = None
            prepare_result = {
                "status": "success",
                "image": image_ref,
                "image_digest": self._image_reference,
                "built": False,
                "pushed": False,
            }
            self.last_prepare_result = prepare_result
            return prepare_result

        # Need runtime for build/push/pull operations
        runtime = self._detect_runtime()

        if dockerfile_path or context_path:
            self._build_container_image(
                runtime=runtime,
                image_ref=image_ref,
                dockerfile_path=dockerfile_path,
                context_path=context_path,
                console=console,
            )
            built_image = True
        elif not self.image:
            raise PackagingError(
                "Container packaging requires either 'image' or 'dockerfile' in the configuration."
            )
        else:
            # Using pre-existing image
            # Only pull if we need the digest for digest-based references
            if self.use_digest:
                try:
                    pull_cmd = [runtime, "pull", image_ref]
                    logger.debug(
                        "Pulling pre-existing image to get digest: %s", image_ref
                    )
                    subprocess.run(
                        pull_cmd,
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    logger.warning("Failed to pull image %s: %s", image_ref, e.stderr)
                    # Continue anyway - image might already be available locally

        if self.push:
            self._push_container_image(
                runtime=runtime, image_ref=image_ref, console=console
            )
            push_performed = True

        # Optionally resolve image digest for reproducibility
        # Note: Some Pyxis/enroot versions may not properly support digest-based auth
        if self.use_digest:
            digest_ref = self._get_image_digest(runtime, image_ref)
            if digest_ref:
                self._image_reference = digest_ref
                logger.debug("Using digest-based image reference: %s", digest_ref)
            else:
                # Fallback to tag-based reference if digest cannot be determined
                self._image_reference = image_ref
                logger.warning(
                    "Could not resolve image digest, using tag-based reference: %s",
                    image_ref,
                )
        else:
            # Use tag-based reference (default for compatibility)
            self._image_reference = image_ref
            logger.debug("Using tag-based image reference: %s", image_ref)

        # Convert to enroot format for Pyxis compatibility
        self._image_reference = self._convert_to_enroot_format(self._image_reference)

        self._runtime_cmd = runtime

        prepare_result = {
            "status": "success",
            "image": image_ref,
            "image_digest": self._image_reference,  # Digest-based reference used in job
            "built": built_image,
            "pushed": push_performed,
            "runtime": runtime,
            "dockerfile": str(dockerfile_path) if dockerfile_path else None,
            "context": str(context_path) if context_path else None,
            "mounts": [self._mount_repr(spec) for spec in self._mount_specs],
            "mount_job_dir": self.mount_job_dir,
            "build_secrets": list(self._active_build_secrets),
        }

        self.last_prepare_result = prepare_result
        return prepare_result

    def generate_setup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        if not self._image_reference:
            raise RuntimeError(
                "Container strategy 'prepare' must be called before generating setup commands."
            )

        commands: List[str] = [
            "echo '--- Configuring container execution (Pyxis) ---'",
            "command -v srun >/dev/null 2>&1 || { echo 'ERROR: srun not found on PATH' >&2; exit 1; }",
            f"CONTAINER_IMAGE={shlex.quote(self._image_reference)}",
            "export CONTAINER_IMAGE",
            f"echo 'Resolved container image reference: {shlex.quote(self._image_reference)}'",
        ]

        # Set PY_EXEC as a bash array if it contains multiple words, otherwise as a simple variable
        py_exec_parts = self.python_executable.split()
        if len(py_exec_parts) > 1:
            # Multi-word: use array syntax PY_EXEC=(word1 word2 ...)
            array_elements = " ".join(shlex.quote(part) for part in py_exec_parts)
            commands.append(f"PY_EXEC=({array_elements})")
        else:
            # Single word: use simple variable
            commands.append(f"PY_EXEC={shlex.quote(self.python_executable)}")

        commands.append("export PY_EXEC")

        if job_dir:
            commands.append(f"CONTAINER_JOB_DIR={job_dir}")
            commands.append("export CONTAINER_JOB_DIR")

        for module_name in self.modules:
            commands.append(f"module load {shlex.quote(module_name)}")

        commands.append("echo 'Container image ready for execution.'")
        return commands

    def generate_cleanup_commands(
        self,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> List[str]:
        return []

    def wrap_execution_command(
        self,
        command: str,
        task: Union["SlurmTask", Callable],
        job_id: Optional[str] = None,
        job_dir: Optional[str] = None,
    ) -> str:
        if not self._image_reference:
            raise RuntimeError(
                "Container strategy 'prepare' must be called before wrapping execution commands."
            )

        job_dir_expr = self._normalise_job_dir(job_dir)
        mounts = [
            spec.render(job_dir_expr=job_dir_expr, job_dir=job_dir)
            for spec in self._mount_specs
        ]
        if self.mount_job_dir and job_dir_expr:
            # Mount the job base directory (parent of parent of job_dir) to allow
            # the runner to find result files from dependent jobs when resolving
            # JobResultPlaceholders. The job_dir structure is: {base}/{task}/{id}/
            job_base_dir_expr = f"$(dirname $(dirname {job_dir_expr}))"
            mounts.append(f"{job_base_dir_expr}:{job_base_dir_expr}:rw")

        srun_parts: List[str] = ["srun"]
        srun_parts.extend(self.srun_args)
        srun_parts.append(f"--container-image={shlex.quote(self._image_reference)}")

        # Add container name for attaching to the container later via `slurm jobs connect`
        if job_id:
            container_name = f"slurm-sdk-{job_id}"
            srun_parts.append(f"--container-name={shlex.quote(container_name)}")

        if mounts:
            mount_value = ",".join(mounts)
            srun_parts.append(f'--container-mounts="{mount_value}"')

        workdir_expr = self._render_workdir(job_dir_expr)
        if workdir_expr:
            srun_parts.append(f'--container-workdir="{workdir_expr}"')

        # Render the command directly after srun container arguments.
        # The command contains the python executable placeholder "${PY_EXEC[@]:-python}"
        # which is expanded by the shell in the sbatch script before srun is invoked.
        #
        # Important: For multi-word executables like "uv run python", the PY_EXEC variable
        # must be set as a bash array: PY_EXEC=(uv run python)
        # This ensures proper word splitting when ${PY_EXEC[@]} is expanded.
        wrapped_command = f"{' '.join(srun_parts)} {command}"
        logger.debug(
            "Wrapped execution command for container (image: %s): %s",
            self._image_reference,
            wrapped_command,
        )
        return wrapped_command

    @staticmethod
    def _as_list(value: Union[str, Iterable[str], None]) -> Iterable[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    @staticmethod
    def _to_sequence(value: Optional[Any]) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"false", "0", "no", "off"}:
                return False
            if lowered in {"true", "1", "yes", "on"}:
                return True
        return bool(value)

    def _normalize_mounts(self, mounts: Optional[Any]) -> List[_MountSpec]:
        normalized: List[_MountSpec] = []
        for entry in self._to_sequence(mounts):
            if isinstance(entry, str):
                normalized.append(self._parse_mount_string(entry))
            elif isinstance(entry, dict):
                normalized.append(self._parse_mount_dict(entry))
            else:
                raise PackagingError(
                    "Container mount entries must be strings or tables."
                )
        return normalized

    def _parse_mount_string(self, value: str) -> _MountSpec:
        parts = value.split(":")
        if len(parts) < 2:
            raise PackagingError(
                f"Invalid container mount '{value}'. Expected 'host:target[:options]'."
            )
        source = parts[0]
        target = parts[1]
        options = ":".join(parts[2:]) if len(parts) > 2 else None
        return _MountSpec(source=source, target=target, options=options or None)

    def _parse_mount_dict(self, value: Dict[str, Any]) -> _MountSpec:
        host = (
            value.get("host_path")
            or value.get("host")
            or value.get("source")
            or value.get("src")
        )
        target = (
            value.get("container_path")
            or value.get("target")
            or value.get("destination")
            or value.get("dst")
        )

        if not host or not target:
            raise PackagingError(
                "Container mount table entries require 'host_path'/'source' and 'container_path'/'target'."
            )

        options: Optional[str] = None
        if "options" in value:
            raw_options = value["options"]
            if isinstance(raw_options, (list, tuple)):
                options = ",".join(str(opt) for opt in raw_options)
            else:
                options = str(raw_options)
        elif "mode" in value:
            options = str(value["mode"])
        elif "access" in value:
            options = str(value["access"])
        elif "read_only" in value:
            read_only = self._coerce_bool(value.get("read_only"))
            options = "ro" if read_only else "rw"

        return _MountSpec(source=str(host), target=str(target), options=options)

    def _normalize_build_secrets(self, secrets: Optional[Any]) -> List[_BuildSecret]:
        normalized: List[_BuildSecret] = []
        for entry in self._to_sequence(secrets):
            secret = self._parse_build_secret(entry)
            if secret is not None:
                normalized.append(secret)
        return normalized

    def _parse_build_secret(self, entry: Any) -> Optional[_BuildSecret]:
        if isinstance(entry, str):
            kv_pairs = {}
            for part in entry.split(","):
                if "=" not in part:
                    continue
                key, raw_val = part.split("=", 1)
                kv_pairs[key.strip().lower()] = raw_val.strip()
            entry = kv_pairs

        if not isinstance(entry, dict):
            raise PackagingError(
                "Build secret entries must be strings or tables with 'id' and 'env'/'file'."
            )

        identifier = entry.get("id") or entry.get("name") or entry.get("identifier")
        env_name = entry.get("env") or entry.get("environment")
        file_path = entry.get("file") or entry.get("src") or entry.get("path")
        required = self._coerce_bool(entry.get("required", True))

        if not identifier:
            raise PackagingError("Build secret entries require an 'id'.")

        if env_name and file_path:
            raise PackagingError(
                f"Build secret '{identifier}' cannot specify both 'env' and 'file'."
            )

        if not env_name and not file_path:
            raise PackagingError(
                f"Build secret '{identifier}' must specify either 'env' or 'file'."
            )

        if env_name:
            return _BuildSecret(
                identifier=str(identifier),
                kind="env",
                reference=str(env_name),
                required=required,
            )

        return _BuildSecret(
            identifier=str(identifier),
            kind="file",
            reference=str(file_path),
            required=required,
        )

    def _prepare_build_secrets(
        self, runtime_name: str
    ) -> Tuple[List[str], Optional[Dict[str, str]]]:
        cli_parts: List[str] = []
        env_overrides: Dict[str, str] = {}
        used_secret_ids: List[str] = []

        for secret in self._build_secrets:
            if secret.kind == "env":
                env_name = secret.reference
                env_value = os.getenv(env_name)
                if env_value is None:
                    if secret.required:
                        raise PackagingError(
                            f"Missing required build secret environment variable '{env_name}' for id '{secret.identifier}'."
                        )
                    logger.debug(
                        "Skipping optional build secret '%s': env '%s' not set.",
                        secret.identifier,
                        env_name,
                    )
                    continue
                flag, value = secret.as_cli_fragment()
                cli_parts.extend([flag, value])
                used_secret_ids.append(secret.identifier)
            else:
                path = pathlib.Path(secret.reference).expanduser()
                if not path.exists():
                    if secret.required:
                        raise PackagingError(
                            f"Missing required build secret file for id '{secret.identifier}': {path}"
                        )
                    logger.debug(
                        "Skipping optional build secret '%s': file '%s' not found.",
                        secret.identifier,
                        path,
                    )
                    continue
                flag = "--secret"
                value = f"id={secret.identifier},src={path}"
                cli_parts.extend([flag, value])
                used_secret_ids.append(secret.identifier)

        if cli_parts and runtime_name == "docker" and not os.getenv("DOCKER_BUILDKIT"):
            env_overrides["DOCKER_BUILDKIT"] = "1"

        self._active_build_secrets = used_secret_ids

        return cli_parts, env_overrides or None

    @staticmethod
    def _mount_repr(spec: _MountSpec) -> str:
        if spec.options:
            return f"{spec.source}:{spec.target}:{spec.options}"
        return f"{spec.source}:{spec.target}"

    def _detect_runtime(self) -> str:
        if self.runtime:
            return self.runtime
        for candidate in ("docker", "podman"):
            if shutil.which(candidate):
                # Check if this is podman masquerading as docker
                # (common on macOS with Podman Desktop which provides a docker shim)
                try:
                    result = subprocess.run(
                        [candidate, "--version"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    if result.returncode == 0 and "podman" in result.stdout.lower():
                        # Use podman directly if available, since we now know it's podman
                        if shutil.which("podman"):
                            logger.debug(
                                "Detected container runtime: podman (via %s shim)",
                                candidate,
                            )
                            return "podman"
                except (subprocess.TimeoutExpired, OSError):
                    pass
                logger.debug("Detected container runtime: %s", candidate)
                return candidate
        raise PackagingError(
            "Container runtime not found.\n\n"
            "The slurm-sdk needs Docker or Podman to build and run container images.\n"
            "Neither 'docker' nor 'podman' was found in your PATH.\n\n"
            "To fix this:\n"
            "  1. Install Docker Desktop: https://www.docker.com/products/docker-desktop\n"
            "  2. Or install Podman: https://podman.io/getting-started/installation\n"
            "  3. Ensure the command is in your PATH (try: which docker)\n"
            "  4. Or specify explicitly: packaging='container', packaging_runtime='/path/to/docker'\n\n"
            "If you don't need container packaging, use: packaging='wheel' or packaging='none'"
        )

    def _resolve_project_root(self) -> pathlib.Path:
        return pathlib.Path.cwd()

    def _resolve_build_paths(
        self,
    ) -> tuple[Optional[pathlib.Path], Optional[pathlib.Path]]:
        project_root = self._resolve_project_root()

        dockerfile_path: Optional[pathlib.Path] = None
        context_path: Optional[pathlib.Path] = None

        if self.dockerfile:
            dockerfile_path = pathlib.Path(self.dockerfile).expanduser()
            if not dockerfile_path.is_absolute():
                dockerfile_path = (project_root / dockerfile_path).resolve()
            if not dockerfile_path.exists():
                raise PackagingError(
                    f"Dockerfile not found.\n\n"
                    f"Expected location: {dockerfile_path}\n"
                    f"Specified path: {self.dockerfile}\n"
                    f"Project root: {project_root}\n\n"
                    "Common causes:\n"
                    "  1. Dockerfile path is incorrect or misspelled\n"
                    "  2. Dockerfile is in a different directory\n"
                    "  3. Relative path is relative to wrong directory\n\n"
                    "To fix:\n"
                    "  1. Verify Dockerfile exists at the specified path\n"
                    "  2. Use absolute path: packaging_dockerfile='/absolute/path/to/Dockerfile'\n"
                    "  3. Or use path relative to project root\n"
                    f"  4. Check working directory: {pathlib.Path.cwd()}"
                )

        context_value = self.context if self._context_provided else None

        if context_value:
            context_path = pathlib.Path(context_value).expanduser()
            if not context_path.is_absolute():
                context_path = (project_root / context_path).resolve()
            if not context_path.exists():
                raise PackagingError(
                    f"Build context directory not found.\n\n"
                    f"Expected location: {context_path}\n"
                    f"Specified path: {context_value}\n"
                    f"Project root: {project_root}\n\n"
                    "The build context is the directory containing files needed for the Docker build.\n\n"
                    "To fix:\n"
                    "  1. Verify the context directory exists\n"
                    "  2. Use absolute path: packaging_context='/absolute/path/to/context'\n"
                    "  3. Or use path relative to project root\n"
                    f"  4. Check working directory: {pathlib.Path.cwd()}"
                )
            if not context_path.is_dir():
                raise PackagingError(
                    f"Build context must be a directory.\n\n"
                    f"Path: {context_path}\n"
                    f"Type: {('file' if context_path.is_file() else 'unknown')}\n\n"
                    "The build context directory contains your Dockerfile and application files.\n\n"
                    "To fix:\n"
                    "  1. Ensure the path points to a directory, not a file\n"
                    "  2. Create the directory if it doesn't exist\n"
                    "  3. Verify permissions allow reading the directory"
                )

        if dockerfile_path and not context_path:
            # Default to project root when no explicit context is provided.
            # This ensures build-time assets like pyproject.toml are available
            # even if the Dockerfile lives outside the repository root.
            context_path = project_root

        return dockerfile_path, context_path

    def _resolve_image_reference(self, task: Union["SlurmTask", Callable]) -> str:
        if self.image:
            image_ref = self.image
        else:
            base_name = self.image_name
            if not base_name:
                task_name = getattr(task, "name", None)
                if not task_name:
                    task_name = getattr(getattr(task, "func", task), "__name__", "task")
                safe_task_name = re.sub(r"[^a-z0-9_.-]+", "-", task_name.lower())
                unique_suffix = uuid.uuid4().hex[:8]
                base_name = f"slurm-task-{safe_task_name}-{unique_suffix}"
            image_ref = base_name

        if ":" not in image_ref:
            image_ref = f"{image_ref}:{self.tag}"

        if self.registry:
            registry_prefix = self.registry.rstrip("/")
            image_ref = f"{registry_prefix}/{image_ref.lstrip('/')}"

        logger.debug("Resolved container image reference: %s", image_ref)
        return image_ref

    def _convert_to_enroot_format(self, image_ref: str) -> str:
        """Convert image reference to enroot format.

        Enroot requires registry:port#path format instead of registry:port/path.

        Args:
            image_ref: Docker-style image reference (e.g., registry:5000/path/image:tag)

        Returns:
            Enroot-compatible image reference (e.g., registry:5000#path/image:tag)
        """
        match = re.match(r"^([^/]+:\d+)/(.*)", image_ref)
        if match:
            registry_with_port = match.group(1)
            image_path = match.group(2)
            converted = f"{registry_with_port}#{image_path}"
            logger.info(
                "Converted image reference to enroot format: %s → %s",
                image_ref,
                converted,
            )
            return converted
        return image_ref

    def _build_container_image(
        self,
        runtime: str,
        image_ref: str,
        dockerfile_path: Optional[pathlib.Path],
        context_path: Optional[pathlib.Path],
        console=None,
    ) -> None:
        if not context_path:
            raise PackagingError(
                "Cannot build container image without a build context directory."
            )

        runtime_name = pathlib.Path(runtime).name.lower()
        cmd: List[str] = [runtime, "build", "-t", image_ref]

        if runtime_name == "docker":
            cmd.extend(["--progress", "plain"])

        if self.platform:
            cmd.extend(["--platform", self.platform])

        if dockerfile_path:
            cmd.extend(["-f", str(dockerfile_path)])

        for key, value in self.build_args.items():
            expanded_value = os.path.expandvars(str(value))
            cmd.extend(["--build-arg", f"{key}={expanded_value}"])

        secret_args, env_overrides = self._prepare_build_secrets(runtime_name)
        cmd.extend(secret_args)

        if self.no_cache:
            cmd.append("--no-cache")

        cmd.append(str(context_path))

        logger.debug("Building container image with command: %s", " ".join(cmd))

        run_env = os.environ.copy()
        if env_overrides:
            run_env.update(env_overrides)

        with progress_task(console, "Container build") as (progress, task_id):
            self._run_container_command(
                cmd,
                description="build",
                progress=progress,
                task_id=task_id,
                progress_mode="build",
                env=run_env,
            )

    def _push_container_image(self, runtime: str, image_ref: str, console=None) -> None:
        cmd = [runtime, "push"]

        # Podman-specific flags (not supported by Docker)
        if runtime == "podman":
            if not self.tls_verify:
                cmd.extend(["--tls-verify=false"])
            # Use Docker v2 format for enroot compatibility (doesn't support OCI)
            cmd.extend(["--format", "v2s2"])

        cmd.append(image_ref)

        logger.debug("Pushing container image with command: %s", " ".join(cmd))
        with progress_task(console, "Container push", total=None) as (
            progress,
            task_id,
        ):
            self._run_container_command(
                cmd,
                description="push",
                progress=progress,
                task_id=task_id,
                progress_mode="push",
            )

    def _get_image_digest(self, runtime: str, image_ref: str) -> Optional[str]:
        """Get the digest (SHA256) of an image after build/push.

        Args:
            runtime: Container runtime (docker/podman)
            image_ref: Image reference (with tag)

        Returns:
            Image digest in format: registry/image@sha256:abc123...
            Returns None if digest cannot be determined
        """
        try:
            # Use inspect to get the repo digest
            cmd = [
                runtime,
                "inspect",
                "--format",
                "{{index .RepoDigests 0}}",
                image_ref,
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            digest_ref = result.stdout.strip()

            # RepoDigests contain the full reference with digest
            # Format: registry/image@sha256:abc123...
            if digest_ref and "@sha256:" in digest_ref:
                logger.info("Resolved image digest: %s", digest_ref)
                return digest_ref

            # If RepoDigests is empty (image not pushed yet), try to get local digest
            cmd = [runtime, "inspect", "--format", "{{.Id}}", image_ref]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            local_id = result.stdout.strip()

            if local_id and local_id.startswith("sha256:"):
                # For local images, construct reference using the local ID
                # Remove tag from image_ref and append digest
                if ":" in image_ref:
                    base_ref = image_ref.rsplit(":", 1)[0]
                else:
                    base_ref = image_ref
                digest_ref = f"{base_ref}@{local_id}"
                logger.info("Using local image digest: %s", digest_ref)
                return digest_ref

            logger.warning("Could not determine digest for image: %s", image_ref)
            return None

        except subprocess.CalledProcessError as e:
            logger.warning("Failed to get image digest for %s: %s", image_ref, e.stderr)
            return None
        except Exception as e:
            logger.warning("Error getting image digest: %s", e)
            return None

    def _run_container_command(
        self,
        cmd: List[str],
        *,
        description: str,
        progress=None,
        task_id: Optional[int] = None,
        progress_mode: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        output_lines: List[str] = []
        return_code: Optional[int] = None

        try:
            assert process.stdout is not None
            for raw_line in process.stdout:
                line = raw_line.rstrip()
                output_lines.append(line)

                handled = False
                if progress and task_id is not None and progress_mode == "build":
                    handled = self._handle_build_progress(progress, task_id, line)
                elif progress and task_id is not None and progress_mode == "push":
                    handled = self._handle_push_progress(progress, task_id, line)

                if not handled:
                    if progress and hasattr(progress, "log"):
                        progress.log(line)
                    else:
                        # Check if we should suppress verbose logging
                        verbose_packaging = getattr(self, "_verbose_packaging", True)
                        packaging_callback = getattr(self, "_packaging_callback", None)

                        if not verbose_packaging and packaging_callback:
                            # In non-verbose mode, send output to callback
                            packaging_callback.update_packaging_output(line)
                        else:
                            # In verbose mode or no callback, use standard logging
                            logger.info("[%s] %s", description, line)

            return_code = process.wait()
        finally:
            if progress and task_id is not None:
                if return_code:
                    progress.update(task_id, description="Container error")
                else:
                    progress.update(task_id, description="Container complete")

        if return_code:
            combined_output = "\n".join(output_lines[-50:])
            logger.error(
                "Container %s command failed (exit %s). Last output:\n%s",
                description,
                return_code,
                combined_output,
            )

            # Build helpful error message
            error_msg = f"Container {description} failed.\n\n"
            error_msg += f"Command: {' '.join(cmd)}\n"
            error_msg += f"Exit code: {return_code}\n\n"
            error_msg += f"Output (last 50 lines):\n{combined_output}\n\n"

            if description == "build":
                error_msg += (
                    "Common causes:\n"
                    "  1. Syntax error in Dockerfile\n"
                    "  2. Base image not found or network issues\n"
                    "  3. Build command failed (RUN, COPY, etc.)\n"
                    "  4. Insufficient disk space or permissions\n\n"
                    "To fix:\n"
                    "  1. Check Dockerfile syntax and fix any errors\n"
                    "  2. Verify base image exists: docker pull <base-image>\n"
                    "  3. Try building manually to see full output:\n"
                    f"     {' '.join(cmd)}\n"
                    "  4. Check Docker daemon logs: docker system df\n"
                    "  5. Ensure you have sufficient disk space"
                )
            elif description == "push":
                error_msg += (
                    "Common causes:\n"
                    "  1. Not authenticated to registry (docker login required)\n"
                    "  2. No permission to push to this repository\n"
                    "  3. Network connectivity issues\n"
                    "  4. Registry is unavailable or rate-limited\n\n"
                    "To fix:\n"
                    "  1. Login to registry: docker login <registry>\n"
                    "  2. Verify you have push access to the repository\n"
                    "  3. Check network connectivity\n"
                    "  4. Try pushing manually to see full output:\n"
                    f"     {' '.join(cmd)}"
                )

            raise PackagingError(error_msg)

    @staticmethod
    def _handle_build_progress(progress, task_id: int, line: str) -> bool:
        match = _STEP_PROGRESS_RE.match(line)
        if not match:
            return False
        current = int(match.group(1))
        total = int(match.group(2))
        description = match.group(3).strip()
        step_desc = f"Build {current}/{total}"
        if description:
            step_desc = f"{step_desc} – {description}"
        progress.update(
            task_id,
            total=total,
            completed=current,
            description=step_desc,
        )
        return True

    @staticmethod
    def _handle_push_progress(progress, task_id: int, line: str) -> bool:
        match = _PUSH_LAYER_RE.match(line)
        if not match:
            if "Pushed" in line or "Mounted" in line:
                progress.update(task_id, description=line.strip())
                return True
            return False
        layer = match.group(1)
        status = match.group(2).strip()
        progress.update(task_id, description=f"{layer}: {status}")
        return True

    @staticmethod
    def _normalise_job_dir(job_dir: Optional[str]) -> Optional[str]:
        if not job_dir:
            return "$JOB_DIR"
        stripped = job_dir.strip('"')
        return stripped or "$JOB_DIR"

    def _render_workdir(self, job_dir_expr: Optional[str]) -> Optional[str]:
        if self.container_workdir:
            # Allow {job_dir} token for convenience
            if "{job_dir}" in self.container_workdir and job_dir_expr:
                return self.container_workdir.format(job_dir=job_dir_expr)
            return self.container_workdir
        return job_dir_expr
