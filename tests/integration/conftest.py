"""Integration test fixtures for Slurm container tests."""

import os
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict, Tuple

import pytest


SLURM_USER = "slurm"


# ============================================================================
# Execution Mode Detection
# ============================================================================


def _detect_execution_mode() -> str:
    """Detect whether running on host, in devcontainer, or in CI.

    The execution mode affects how we connect to the Slurm cluster:
    - "host": Running on developer's machine, connect via localhost:2222
    - "devcontainer": Running inside devcontainer, connect via slurm:22 (internal DNS)
    - "ci": Running in CI pipeline, similar to devcontainer

    Returns:
        str: One of "host", "devcontainer", "ci"
    """
    # Explicit override via environment variable
    if mode := os.environ.get("SLURM_SDK_DEV_MODE"):
        return mode

    # CI detection (GitHub Actions, GitLab CI, etc.)
    if (
        os.environ.get("CI")
        or os.environ.get("GITHUB_ACTIONS")
        or os.environ.get("GITLAB_CI")
    ):
        return "ci"

    # VSCode devcontainer detection
    if os.environ.get("REMOTE_CONTAINERS"):
        return "devcontainer"

    # Check if we're inside a Docker container
    if os.path.exists("/.dockerenv"):
        return "devcontainer"

    # Check for container-specific environment
    if os.environ.get("SLURM_HOST") == "slurm":
        return "devcontainer"

    return "host"


EXECUTION_MODE = _detect_execution_mode()


def _docker_cli_available() -> bool:
    """Check if docker or podman CLI is available and can access the daemon.

    Some tests use testinfra's DockerBackend to run commands inside containers,
    which requires docker/podman CLI access. This isn't available inside the
    devcontainer unless the socket is mounted with proper permissions.
    """
    for cli in ["docker", "podman"]:
        try:
            result = subprocess.run(
                [cli, "info"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return False


DOCKER_CLI_AVAILABLE = _docker_cli_available()


# ============================================================================
# Container Test Markers
# ============================================================================
#
# We distinguish two types of container tests:
#
# - container_runtime: Tests that RUN containers but use prebuilt images.
#   These can run without docker CLI if images are already in the registry.
#
# - container_build: Tests that BUILD containers from Dockerfiles.
#   These require docker/podman CLI access to build and push images.
#


@pytest.fixture(autouse=True)
def skip_container_build_without_docker(request):
    """Skip container_build tests if docker CLI is not available.

    container_build tests require docker/podman CLI to build images from
    Dockerfiles. container_runtime tests use prebuilt images and only need
    docker CLI for the prebuilt_integration_image fixture.
    """
    if DOCKER_CLI_AVAILABLE:
        return  # Docker CLI available, no need to skip

    marker = request.node.get_closest_marker("container_build")
    if marker is not None:
        pytest.skip("Docker CLI not available (required for container_build tests)")


def _get_slurm_connection_info() -> Tuple[str, int]:
    """Get Slurm connection info based on execution mode.

    Returns:
        Tuple[str, int]: (hostname, port) for SSH connection to Slurm
    """
    if EXECUTION_MODE in ("devcontainer", "ci"):
        # Inside container network - use internal DNS
        hostname = os.environ.get("SLURM_HOST", "slurm")
        port = int(os.environ.get("SLURM_PORT", "22"))
        return hostname, port
    else:
        # On host - use port mapping
        return "127.0.0.1", 2222


def _services_already_running() -> bool:
    """Check if docker-compose services are already running.

    This is useful when running inside a devcontainer where services
    are started by the devcontainer configuration, or when re-running
    integration tests on host with SLURM_TEST_KEEP=1.
    """
    # Check if slurm is reachable by attempting to connect
    hostname, port = _get_slurm_connection_info()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((hostname, port))
        sock.close()
        return result == 0
    except Exception:
        return False


@pytest.fixture(scope="session")
def slurm_cluster_config(pyxis_container):
    """Configuration dict for Cluster.from_env().

    Now uses the unified docker-compose Slurm container instead of podman run.
    """
    return {
        "cluster": {
            "backend": "ssh",
            "job_base_dir": pyxis_container["job_base_dir"],
            "backend_config": {
                "hostname": pyxis_container["hostname"],
                "port": pyxis_container["port"],
                "username": pyxis_container["username"],
                "password": pyxis_container["password"],
                "look_for_keys": False,
                "allow_agent": False,
                "banner_timeout": 30,
            },
        },
        "packaging": {
            "type": "wheel",
            "python_executable": "/usr/bin/python3",
            "upgrade_pip": False,
        },
        "submit": {
            "partition": pyxis_container["partition"],
        },
    }


@pytest.fixture(scope="session")
def slurm_testinfra(pyxis_container):
    """Testinfra connection to the Slurm container.

    Now uses the unified docker-compose Slurm container instead of podman run.

    This fixture requires docker CLI access to the host daemon. When running
    inside a devcontainer without socket access, tests using this fixture
    will be skipped.
    """
    if not DOCKER_CLI_AVAILABLE:
        pytest.skip("Docker CLI not available (required for testinfra docker backend)")

    testinfra = pytest.importorskip("testinfra")

    # Use docker/podman backend to connect directly to the container
    # This avoids SSH authentication issues
    backend = f"{pyxis_container['runtime']}://{pyxis_container['name']}"

    return testinfra.get_host(backend, sudo=False)


@pytest.fixture
def slurm_cluster(slurm_cluster_config, tmp_path):
    """
    Provides a configured Cluster instance ready for integration testing.

    This fixture handles:
    - Creating a temporary Slurmfile with the correct configuration
    - Instantiating a Cluster from the Slurmfile
    - Using the session-scoped slurm_container connection info

    Returns:
        Cluster: A configured Cluster instance ready to submit jobs
    """
    import textwrap
    from slurm.cluster import Cluster

    # Build Slurmfile content from cluster config
    backend = slurm_cluster_config["cluster"]["backend"]
    job_base_dir = slurm_cluster_config["cluster"]["job_base_dir"]
    backend_config = slurm_cluster_config["cluster"]["backend_config"]
    packaging = slurm_cluster_config["packaging"]
    submit = slurm_cluster_config["submit"]

    # Optional python config: prefer explicit executable, fall back to version
    python_cfg_line = ""
    if "python_executable" in packaging and packaging["python_executable"]:
        python_cfg_line = (
            f'\n        python_executable = "{packaging["python_executable"]}"'
        )
    elif "python_version" in packaging and packaging["python_version"]:
        python_cfg_line = f'\n        python_version = "{packaging["python_version"]}"'

    content = textwrap.dedent(
        f"""
        [default.cluster]
        backend = \"{backend}\"
        job_base_dir = \"{job_base_dir}\"

        [default.cluster.backend_config]
        hostname = \"{backend_config["hostname"]}\"
        port = {backend_config["port"]}
        username = \"{backend_config["username"]}\"
        password = \"{backend_config["password"]}\"
        look_for_keys = false
        allow_agent = false
        banner_timeout = {backend_config["banner_timeout"]}

        [default.packaging]
        type = \"{packaging["type"]}\"{python_cfg_line}

        [default.submit]
        partition = \"{submit["partition"]}\"
        """
    ).strip()

    slurmfile_path = tmp_path / "Slurmfile.toml"
    slurmfile_path.write_text(content, encoding="utf-8")

    return Cluster.from_env(slurmfile_path, env="default")


# ============================================================================
# Container Packaging Fixtures (Pyxis/enroot)
# ============================================================================

REGISTRY_PORT = 20002


def _wait_for_port(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for a port to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


COMPOSE_FILE = Path(__file__).parent.parent.parent / "containers" / "docker-compose.yml"


def _compose_cmd() -> list[str] | None:
    override = os.environ.get("SLURM_TEST_COMPOSE_COMMAND")
    if override:
        parts = shlex.split(override)
        if parts and shutil.which(parts[0]) is not None:
            return parts
        return None

    if shutil.which("docker-compose") is not None:
        return ["docker-compose"]
    if shutil.which("docker") is not None:
        return ["docker", "compose"]
    if shutil.which("podman-compose") is not None:
        return ["podman-compose"]
    if shutil.which("podman") is not None:
        return ["podman", "compose"]
    return None


def _engine_cmd_for(compose_cmd: list[str]) -> str | None:
    if not compose_cmd:
        return None
    head = compose_cmd[0]
    if head in {"docker", "docker-compose"}:
        return "docker"
    if head in {"podman", "podman-compose"}:
        return "podman"
    return None


def _preflight_engine(engine_cmd: str) -> None:
    """Validate that the container engine is reachable (daemon/VM running)."""
    if shutil.which(engine_cmd) is None:
        pytest.skip(f"Integration tests require `{engine_cmd}` to be installed.")

    result = subprocess.run(
        [engine_cmd, "info"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    stderr = result.stderr.strip()
    stdout = result.stdout.strip()
    combined = "\n".join(part for part in [stdout, stderr] if part)

    needles = [
        "Cannot connect to the Docker daemon",
        "Is the docker daemon running",
        "Cannot connect to Podman",
        "unable to connect",
        "failed to connect",
        "Error:",
    ]
    first_line = next(
        (
            line.strip()
            for line in combined.splitlines()
            if any(n in line for n in needles)
        ),
        next(
            (line.strip() for line in combined.splitlines() if line.strip()), combined
        ),
    )

    hint_lines: list[str] = []
    display_engine = engine_cmd
    if engine_cmd == "docker" and "Cannot connect to Podman" in combined:
        display_engine = "docker (podman-docker shim)"
    if (
        "Cannot connect to the Docker daemon" in combined
        or "Is the docker daemon running" in combined
    ):
        hint_lines.append(
            "Hint: start Docker Desktop (or ensure the Docker daemon is running)."
        )
    if "Cannot connect to Podman" in combined or "podman machine" in combined:
        hint_lines.append(
            "Hint: run `podman machine init --now` (one-time) then `podman machine start`."
        )

    hint = ("\n" + "\n".join(hint_lines)) if hint_lines else ""
    pytest.fail(
        f"Container engine `{display_engine}` is not reachable: {first_line}{hint}\n\n{combined}"
    )


def _assert_port_available(host: str, port: int, description: str) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
    except OSError:
        pytest.fail(
            f"Port {port} is already in use ({description}). "
            "Stop the conflicting service or change the port mappings in containers/docker-compose.yml."
        )
    finally:
        sock.close()


def _container_runtime_for(container_name: str) -> str | None:
    preferred = os.environ.get("SLURM_TEST_CONTAINER_RUNTIME")
    candidates: list[str] = [preferred] if preferred else []
    candidates.extend(["docker", "podman"])

    tried: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in tried:
            continue
        tried.add(candidate)
        if shutil.which(candidate) is None:
            continue
        result = subprocess.run(
            [candidate, "inspect", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return candidate
    return None


@pytest.fixture(scope="session")
def docker_compose_project(request):
    """Start docker-compose services and provide connection info.

    In devcontainer/CI mode, services may already be running (started by
    docker-compose via devcontainer.json). In that case, we skip the startup
    and just return connection info.

    Returns:
        dict: Connection info for registry and Pyxis services
    """
    # Check if services are already running (devcontainer/CI mode)
    services_prestarted = _services_already_running()
    hostname, port = _get_slurm_connection_info()

    if services_prestarted:
        print(
            f"\nServices already running (mode={EXECUTION_MODE}), using {hostname}:{port}"
        )
        # Detect container runtime for exec commands
        container_runtime = _container_runtime_for("slurm-test")
        if container_runtime is None:
            # In devcontainer, we might not have direct container access
            # Try common runtimes
            for rt in ["docker", "podman"]:
                if shutil.which(rt):
                    container_runtime = rt
                    break
            if container_runtime is None:
                container_runtime = "docker"  # Default fallback

        return {
            "registry_url": "registry:20002",
            "registry_url_container": "registry:20002",
            "container_runtime": container_runtime,
            "compose_cmd": ["docker", "compose"],  # Assume docker compose
            "pyxis_hostname": hostname,
            "pyxis_port": port,
            "pyxis_username": SLURM_USER,
            "pyxis_password": SLURM_USER,
            "pyxis_container_name": "slurm-test",
            "_services_prestarted": True,
        }

    # Host mode: start services ourselves
    compose_cmd = _compose_cmd()
    if compose_cmd is None:
        pytest.skip(
            "Integration tests require Docker Compose; install `docker compose` or `docker-compose`, "
            "or set SLURM_TEST_COMPOSE_COMMAND."
        )

    engine_cmd = _engine_cmd_for(compose_cmd)
    if engine_cmd is not None:
        _preflight_engine(engine_cmd)

    _assert_port_available("0.0.0.0", REGISTRY_PORT, "test registry")
    _assert_port_available("0.0.0.0", 2222, "Slurm SSH")

    print("\nStarting docker-compose services...")
    up_cmd = [*compose_cmd, "-f", str(COMPOSE_FILE), "up", "-d"]
    if os.environ.get("SLURM_TEST_COMPOSE_NO_BUILD") is None:
        up_cmd.append("--build")

    result = subprocess.run(
        up_cmd,
        cwd=COMPOSE_FILE.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        command_str = " ".join(shlex.quote(part) for part in up_cmd)
        combined = "\n".join(
            part for part in [result.stdout.strip(), result.stderr.strip()] if part
        )
        stderr_first = next(
            (line for line in combined.splitlines() if line.strip()), "(no output)"
        )
        hint_lines: list[str] = []
        if (
            "Cannot connect to the Docker daemon" in combined
            or "Is the docker daemon running" in combined
        ):
            hint_lines.append(
                "Hint: start Docker Desktop (or ensure the Docker daemon is running)."
            )
        if "Cannot connect to Podman" in combined or "podman machine" in combined:
            hint_lines.append(
                "Hint: run `podman machine init --now` (one-time) then `podman machine start`."
            )
        hint = ("\n" + "\n".join(hint_lines)) if hint_lines else ""
        details = "\n".join(
            [
                f"Failed to start docker-compose services: {stderr_first}{hint}",
                f"Command: {command_str}",
                f"stdout:\n{result.stdout}",
                f"stderr:\n{result.stderr}",
            ]
        )
        pytest.fail(details)

    max_retries = 60
    for i in range(max_retries):
        result = subprocess.run(
            [
                *compose_cmd,
                "-f",
                str(COMPOSE_FILE),
                "ps",
                "--services",
                "--filter",
                "status=running",
            ],
            cwd=COMPOSE_FILE.parent,
            capture_output=True,
            text=True,
            check=False,
        )
        running_services = (
            result.stdout.strip().split("\n") if result.stdout.strip() else []
        )
        if "registry" in running_services and "slurm" in running_services:
            print(f"Services running: {running_services}")
            break
        time.sleep(1)
    else:
        subprocess.run(
            [*compose_cmd, "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
        )
        pytest.fail("Docker compose services did not start within timeout")

    container_runtime = _container_runtime_for("slurm-test")
    if container_runtime is None:
        subprocess.run(
            [*compose_cmd, "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
        )
        pytest.fail(
            "Could not detect container runtime (`docker` or `podman`) for container `slurm-test`. "
            "Set SLURM_TEST_CONTAINER_RUNTIME to `docker` or `podman`."
        )

    # Wait for systemd to initialize
    time.sleep(10)
    for i in range(30):
        result = subprocess.run(
            [
                container_runtime,
                "exec",
                "slurm-test",
                "systemctl",
                "is-active",
                "sshd",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and "active" in result.stdout:
            print("SSH service is active in Slurm container")
            break
        time.sleep(1)
    else:
        subprocess.run(
            [*compose_cmd, "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
        )
        pytest.fail("SSH service did not start in Pyxis container")

    # Enable oversubscription so workflows can run child tasks on the same node
    subprocess.run(
        [
            container_runtime,
            "exec",
            "slurm-test",
            "scontrol",
            "update",
            "PartitionName=debug",
            "OverSubscribe=YES",
        ],
        capture_output=True,
        check=False,
    )

    info = {
        "registry_url": "registry:20002",
        "registry_url_container": "registry:20002",
        "container_runtime": container_runtime,
        "compose_cmd": compose_cmd,
        "pyxis_hostname": hostname,
        "pyxis_port": port,
        "pyxis_username": SLURM_USER,
        "pyxis_password": SLURM_USER,
        "pyxis_container_name": "slurm-test",
        "_services_prestarted": False,
    }

    def cleanup():
        # Don't clean up if services were pre-started (devcontainer/CI mode)
        # or if SLURM_TEST_KEEP is set
        if info.get("_services_prestarted"):
            print("\nKeeping docker-compose services (pre-started by devcontainer)")
            return
        if os.environ.get("SLURM_TEST_KEEP"):
            print("\nKeeping docker-compose services (SLURM_TEST_KEEP set)")
            return
        print("\nStopping docker-compose services...")
        subprocess.run(
            [*compose_cmd, "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
            capture_output=True,
        )

    request.addfinalizer(cleanup)

    return info


@pytest.fixture(scope="session")
def local_registry(docker_compose_project):
    """Return registry URL, checking that /etc/hosts is configured.

    Returns:
        str: Registry URL (e.g., "registry:20002")
    """
    import socket

    try:
        socket.gethostbyname("registry")
    except socket.gaierror:
        print("\n" + "=" * 70)
        print("SETUP REQUIRED: Add registry hostname to /etc/hosts")
        print("=" * 70)
        print("\nWhen running tests from your host machine, run:")
        print("  echo '127.0.0.1 registry' | sudo tee -a /etc/hosts")
        print("\nThis allows both host (pushing) and Slurm container (pulling)")
        print("to resolve 'registry:20002' to the same registry service.")
        print("\nAlternatively, run tests inside the devcontainer where Docker")
        print("DNS handles resolution automatically.")
        print("=" * 70 + "\n")
        pytest.skip("Registry hostname not configured in /etc/hosts")

    return docker_compose_project["registry_url"]


@pytest.fixture(scope="session")
def registry_for_containers(docker_compose_project):
    """Return registry URL for containers using docker-compose DNS.

    Returns:
        str: Registry URL accessible from containers
    """
    return docker_compose_project["registry_url_container"]


@pytest.fixture(scope="session")
def pyxis_container(docker_compose_project):
    """Return Pyxis container connection info.

    Returns:
        dict: Container information including hostname, port, credentials
    """
    return {
        "runtime": docker_compose_project["container_runtime"],
        "hostname": docker_compose_project["pyxis_hostname"],
        "port": docker_compose_project["pyxis_port"],
        "username": docker_compose_project["pyxis_username"],
        "password": docker_compose_project["pyxis_password"],
        "name": docker_compose_project["pyxis_container_name"],
        "job_base_dir": f"/home/{SLURM_USER}/slurm_jobs",
        "partition": "debug",
    }


@pytest.fixture(scope="session")
def sdk_on_pyxis_cluster(docker_compose_project) -> Path:
    """Install the SDK on the Pyxis cluster container.

    This fixture requires docker CLI access to run docker exec commands.
    When running inside a devcontainer without socket access, tests using
    this fixture will be skipped.

    Returns:
        Path: Path to SDK installation on the container
    """
    if not DOCKER_CLI_AVAILABLE:
        pytest.skip(
            "Docker CLI not available (required for sdk_on_pyxis_cluster fixture)"
        )

    container_name = docker_compose_project["pyxis_container_name"]
    container_runtime = docker_compose_project["container_runtime"]
    sdk_root = Path(__file__).resolve().parents[2]

    subprocess.run(
        [container_runtime, "exec", container_name, "mkdir", "-p", "/tmp/slurm_sdk"],
        check=True,
        capture_output=True,
    )

    # Include docs and mkdocs.yml required by pyproject.toml force-include for bundled docs
    tar_proc = subprocess.Popen(
        [
            "tar",
            "-cf",
            "-",
            "-C",
            str(sdk_root),
            "src",
            "pyproject.toml",
            "README.md",
            "docs",
            "mkdocs.yml",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    assert tar_proc.stdout is not None
    assert tar_proc.stderr is not None
    exec_result = subprocess.run(
        [
            container_runtime,
            "exec",
            "-i",
            container_name,
            "tar",
            "-xf",
            "-",
            "-C",
            "/tmp/slurm_sdk",
        ],
        stdin=tar_proc.stdout,
        capture_output=True,
        check=False,
    )
    tar_proc.stdout.close()
    tar_stderr = tar_proc.stderr.read()
    tar_returncode = tar_proc.wait(timeout=60)

    if tar_returncode != 0:
        tar_error = tar_stderr.decode("utf-8", "replace")
        pytest.fail(f"Failed to create SDK tarball: {tar_error}")
    if exec_result.returncode != 0:
        exec_error = exec_result.stderr.decode("utf-8", "replace")
        pytest.fail(f"Failed to copy SDK files to Pyxis cluster: {exec_error}")

    # --break-system-packages required for Debian 12+
    result = subprocess.run(
        [
            container_runtime,
            "exec",
            container_name,
            "python3",
            "-m",
            "pip",
            "install",
            "-e",
            "/tmp/slurm_sdk",
            "--break-system-packages",
            "--quiet",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to install SDK on Pyxis cluster: {result.stderr}")

    return Path("/tmp/slurm_sdk")


@pytest.fixture(scope="session")
def slurm_pyxis_cluster_config(
    pyxis_container: Dict[str, str], sdk_on_pyxis_cluster: Path
) -> Dict:
    """Return Cluster configuration for the Pyxis cluster.

    Returns:
        dict: Configuration for the Pyxis cluster
    """
    return {
        "cluster": {
            "backend": "ssh",
            "job_base_dir": pyxis_container["job_base_dir"],
            "backend_config": {
                "hostname": pyxis_container["hostname"],
                "port": pyxis_container["port"],
                "username": pyxis_container["username"],
                "password": pyxis_container["password"],
                "look_for_keys": False,
                "allow_agent": False,
                "banner_timeout": 30,
            },
        },
        "packaging": {
            "type": "wheel",
            "python_executable": "/usr/bin/python3",
            "upgrade_pip": False,
        },
        "submit": {
            "partition": pyxis_container["partition"],
        },
    }


@pytest.fixture
def slurm_pyxis_cluster(slurm_pyxis_cluster_config: Dict, tmp_path: Path):
    """Return configured Cluster instance for Pyxis/enroot testing.

    Returns:
        Cluster: A configured Cluster instance ready to submit jobs
    """
    import textwrap
    from slurm.cluster import Cluster

    backend = slurm_pyxis_cluster_config["cluster"]["backend"]
    job_base_dir = slurm_pyxis_cluster_config["cluster"]["job_base_dir"]
    backend_config = slurm_pyxis_cluster_config["cluster"]["backend_config"]
    packaging = slurm_pyxis_cluster_config["packaging"]
    submit = slurm_pyxis_cluster_config["submit"]

    python_cfg_line = ""
    if "python_executable" in packaging and packaging["python_executable"]:
        python_cfg_line = (
            f'\n        python_executable = "{packaging["python_executable"]}"'
        )
    elif "python_version" in packaging and packaging["python_version"]:
        python_cfg_line = f'\n        python_version = "{packaging["python_version"]}"'

    upgrade_pip_line = ""
    if "upgrade_pip" in packaging:
        upgrade_pip_line = (
            f"\n        upgrade_pip = {str(packaging['upgrade_pip']).lower()}"
        )

    content = textwrap.dedent(
        f"""
        [default.cluster]
        backend = "{backend}"
        job_base_dir = "{job_base_dir}"

        [default.cluster.backend_config]
        hostname = "{backend_config["hostname"]}"
        port = {backend_config["port"]}
        username = "{backend_config["username"]}"
        password = "{backend_config["password"]}"
        look_for_keys = {str(backend_config.get("look_for_keys", False)).lower()}
        allow_agent = {str(backend_config.get("allow_agent", False)).lower()}
        banner_timeout = {backend_config.get("banner_timeout", 30)}

        [default.packaging]
        type = "{packaging["type"]}"{python_cfg_line}{upgrade_pip_line}

        [default.submit]
        partition = "{submit["partition"]}"
        """
    ).strip()

    slurmfile_path = tmp_path / "Slurmfile.toml"
    slurmfile_path.write_text(content, encoding="utf-8")

    return Cluster.from_env(slurmfile_path, env="default")


# ============================================================================
# Prebuilt Integration Test Image Fixture
# ============================================================================


def _get_integration_test_dockerfile() -> str:
    """Return the Dockerfile content for the integration test base image.

    Includes numpy for tests that verify dependency handling.
    """
    return """FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/
COPY tests/__init__.py tests/__init__.py
COPY tests/integration/__init__.py tests/integration/__init__.py
COPY tests/integration/container_test_tasks.py tests/integration/

RUN pip install --no-cache-dir . numpy

# Tests module isn't installed as a package, so add to PYTHONPATH
ENV PYTHONPATH=/workspace:$PYTHONPATH
"""


def _detect_host_platform() -> str:
    """Detect the host platform for container builds.

    Returns platform in Docker format (e.g., 'linux/amd64', 'linux/arm64').
    """
    import platform

    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "linux/arm64"
    elif machine in ("x86_64", "amd64"):
        return "linux/amd64"
    else:
        # Default to amd64 for unknown architectures
        return "linux/amd64"


def _is_podman_masquerading_as_docker(runtime: str) -> bool:
    """Check if docker command is actually podman (common on macOS).

    On macOS with podman-docker or Docker Desktop using podman backend,
    the 'docker' command might be podman. We need to detect this to use
    the correct push flags for insecure registries.
    """
    if runtime != "docker":
        return False

    try:
        # Check docker info for podman-specific fields
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = result.stdout.lower()
        # Look for podman-specific indicators in docker info output
        if "buildahversion" in output or "podman" in output:
            return True
    except Exception:
        pass

    return False


@pytest.fixture(scope="session")
def prebuilt_integration_image(docker_compose_project, local_registry, request):
    """Build and push a shared integration test image once per session.

    This fixture builds once at session start and all tests reuse the same image.

    Returns:
        dict: Image information including:
            - image_ref: Full image reference (registry:port/name:tag)
            - tag: The tag used
            - platform: The build platform
    """
    if not DOCKER_CLI_AVAILABLE:
        pytest.skip("Docker CLI not available (required for prebuilt image)")

    project_root = Path(__file__).parent.parent.parent
    tag = f"session-{os.getpid()}"
    image_name = "slurm-sdk/integration-test"
    image_ref = f"{local_registry}/{image_name}:{tag}"
    platform = _detect_host_platform()

    # Determine which container runtime to use
    container_runtime = docker_compose_project["container_runtime"]

    # Write Dockerfile
    dockerfile_path = project_root / "integration_test.Dockerfile"
    dockerfile_path.write_text(_get_integration_test_dockerfile())

    try:
        print(f"\nBuilding integration test image: {image_ref}")

        # Build the image
        build_cmd = [
            container_runtime,
            "build",
            "-t",
            image_ref,
            "-f",
            str(dockerfile_path),
            "--platform",
            platform,
            str(project_root),
        ]

        result = subprocess.run(
            build_cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode != 0:
            pytest.fail(
                f"Failed to build integration test image:\n"
                f"Command: {' '.join(build_cmd)}\n"
                f"stderr: {result.stderr}\n"
                f"stdout: {result.stdout}"
            )

        print(f"Pushing integration test image: {image_ref}")

        # Detect if docker is actually podman (common on macOS)
        is_podman = container_runtime == "podman" or _is_podman_masquerading_as_docker(
            container_runtime
        )

        # Push the image with appropriate flags for insecure registry
        push_cmd = [container_runtime, "push"]
        if is_podman:
            # Podman needs explicit flags for insecure registry and Docker format
            push_cmd.extend(["--tls-verify=false", "--format", "v2s2"])
        push_cmd.append(image_ref)

        result = subprocess.run(
            push_cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            hint = ""
            if not is_podman and "server gave HTTP response to HTTPS" in result.stderr:
                hint = (
                    "\n\nHint: Docker requires insecure registry configuration.\n"
                    "Add to /etc/docker/daemon.json:\n"
                    '  {"insecure-registries": ["registry:20002"]}\n'
                    "Then restart Docker."
                )
            pytest.fail(
                f"Failed to push integration test image:\n"
                f"Command: {' '.join(push_cmd)}\n"
                f"stderr: {result.stderr}\n"
                f"stdout: {result.stdout}{hint}"
            )

        print(f"Integration test image ready: {image_ref}")

    finally:
        # Clean up Dockerfile
        if dockerfile_path.exists():
            dockerfile_path.unlink()

    return {
        "image_ref": image_ref,
        "tag": tag,
        "platform": platform,
    }
