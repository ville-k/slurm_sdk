"""Integration test fixtures for Slurm container tests."""

import os
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import Dict

import pytest


CONTEXT_DIR = Path(__file__).resolve().parents[2] / "containers" / "slurm-integration"
TEST_ID = uuid.uuid4().hex[:8]
IMAGE_TAG = f"slurm-integration:{TEST_ID}"
CONTAINER_NAME = f"slurm-integration-{TEST_ID}"
SLURM_USER = "slurm"


def _find_runtime() -> str:
    """Find available container runtime (podman or docker)."""
    for runtime in ("podman", "docker"):
        try:
            result = subprocess.run(
                [runtime, "info"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if result.returncode == 0:
                return runtime
        except FileNotFoundError:
            continue
    pytest.skip("No container runtime (podman/docker) available")


def _build_image(runtime: str) -> str:
    """Build the Slurm integration container image."""
    build_cmd = [
        runtime,
        "build",
        "-t",
        IMAGE_TAG,
        "-f",
        str(CONTEXT_DIR / "Containerfile"),
        str(CONTEXT_DIR),
    ]
    result = subprocess.run(
        build_cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(
            f"Failed to build container image (exit {result.returncode})\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    return IMAGE_TAG


def _start_container(runtime: str, image: str) -> str:
    """Start the Slurm container with systemd support."""
    run_cmd = [
        runtime,
        "run",
        "-d",
        "--name",
        CONTAINER_NAME,
        "--hostname",
        "slurm-control",
        "--privileged",  # Required for systemd
        "--systemd=always",  # Enable systemd mode
        "--tmpfs",
        "/run",
        "--tmpfs",
        "/run/lock",
        "-v",
        "/sys/fs/cgroup:/sys/fs/cgroup:rw",  # Required for systemd/cgroups
        "-p",
        ":22",  # Map SSH port to random host port
        "-p",
        ":6817",  # Slurmctld port
        "-p",
        ":6818",  # Slurmd port
        image,
    ]

    result = subprocess.run(
        run_cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to start container: {result.stderr}")

    return CONTAINER_NAME


def _get_port(runtime: str, container: str, container_port: int) -> int:
    """Get the host port mapped to a container port."""
    result = subprocess.run(
        [runtime, "port", container, str(container_port)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to get port mapping: {result.stderr}")

    # Output format: "0.0.0.0:PORT" or ":::PORT"
    port_line = result.stdout.strip().split("\n")[0]
    port = int(port_line.split(":")[-1])
    return port


def _wait_for_ssh(host: str, port: int, timeout: int = 30) -> None:
    """Wait for SSH to be available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2) as sock:
                sock.settimeout(2)
                banner = sock.recv(256)
                if b"SSH" in banner:
                    return
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(0.5)
    raise TimeoutError(f"SSH not available on {host}:{port} after {timeout}s")


def _wait_for_slurm(runtime: str, container: str, timeout: int = 60) -> None:
    """Wait for Slurm services to be ready."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        # Check if slurmctld is running
        result = subprocess.run(
            [runtime, "exec", container, "pgrep", "-f", "slurmctld"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if result.returncode == 0 and result.stdout.strip():
            # slurmctld is running, check if cluster is responsive
            result = subprocess.run(
                [runtime, "exec", container, "sinfo", "-h"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if result.returncode == 0:
                return

        time.sleep(1)

    raise TimeoutError(f"Slurm services not ready after {timeout}s")


def _stop_container(runtime: str, container: str) -> None:
    """Stop and remove the container."""
    subprocess.run([runtime, "rm", "-f", container], check=False)


@pytest.fixture(scope="session")
def container_runtime() -> str:
    """Provides the container runtime (podman or docker)."""
    return _find_runtime()


@pytest.fixture(scope="session")
def slurm_image(container_runtime: str) -> str:
    """Builds the Slurm integration container image."""
    return _build_image(container_runtime)


@pytest.fixture(scope="session")
def slurm_container(container_runtime: str, slurm_image: str):
    """
    Starts the Slurm container and provides connection information.

    Yields a dict with:
        - name: container name
        - runtime: container runtime command
        - hostname: SSH hostname
        - port: SSH port
        - username: SSH username
        - password: SSH password
        - job_base_dir: Base directory for Slurm jobs
        - partition: Default Slurm partition
    """
    container_name = None

    try:
        # Start container
        container_name = _start_container(container_runtime, slurm_image)

        # Wait for Slurm to be ready
        _wait_for_slurm(container_runtime, container_name, timeout=60)

        # Get SSH port
        ssh_port = _get_port(container_runtime, container_name, 22)

        # Wait for SSH to be available
        _wait_for_ssh("127.0.0.1", ssh_port, timeout=30)

        # Yield container info
        yield {
            "name": container_name,
            "runtime": container_runtime,
            "hostname": "127.0.0.1",
            "port": ssh_port,
            "username": SLURM_USER,
            "password": SLURM_USER,
            "job_base_dir": f"/home/{SLURM_USER}/slurm_jobs",
            "partition": "debug",
        }

    finally:
        # Cleanup unless SLURM_TEST_KEEP is set
        if container_name and os.environ.get("SLURM_TEST_KEEP") != "1":
            _stop_container(container_runtime, container_name)


@pytest.fixture(scope="session")
def slurm_cluster_config(slurm_container):
    """Configuration dict for Cluster.from_env()."""
    return {
        "cluster": {
            "backend": "ssh",
            "job_base_dir": slurm_container["job_base_dir"],
            "backend_config": {
                "hostname": slurm_container["hostname"],
                "port": slurm_container["port"],
                "username": slurm_container["username"],
                "password": slurm_container["password"],
                "look_for_keys": False,
                "allow_agent": False,
                "banner_timeout": 30,
            },
        },
        "packaging": {
            "type": "wheel",
            "python_executable": "/usr/bin/python3.11",
            "upgrade_pip": False,
        },
        "submit": {
            "partition": slurm_container["partition"],
        },
    }


@pytest.fixture(scope="session")
def slurm_testinfra(slurm_container):
    """Testinfra connection to the Slurm container."""
    testinfra = pytest.importorskip("testinfra")

    # Use docker/podman backend to connect directly to the container
    # This avoids SSH authentication issues
    backend = f"{slurm_container['runtime']}://{slurm_container['name']}"

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

REGISTRY_PORT = 5000


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


COMPOSE_FILE = Path(__file__).parent / "docker-compose.yml"


@pytest.fixture(scope="session")
def docker_compose_project(request):
    """Start docker-compose services and provide connection info.

    Returns:
        dict: Connection info for registry and Pyxis services
    """
    print("\nStarting docker-compose services...")
    result = subprocess.run(
        ["docker-compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"],
        cwd=COMPOSE_FILE.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to start docker-compose services:\n{result.stderr}")

    max_retries = 60
    for i in range(max_retries):
        result = subprocess.run(
            [
                "docker-compose",
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
        if "registry" in running_services and "slurm-pyxis" in running_services:
            print(f"Services running: {running_services}")
            break
        time.sleep(1)
    else:
        subprocess.run(
            ["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
        )
        pytest.fail("Docker compose services did not start within timeout")

    # Wait for systemd to initialize
    time.sleep(10)
    for i in range(30):
        result = subprocess.run(
            ["podman", "exec", "slurm-test-pyxis", "systemctl", "is-active", "sshd"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and "active" in result.stdout:
            print("SSH service is active in Pyxis container")
            break
        time.sleep(1)
    else:
        subprocess.run(
            ["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=COMPOSE_FILE.parent,
            check=False,
        )
        pytest.fail("SSH service did not start in Pyxis container")

    info = {
        "registry_url": "registry:5000",
        "registry_url_container": "registry:5000",
        "pyxis_hostname": "127.0.0.1",
        "pyxis_port": 2222,
        "pyxis_username": SLURM_USER,
        "pyxis_password": SLURM_USER,
        "pyxis_container_name": "slurm-test-pyxis",
    }

    def cleanup():
        if not os.environ.get("SLURM_TEST_KEEP"):
            print("\nStopping docker-compose services...")
            subprocess.run(
                ["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"],
                cwd=COMPOSE_FILE.parent,
                check=False,
                capture_output=True,
            )
        else:
            print("\nKeeping docker-compose services (SLURM_TEST_KEEP set)")

    request.addfinalizer(cleanup)

    return info


@pytest.fixture(scope="session")
def local_registry(docker_compose_project):
    """Return registry URL, checking that /etc/hosts is configured.

    Returns:
        str: Registry URL (e.g., "registry:5000")
    """
    import socket

    try:
        socket.gethostbyname("registry")
    except socket.gaierror:
        print("\n" + "=" * 70)
        print("SETUP REQUIRED: Add registry hostname to /etc/hosts")
        print("=" * 70)
        print("\nTo use container packaging tests, run:")
        print("  echo '127.0.0.1 registry' | sudo tee -a /etc/hosts")
        print("\nThis allows both host (for building/pushing) and containers")
        print("(for pulling) to access the test registry at registry:5000")
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
        "runtime": _find_runtime(),
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

    Returns:
        Path: Path to SDK installation on the container
    """
    container_name = docker_compose_project["pyxis_container_name"]
    sdk_root = Path(__file__).resolve().parents[2]

    subprocess.run(
        ["podman", "exec", container_name, "mkdir", "-p", "/tmp/slurm_sdk"],
        check=True,
        capture_output=True,
    )

    tar_cmd = f"tar -cf - -C {sdk_root} src pyproject.toml README.md | podman exec -i {container_name} tar -xf - -C /tmp/slurm_sdk"
    result = subprocess.run(
        tar_cmd, shell=True, capture_output=True, text=True, check=False
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to copy SDK files to Pyxis cluster: {result.stderr}")

    # --break-system-packages required for Debian 12+
    result = subprocess.run(
        [
            "podman",
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
