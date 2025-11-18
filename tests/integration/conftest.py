"""Integration test fixtures for Slurm container tests."""

import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict

import pytest


SLURM_USER = "slurm"


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
    """
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
        if "registry" in running_services and "slurm" in running_services:
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
            ["podman", "exec", "slurm-test", "systemctl", "is-active", "sshd"],
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
        "pyxis_container_name": "slurm-test",
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
        "runtime": "podman",  # Docker-compose uses podman for container operations
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
