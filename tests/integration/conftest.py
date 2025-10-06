"""Integration test fixtures for Slurm container tests."""

import os
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

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
