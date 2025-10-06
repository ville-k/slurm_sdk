from __future__ import annotations

import textwrap

import pytest

from slurm.cluster import Cluster
from slurm.examples.integration_test_task import simple_integration_task


@pytest.mark.integration_test
def test_slurm_services_running(slurm_testinfra):
    result = slurm_testinfra.run("sinfo")
    assert result.rc == 0, result.stderr
    assert "debug" in result.stdout

    procs = slurm_testinfra.check_output("pgrep -f slurmctld")
    assert procs.strip(), "slurmctld must be running"


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_submit_job_over_ssh(slurm_cluster):
    import logging

    logging.basicConfig(level=logging.DEBUG)

    job = slurm_cluster.submit(simple_integration_task)()
    result = job.wait(timeout=180, poll_interval=5)
    print(result)

    assert job.is_successful()
    assert job.get_result() == "integration-ok"


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_submit_container_job_with_multiword_python_executable(
    slurm_cluster_config, tmp_path, slurm_testinfra
):
    """Test that multi-word python executables like 'uv run python' work in containers.

    This reproduces the real-world scenario where the user wants to use 'uv run python'
    inside a container via Pyxis/Enroot.
    """
    import logging

    logging.basicConfig(level=logging.DEBUG)

    # First, build a test container with uv installed
    # We'll use a simple python image and install uv in it
    dockerfile_content = textwrap.dedent(
        """
        FROM python:3.11-slim

        # Install uv
        ADD https://astral.sh/uv/install.sh /uv-installer.sh
        RUN sh /uv-installer.sh && rm /uv-installer.sh
        ENV PATH="/root/.cargo/bin:$PATH"

        # Create a minimal uv project
        WORKDIR /app
        RUN echo '[project]' > pyproject.toml && \\
            echo 'name = "test"' >> pyproject.toml && \\
            echo 'version = "0.1.0"' >> pyproject.toml && \\
            echo 'requires-python = ">=3.11"' >> pyproject.toml

        # Create empty __init__.py so uv sync works
        RUN mkdir test && touch test/__init.py

        # Run uv sync to create .venv
        RUN uv sync --no-dev

        CMD ["uv", "run", "python", "--version"]
        """
    )

    # Write dockerfile
    dockerfile_path = tmp_path / "Dockerfile.uvtest"
    dockerfile_path.write_text(dockerfile_content)

    # Build the image inside the SLURM container
    # We need to use the container runtime available in the integration environment
    build_result = slurm_testinfra.run(
        f"podman build -t uvtest:latest -f - {tmp_path} < {dockerfile_path}"
    )

    if build_result.rc != 0:
        pytest.skip(f"Failed to build test container: {build_result.stderr}")

    # Create Slurmfile with container packaging
    slurmfile_content = textwrap.dedent(
        f"""
        [default.cluster]
        backend = "ssh"
        job_base_dir = "{slurm_cluster_config["cluster"]["job_base_dir"]}"
        
        [default.cluster.backend_config]
        hostname = "{slurm_cluster_config["cluster"]["backend_config"]["hostname"]}"
        port = {slurm_cluster_config["cluster"]["backend_config"]["port"]}
        username = "{slurm_cluster_config["cluster"]["backend_config"]["username"]}"
        password = "{slurm_cluster_config["cluster"]["backend_config"]["password"]}"
        look_for_keys = false
        allow_agent = false
        banner_timeout = {slurm_cluster_config["cluster"]["backend_config"]["banner_timeout"]}
        
        [default.packaging]
        type = "container"
        image = "uvtest:latest"
        python_executable = "uv run python"
        
        [default.submit]
        partition = "{slurm_cluster_config["submit"]["partition"]}"
        """
    ).strip()

    slurmfile_path = tmp_path / "Slurmfile.toml"
    slurmfile_path.write_text(slurmfile_content)

    # Submit the job
    cluster = Cluster.from_env(slurmfile_path, env="default")

    job = cluster.submit(simple_integration_task)()
    result = job.wait(timeout=180, poll_interval=5)
    print(result)

    assert job.is_successful()
    assert job.get_result() == "integration-ok"
