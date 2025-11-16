"""Advanced container packaging integration tests.

Tests advanced scenarios like array jobs and volume mounts using Pyxis and enroot
with the SDK's container packaging functionality.
"""

from __future__ import annotations

import pytest

from slurm.decorators import task
from tests.integration import container_test_tasks


@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_array_job_with_containers(
    slurm_pyxis_cluster,
    local_registry,  # registry:5000 - requires /etc/hosts entry: "127.0.0.1 registry"
    tmp_path,
):
    """Test array jobs using container packaging."""
    dockerfile_path = tmp_path / "array_test.Dockerfile"
    dockerfile_path.write_text("""FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ src/
COPY tests/__init__.py tests/__init__.py
COPY tests/integration/__init__.py tests/integration/__init__.py
COPY tests/integration/container_test_tasks.py tests/integration/

RUN pip install --no-cache-dir .

# Tests module isn't installed as a package, so add to PYTHONPATH
ENV PYTHONPATH=/workspace:$PYTHONPATH
""")

    process_item_packaged = task(
        packaging="container",
        packaging_dockerfile=str(dockerfile_path),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",  # Apple Silicon host
        packaging_tls_verify=False,  # Local registry uses HTTP
    )(container_test_tasks.process_item.unwrapped)

    with slurm_pyxis_cluster:
        items = [1, 2, 3, 4, 5]
        array_job = process_item_packaged.map(items)
        assert array_job.wait(timeout=300), "Array job did not complete"
        results = array_job.get_results()
        assert results == [2, 4, 6, 8, 10], f"Unexpected results: {results}"


@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_container_with_mounts(
    slurm_pyxis_cluster,
    local_registry,  # registry:5000 - requires /etc/hosts entry: "127.0.0.1 registry"
    tmp_path,
):
    """Test container with volume mounts."""
    # Volume mounts must reference paths on the cluster, not the local machine
    backend = slurm_pyxis_cluster.backend
    remote_data_dir = "/home/slurm/test_mount_data"
    remote_test_file = f"{remote_data_dir}/input.txt"

    backend.execute_command(f"mkdir -p {remote_data_dir}")
    backend.execute_command(
        f"echo 'test data from mounted volume' > {remote_test_file}"
    )

    dockerfile_path = tmp_path / "mount_test.Dockerfile"
    dockerfile_path.write_text("""FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ src/
COPY tests/__init__.py tests/__init__.py
COPY tests/integration/__init__.py tests/integration/__init__.py
COPY tests/integration/container_test_tasks.py tests/integration/

RUN pip install --no-cache-dir .

# Tests module isn't installed as a package, so add to PYTHONPATH
ENV PYTHONPATH=/workspace:$PYTHONPATH
""")

    read_mounted_file_packaged = task(
        packaging="container",
        packaging_dockerfile=str(dockerfile_path),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",  # Apple Silicon host
        packaging_tls_verify=False,  # Local registry uses HTTP
        packaging_mounts=[f"{remote_data_dir}:/data:ro"],
    )(container_test_tasks.read_mounted_file.unwrapped)

    try:
        with slurm_pyxis_cluster:
            job = slurm_pyxis_cluster.submit(read_mounted_file_packaged)(
                file_path="/data/input.txt"
            )
            assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
            result = job.get_result()
            assert result.strip() == "test data from mounted volume"
    finally:
        backend.execute_command(f"rm -rf {remote_data_dir}")
