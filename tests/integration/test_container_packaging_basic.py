"""Basic container packaging integration tests.

Tests basic container execution using Pyxis and enroot with the SDK's
container packaging functionality.
"""

from __future__ import annotations

import pytest

from slurm.decorators import task
from tests.integration import container_test_tasks


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_basic_container_task_execution(
    slurm_pyxis_cluster,
    local_registry,  # registry:5000 - requires /etc/hosts entry: "127.0.0.1 registry"
    tmp_path,
):
    """Test that a task runs successfully in a container via Pyxis."""
    dockerfile_path = tmp_path / "basic_test.Dockerfile"
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

    hello_container_packaged = task(
        packaging="container",
        packaging_dockerfile=str(dockerfile_path),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",  # Apple Silicon host
        packaging_tls_verify=False,  # Local registry uses HTTP
    )(container_test_tasks.hello_container.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_container_packaged)()
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert "Hello from" in result, f"Unexpected result: {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_container_with_dependencies(
    slurm_pyxis_cluster,
    local_registry,  # registry:5000 - requires /etc/hosts entry: "127.0.0.1 registry"
    tmp_path,
):
    """Test container task with Python dependencies (numpy)."""
    dockerfile_path = tmp_path / "numpy_test.Dockerfile"
    dockerfile_path.write_text("""FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ src/
COPY tests/__init__.py tests/__init__.py
COPY tests/integration/__init__.py tests/integration/__init__.py
COPY tests/integration/container_test_tasks.py tests/integration/

RUN pip install --no-cache-dir . numpy

# Tests module isn't installed as a package, so add to PYTHONPATH
ENV PYTHONPATH=/workspace:$PYTHONPATH
""")

    numpy_task_packaged = task(
        packaging="container",
        packaging_dockerfile=str(dockerfile_path),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",  # Apple Silicon host
        packaging_tls_verify=False,  # Local registry uses HTTP
    )(container_test_tasks.numpy_task.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(numpy_task_packaged)()
        # Longer timeout needed for image build + task execution
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert result == 6, f"Unexpected result: {result}"
