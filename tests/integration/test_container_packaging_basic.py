"""Basic container packaging integration tests.

Tests basic container execution using Pyxis and enroot with the SDK's
container packaging functionality.

NOTE: These tests use a shared prebuilt image (built once per session via the
prebuilt_integration_image fixture) to avoid redundant image builds.
"""

from __future__ import annotations

import pytest

from slurm.decorators import task
from tests.integration import container_test_tasks


def _container_config(prebuilt_image: dict) -> dict:
    """Return common container packaging kwargs for tests using prebuilt image."""
    return {
        "packaging": "container",
        "packaging_image": prebuilt_image["image_ref"],
        "packaging_push": False,
        "packaging_platform": prebuilt_image["platform"],
        "packaging_tls_verify": False,
    }


@pytest.mark.container_runtime
@pytest.mark.slow_integration_test
def test_basic_container_task_execution(
    slurm_pyxis_cluster,
    prebuilt_integration_image,
):
    """Test that a task runs successfully in a container via Pyxis."""
    config = _container_config(prebuilt_integration_image)
    hello_container_packaged = task(**config)(
        container_test_tasks.hello_container.unwrapped
    )

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_container_packaged)()
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert "Hello from" in result, f"Unexpected result: {result}"


@pytest.mark.container_runtime
@pytest.mark.slow_integration_test
def test_container_with_dependencies(
    slurm_pyxis_cluster,
    prebuilt_integration_image,
):
    """Test container task with Python dependencies (numpy).

    The prebuilt integration image includes numpy, so this test verifies
    that external dependencies work correctly in the container.
    """
    config = _container_config(prebuilt_integration_image)
    numpy_task_packaged = task(**config)(container_test_tasks.numpy_task.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(numpy_task_packaged)()
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert result == 6, f"Unexpected result: {result}"
