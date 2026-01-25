"""Advanced container packaging integration tests.

Tests advanced scenarios like array jobs and volume mounts using Pyxis and enroot
with the SDK's container packaging functionality.

NOTE: These tests use a shared prebuilt image (built once per session via the
prebuilt_integration_image fixture) to avoid redundant image builds.
"""

from __future__ import annotations

import pytest

from slurm.decorators import task
from tests.integration import container_test_tasks


def _container_config(prebuilt_image: dict, **extra) -> dict:
    """Return common container packaging kwargs for tests using prebuilt image."""
    config = {
        "packaging": "container",
        "packaging_image": prebuilt_image["image_ref"],
        "packaging_push": False,
        "packaging_platform": prebuilt_image["platform"],
        "packaging_tls_verify": False,
    }
    config.update(extra)
    return config


@pytest.mark.container_runtime
@pytest.mark.integration_test
def test_array_job_with_containers(
    slurm_pyxis_cluster,
    prebuilt_integration_image,
):
    """Test array jobs using container packaging."""
    config = _container_config(prebuilt_integration_image)
    process_item_packaged = task(**config)(container_test_tasks.process_item.unwrapped)

    with slurm_pyxis_cluster:
        items = [1, 2, 3, 4, 5]
        array_job = process_item_packaged.map(items)
        assert array_job.wait(timeout=300), "Array job did not complete"
        results = array_job.get_results()
        assert results == [2, 4, 6, 8, 10], f"Unexpected results: {results}"


@pytest.mark.container_runtime
@pytest.mark.integration_test
def test_container_with_mounts(
    slurm_pyxis_cluster,
    prebuilt_integration_image,
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

    # Use prebuilt image with volume mount
    config = _container_config(
        prebuilt_integration_image,
        packaging_mounts=[f"{remote_data_dir}:/data:ro"],
    )
    read_mounted_file_packaged = task(**config)(
        container_test_tasks.read_mounted_file.unwrapped
    )

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
