"""Integration tests for job script persistence."""

import pytest
from pathlib import Path


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_job_script_persisted_in_job_directory(slurm_cluster):
    """Test that job scripts are persisted in the job directory."""
    from slurm.examples.hello_world import hello_world

    # Submit a job
    job = slurm_cluster.submit(hello_world)()

    # Wait for completion
    success = job.wait(timeout=180, poll_interval=5)
    assert success, f"Job failed: {job.get_status()}"

    # Verify script exists in job directory
    expected_script_path = (
        Path(job.target_job_dir) / f"slurm_job_{job.pre_submission_id}_script.sh"
    )

    if slurm_cluster.backend.is_remote():
        # For SSH backend, check remotely
        script_exists = slurm_cluster.backend.execute_command(
            f"test -f {expected_script_path} && echo 'exists' || echo 'missing'"
        ).strip()
        assert script_exists == "exists", f"Script not found at {expected_script_path}"

        # Verify script content via get_script()
        script_content = job.get_script()
        assert script_content is not None
        assert "#!/bin/bash" in script_content
        assert "#SBATCH" in script_content
        assert "slurm.runner" in script_content
    else:
        # For local backend, check directly
        assert expected_script_path.exists(), (
            f"Script not found at {expected_script_path}"
        )

        # Verify script content via get_script()
        script_content = job.get_script()
        assert script_content is not None
        assert "#!/bin/bash" in script_content
        assert "#SBATCH" in script_content
        assert "slurm.runner" in script_content


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_job_get_script_retrieves_persisted_script(slurm_cluster):
    """Test that Job.get_script() can retrieve the persisted script."""
    from slurm.examples.hello_world import hello_world

    # Submit a job
    job = slurm_cluster.submit(hello_world)()

    # Wait for completion
    success = job.wait(timeout=180, poll_interval=5)
    assert success, f"Job failed: {job.get_status()}"

    # Retrieve script via get_script()
    script = job.get_script()

    # Verify script content
    assert script is not None
    assert len(script) > 0
    assert "#!/bin/bash" in script
    assert f'--pre-submission-id "{job.pre_submission_id}"' in script
    assert "slurm.runner" in script
