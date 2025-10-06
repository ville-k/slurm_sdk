"""Integration test for TaskContext.output_dir functionality.

This test verifies that:
1. Tasks can write files to task.output_dir
2. Tasks can return the path to files written
3. The submitter can fetch and inspect the written files
"""

from __future__ import annotations

from pathlib import Path

import pytest

from slurm.examples.integration_test_task import (
    write_output_file_task,
    get_output_dir_task,
)


@pytest.mark.slow_integration_test
def test_output_dir_write_and_read(slurm_cluster):
    """Test that a task can write to output_dir and the submitter can read the file.

    This test verifies the complete workflow:
    1. Submit a task that writes a file to task.output_dir
    2. The task returns the Path to the written file
    3. Wait for the job to complete
    4. Retrieve the result (which is the Path)
    5. Download and verify the file contents via SSH backend
    """
    import logging
    import json

    logging.basicConfig(level=logging.DEBUG)

    # Submit the task with a test message
    test_message = "Hello from integration test!"
    job = slurm_cluster.submit(write_output_file_task)(test_message)

    # Wait for completion
    success = job.wait(timeout=180, poll_interval=5)

    # If job failed, download and print logs for debugging
    if not success:
        import tempfile

        status = job.get_status()
        stderr_path = status.get("StdErr", "")
        stdout_path = status.get("StdOut", "")

        print(f"\n=== JOB FAILED - Status: {status} ===")

        if stderr_path:
            try:
                with tempfile.NamedTemporaryFile(mode="r", delete=False) as f:
                    temp_stderr = f.name
                slurm_cluster.backend.download_file(stderr_path, temp_stderr)
                with open(temp_stderr, "r") as f:
                    print(f"\n=== STDERR ({stderr_path}) ===")
                    print(f.read())
                import os

                os.unlink(temp_stderr)
            except Exception as e:
                print(f"Could not download stderr: {e}")

        if stdout_path:
            try:
                with tempfile.NamedTemporaryFile(mode="r", delete=False) as f:
                    temp_stdout = f.name
                slurm_cluster.backend.download_file(stdout_path, temp_stdout)
                with open(temp_stdout, "r") as f:
                    print(f"\n=== STDOUT ({stdout_path}) ===")
                    print(f.read())
                import os

                os.unlink(temp_stdout)
            except Exception as e:
                print(f"Could not download stdout: {e}")

    assert success, f"Job failed or timed out. Status: {job.get_status()}"
    assert job.is_successful(), f"Job completed but not successful: {job.get_status()}"

    # Get the result - this should be the Path to the written file
    result_path = job.get_result()
    assert isinstance(result_path, Path), f"Expected Path, got {type(result_path)}"
    assert result_path.name == "test_output.txt"

    # Now verify we can actually read the file
    import tempfile

    # Download the text file
    with tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".txt") as f:
        local_text_path = f.name

    slurm_cluster.backend.download_file(str(result_path), local_text_path)

    # Read and verify the text file content
    with open(local_text_path, "r") as f:
        content = f.read()

    assert f"Message: {test_message}" in content
    assert "Job ID:" in content

    # Also download and verify the JSON file
    json_remote_path = str(result_path).replace("test_output.txt", "metadata.json")
    with tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".json") as f:
        local_json_path = f.name

    slurm_cluster.backend.download_file(json_remote_path, local_json_path)

    # Read and verify the JSON file
    with open(local_json_path, "r") as f:
        metadata = json.load(f)

    assert metadata["message"] == test_message
    assert metadata["job_id"] == job.id
    assert "output_dir" in metadata
    assert metadata["output_dir"] == str(result_path.parent)

    # Cleanup temp files
    import os

    os.unlink(local_text_path)
    os.unlink(local_json_path)


@pytest.mark.slow_integration_test
def test_output_dir_is_set_correctly(slurm_cluster):
    """Test that job.output_dir is correctly populated from JOB_DIR."""
    import logging

    logging.basicConfig(level=logging.DEBUG)

    # Submit task
    job = slurm_cluster.submit(get_output_dir_task)()

    # Wait for completion
    success = job.wait(timeout=180, poll_interval=5)
    assert success, f"Job failed or timed out. Status: {job.get_status()}"

    # Get the result
    result = job.get_result()

    # Verify output_dir is set
    assert result["output_dir"] is not None, "output_dir should not be None"
    assert result["output_dir_exists"], "output_dir should exist"
    assert result["job_id"] == job.id

    # Verify it matches the job's target_job_dir
    if job.target_job_dir:
        assert result["output_dir"] == job.target_job_dir, (
            f"output_dir {result['output_dir']} should match "
            f"target_job_dir {job.target_job_dir}"
        )
