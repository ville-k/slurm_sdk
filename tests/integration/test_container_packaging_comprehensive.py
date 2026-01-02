"""Comprehensive container packaging integration tests.

Tests workflow, callback, output directory, and script persistence scenarios
using container packaging to achieve parity with wheel packaging tests.

NOTE: These tests use a shared Docker image (built once per session) to avoid
the overhead of building separate images for each test.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import pytest
import tempfile
import time

from slurm.callbacks import BenchmarkCallback, LoggerCallback
from slurm.decorators import task
from slurm.decorators import workflow
from slurm.examples import container_test_functions
from tests.integration import container_test_tasks


# ============================================================================
# Shared Dockerfile Fixture (Built Once Per Session)
# ============================================================================


@pytest.fixture(scope="session")
def shared_comprehensive_dockerfile():
    """Create a shared Dockerfile for all comprehensive tests.

    Built once per test session to avoid redundant image builds.
    Creates Dockerfile in project root for proper build context.
    """
    project_root = Path(__file__).parent.parent.parent
    dockerfile_path = project_root / "comprehensive_test.Dockerfile"

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

    yield dockerfile_path

    # Cleanup after session
    if dockerfile_path.exists():
        os.unlink(dockerfile_path)


# ============================================================================
# Workflow Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_workflow_with_param_no_context(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test containerized workflow with parameter but NO WorkflowContext.

    This isolates whether the issue is with parameters in general
    or specifically with WorkflowContext.
    """
    containerized_workflow = workflow(
        time="00:02:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.workflow_with_param_no_context.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(containerized_workflow)(5)
        assert job.wait(timeout=60), f"Workflow did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert result == 15, f"Expected 15 (5*3), got {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_workflow_with_context(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test containerized workflow WITH WorkflowContext parameter.

    This tests that workflows with WorkflowContext complete properly
    and don't hang due to unclosed SSH connections.
    """
    containerized_workflow = workflow(
        time="00:02:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.simple_value_workflow.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(containerized_workflow)(10)
        assert job.wait(timeout=60), f"Workflow did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert result == 20, f"Expected 20 (10*2), got {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_minimal_containerized_workflow(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test the absolute minimal containerized workflow for debugging.

    This workflow takes no parameters and just returns a constant.
    Used to isolate workflow completion issues with container packaging.
    """
    # Wrap the minimal workflow with container packaging
    containerized_workflow = workflow(
        time="00:02:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.minimal_workflow.unwrapped)

    with slurm_pyxis_cluster:
        # Submit the containerized workflow
        job = slurm_pyxis_cluster.submit(containerized_workflow)()

        # Check job status periodically
        print(f"\nJob ID: {job.id}")
        print(f"Job dir: {job.target_job_dir}")

        # Wait a bit for job to start
        time.sleep(10)

        # Check Slurm job status
        backend = slurm_pyxis_cluster.backend
        status_cmd = f"squeue -j {job.id} -o '%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R'"
        print(f"\nJob status:\n{backend.execute_command(status_cmd)}")

        # Get stderr to see what's happening
        print(f"\nJob stderr so far:\n{job.get_stderr()}")

        # Try to wait for completion
        if not job.wait(timeout=180):
            # If timeout, check status again
            print(f"\nJob status after timeout:\n{backend.execute_command(status_cmd)}")
            print(f"\nFull stderr:\n{job.get_stderr()}")

            # Check if job is still running
            sacct_cmd = f"sacct -j {job.id} --format=JobID,State,ExitCode -P"
            print(f"\nJob accounting:\n{backend.execute_command(sacct_cmd)}")

            assert False, "Workflow did not complete within timeout"

        result = job.get_result()
        assert result == 42, f"Expected 42, got {result}"


# ============================================================================
# Task Execution Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_simple_task_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that simple tasks execute correctly with container packaging."""
    # Containerize the add task
    container_add = task(
        time="00:01:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.container_add)

    with slurm_pyxis_cluster:
        # Submit containerized task
        job = slurm_pyxis_cluster.submit(container_add)(5, 10)
        assert job.wait(timeout=300), f"Task did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert result == 15, f"Expected 15, got {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_multiple_tasks_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test executing multiple containerized tasks sequentially."""
    # Containerize tasks
    container_add = task(
        time="00:01:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.container_add)

    container_multiply = task(
        time="00:01:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.container_multiply)

    with slurm_pyxis_cluster:
        # Submit first task
        job1 = slurm_pyxis_cluster.submit(container_add)(5, 10)
        assert job1.wait(timeout=300), "First task did not complete"
        result1 = job1.get_result()
        assert result1 == 15

        # Submit second task using result from first
        job2 = slurm_pyxis_cluster.submit(container_multiply)(result1, 2)
        assert job2.wait(timeout=300), "Second task did not complete"
        result2 = job2.get_result()
        # (5 + 10) * 2 = 30
        assert result2 == 30, f"Expected 30, got {result2}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_exception_handling_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test exception handling in containerized tasks."""
    # Containerize failing task
    container_failing_task = task(
        time="00:01:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.container_failing_task)

    with slurm_pyxis_cluster:
        # Submit the failing task
        job = slurm_pyxis_cluster.submit(container_failing_task)()
        job.wait(timeout=300)

        # Job should fail
        assert not job.is_successful(), "Expected task to fail but it succeeded"


# ============================================================================
# Callback Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_callbacks_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that callbacks work correctly with container packaging."""
    logging.basicConfig(level=logging.INFO)

    hello_containerized = task(
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_tasks.hello_container.unwrapped)

    # Create callbacks
    benchmark = BenchmarkCallback()
    logger = LoggerCallback()

    # Add callbacks to cluster
    slurm_pyxis_cluster.callbacks.extend([benchmark, logger])

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_containerized)()
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert "Hello from" in result

        # Verify job completed successfully
        # Note: BenchmarkCallback logs metrics but doesn't expose a jobs list
        # This test verifies that callbacks can be used with container packaging
        assert job.is_successful(), "Job should have completed successfully"


# ============================================================================
# Output Directory Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_output_dir_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test output directory functionality with container packaging."""

    write_output_containerized = task(
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_tasks.write_output_file.unwrapped)

    with slurm_pyxis_cluster:
        # Write output file and get the path back
        write_job = slurm_pyxis_cluster.submit(write_output_containerized)(
            "test_output.txt", "container packaging test data"
        )
        assert write_job.wait(timeout=300), "Write job did not complete"

        # Get the remote file path from the job result
        remote_file_path = write_job.get_result()
        assert remote_file_path is not None, "Write job returned None"

        # Download and verify the file directly from the remote path
        with tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".txt") as f:
            local_path = f.name

        slurm_pyxis_cluster.backend.download_file(remote_file_path, local_path)

        with open(local_path, "r") as f:
            content = f.read()

        assert content == "container packaging test data", (
            f"Unexpected content: {content}"
        )

        # Cleanup
        import os

        os.unlink(local_path)


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_output_dir_path_correctness_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that output_dir paths are correctly set with container packaging."""
    get_output_dir_containerized = task(
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_tasks.get_output_dir_path.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(get_output_dir_containerized)()
        timeout = int(os.environ.get("SLURM_TEST_JOB_TIMEOUT", "900"))
        if not job.wait(timeout=timeout):
            status = job.get_status()
            try:
                stdout = job.get_stdout()
            except Exception as exc:
                stdout = f"<stdout unavailable: {exc}>"
            try:
                stderr = job.get_stderr()
            except Exception as exc:
                stderr = f"<stderr unavailable: {exc}>"
            pytest.fail(
                f"Job did not complete successfully within {timeout}s.\n"
                f"Status: {status}\n"
                f"--- Stdout ---\n{stdout}\n"
                f"--- Stderr ---\n{stderr}\n"
            )
        output_dir_path = job.get_result()

        # Verify output_dir was set and is a valid path
        assert output_dir_path is not None, "output_dir was None"
        assert len(output_dir_path) > 0, "output_dir was empty"

        # Verify the path contains expected structure
        assert "slurm_jobs" in output_dir_path or "output" in output_dir_path.lower()


# ============================================================================
# Script Persistence Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_job_script_persistence_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that job scripts are persisted with container packaging."""
    hello_containerized = task(
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_tasks.hello_container.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_containerized)()
        assert job.wait(timeout=300), "Job did not complete"

        # Verify job script exists in job directory
        backend = slurm_pyxis_cluster.backend
        job_dir = job.target_job_dir

        # Check for job script file (named with pattern: slurm_job_<id>_script.sh)
        ls_output = backend.execute_command(f"ls {job_dir}/")
        assert "_script.sh" in ls_output, (
            f"Job script not found in {job_dir}. Files: {ls_output}"
        )


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_job_get_script_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that job.get_script() retrieves persisted script with container packaging."""
    hello_containerized = task(
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_tasks.hello_container.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_containerized)()
        assert job.wait(timeout=300), "Job did not complete"

        # Retrieve job script
        script_content = job.get_script()

        # Verify script contains expected elements
        assert script_content is not None, "get_script() returned None"
        assert len(script_content) > 0, "get_script() returned empty string"
        assert "#!/bin/bash" in script_content or "#SBATCH" in script_content, (
            "Script doesn't contain expected job script markers"
        )


# ============================================================================
# Advanced Workflow Tests with Container Packaging
# ============================================================================


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_workflow_submitting_tasks(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test containerized workflow that submits child tasks.

    This tests that:
    1. Workflow runs in container
    2. Child tasks receive the packaging config via SLURM_SDK_PACKAGING_CONFIG
    3. Child tasks reuse the parent's container image (no rebuild)
    """

    containerized_workflow = workflow(
        time="00:05:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.simple_container_workflow.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(containerized_workflow)(5)
        assert job.wait(timeout=180), f"Workflow did not complete: {job.get_stderr()}"
        result = job.get_result()
        # simple_container_workflow: container_add(x, 10) then container_multiply(result, 2)
        # (5 + 10) * 2 = 30
        assert result == 30, f"Expected 30, got {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_workflow_with_inheriting_tasks(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test containerized workflow with tasks using inherit packaging.

    This tests that tasks with packaging='inherit' correctly receive
    and use the parent workflow's container configuration.
    """

    containerized_workflow = workflow(
        time="00:05:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.simple_inheriting_workflow.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(containerized_workflow)(5)
        assert job.wait(timeout=180), f"Workflow did not complete: {job.get_stderr()}"
        result = job.get_result()
        # simple_inheriting_workflow: inheriting_add(x, 10) = 5 + 10 = 15
        assert result == 15, f"Expected 15, got {result}"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_failing_workflow_with_containers(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test that failing containerized workflows report errors correctly."""

    containerized_workflow = workflow(
        time="00:05:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.failing_container_workflow.unwrapped)

    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(containerized_workflow)()
        job.wait(timeout=180)

        # Job should fail
        assert not job.is_successful(), "Expected workflow to fail but it succeeded"


@pytest.mark.container_packaging
@pytest.mark.slow_integration_test
def test_workflow_with_different_child_container(
    slurm_pyxis_cluster,
    local_registry,
    shared_comprehensive_dockerfile,
):
    """Test workflow using with_dependencies to pre-build a child task's container.

    This tests the with_dependencies() API which allows workflows to have
    child tasks with different containers than the parent workflow.

    The workflow uses container A, but calls a task that needs container B.
    Using with_dependencies(), we pre-build container B before submitting
    the workflow.
    """

    # Create the workflow with its container config
    containerized_workflow = workflow(
        time="00:05:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )(container_test_functions.workflow_with_dependency_task.unwrapped)

    # Create the task with its own container config (different tag)
    task_with_container = container_test_functions.task_with_own_container.with_options(
        packaging_dockerfile=str(shared_comprehensive_dockerfile),
        packaging_registry=f"{local_registry}/test/",
        packaging_tag="child-v1",
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )

    with slurm_pyxis_cluster:
        # Use with_dependencies to pre-build the child task's container
        job = slurm_pyxis_cluster.submit(
            containerized_workflow.with_dependencies([task_with_container])
        )(3)

        assert job.wait(timeout=240), f"Workflow did not complete: {job.get_stderr()}"
        result = job.get_result()
        # workflow_with_dependency_task: task_with_own_container(x) * 10, then container_add(result, 5)
        # (3 * 10) + 5 = 35
        assert result == 35, f"Expected 35, got {result}"
