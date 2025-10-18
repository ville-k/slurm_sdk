"""Integration tests for workflow callbacks and context functionality."""

from __future__ import annotations

import logging

import pytest

from slurm.callbacks import BenchmarkCallback, LoggerCallback
from slurm.examples.integration_test_workflow import (
    simple_workflow,
    inner_workflow,
    outer_workflow,
    failing_workflow,
    sequential_workflow,
)


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_workflow_with_callbacks(slurm_cluster):
    """Test that workflows execute correctly with callbacks tracking them."""
    logging.basicConfig(level=logging.INFO)

    # Create callbacks to track execution
    benchmark = BenchmarkCallback()
    logger = LoggerCallback()

    # Add callbacks to cluster
    slurm_cluster.callbacks.extend([benchmark, logger])

    # Submit workflow
    job = slurm_cluster.submit(simple_workflow)(5)
    result = job.wait(timeout=180, poll_interval=5)

    # If job failed, fetch and print logs for debugging
    if not job.is_successful():
        status = job.get_status()
        print(f"\n{'='*70}")
        print(f"JOB FAILED - Status: {status}")
        print(f"{'='*70}")

        # Try to fetch stdout
        if hasattr(job, 'stdout_path') and job.stdout_path:
            try:
                stdout_content = slurm_cluster.backend.execute_command(f"cat {job.stdout_path}")
                print(f"\n--- STDOUT ({job.stdout_path}) ---")
                print(stdout_content)
            except Exception as e:
                print(f"Could not read stdout: {e}")

        # Try to fetch stderr
        if hasattr(job, 'stderr_path') and job.stderr_path:
            try:
                stderr_content = slurm_cluster.backend.execute_command(f"cat {job.stderr_path}")
                print(f"\n--- STDERR ({job.stderr_path}) ---")
                print(stderr_content)
            except Exception as e:
                print(f"Could not read stderr: {e}")

        # Check if Slurmfile was uploaded
        try:
            job_dir = job.target_job_dir
            print(f"\n--- Job Directory Listing ({job_dir}) ---")
            ls_output = slurm_cluster.backend.execute_command(f"ls -la {job_dir}")
            print(ls_output)

            # Check Slurmfile content
            slurmfile_content = slurm_cluster.backend.execute_command(f"cat {job_dir}/Slurmfile.toml")
            print(f"\n--- Slurmfile.toml Content ---")
            print(slurmfile_content)

            # Check the job script for environment variables
            print(f"\n--- Checking for SLURM_SDK env vars in stderr ---")
            stderr_grep = slurm_cluster.backend.execute_command(
                f"grep -E 'SLURM_SDK_SLURMFILE|SLURM_SDK_ENV|export.*SLURM_SDK' {job.stderr_path} || echo 'Not found in stderr'"
            )
            print(stderr_grep)

            # Check child task outputs
            print(f"\n--- Checking Child Task Outputs ---")
            tasks_dir = f"{job_dir}/tasks"
            find_tasks = slurm_cluster.backend.execute_command(
                f"find {tasks_dir} -name '*.err' -o -name '*.out' 2>/dev/null || echo 'No task outputs found'"
            )
            print(f"Task output files: {find_tasks}")

            # Print first child task stderr if exists
            first_stderr = slurm_cluster.backend.execute_command(
                f"find {tasks_dir} -name '*.err' | head -1 | xargs cat 2>/dev/null || echo 'No stderr files found'"
            )
            if first_stderr and "No stderr files found" not in first_stderr:
                print(f"\n--- First Child Task STDERR ---")
                print(first_stderr)

        except Exception as e:
            print(f"Could not check files: {e}")

        print(f"{'='*70}\n")

    # Check result
    assert job.is_successful(), f"Job failed: {job.get_status()}"

    # Expected: simple_task(5) = 10, simple_task(6) = 12, add_task(10, 12) = 22
    assert result == 22

    # Check that benchmark callback tracked the workflow
    workflow_metrics = benchmark.get_all_workflow_metrics()
    assert len(workflow_metrics) > 0, "BenchmarkCallback should have tracked workflow"

    # Get metrics for the workflow
    workflow_id = job.id
    metrics = benchmark.get_workflow_metrics(workflow_id)

    if metrics:
        # Verify metrics were captured
        assert "name" in metrics
        assert metrics["name"] == "simple_workflow"
        assert "child_count" in metrics
        # Should have submitted 3 child tasks (simple_task x2, add_task x1)
        assert metrics["child_count"] == 3
        print(f"Workflow metrics: {metrics}")


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_nested_workflow_with_callbacks(slurm_cluster):
    """Test nested workflows with callback tracking."""
    logging.basicConfig(level=logging.INFO)

    # Create callbacks
    benchmark = BenchmarkCallback()
    logger = LoggerCallback()
    slurm_cluster.callbacks.extend([benchmark, logger])

    # Submit outer workflow
    job = slurm_cluster.submit(outer_workflow)(7)
    result = job.wait(timeout=300, poll_interval=5)

    # Check result: simple_task(7) = 14, add_task(14, 10) = 24
    assert job.is_successful()
    assert result == 24

    # Check that benchmark tracked both workflows
    all_metrics = benchmark.get_all_workflow_metrics()
    assert len(all_metrics) >= 1, "Should have tracked at least the outer workflow"

    # The outer workflow should show child tasks submitted
    outer_metrics = benchmark.get_workflow_metrics(job.id)
    if outer_metrics:
        assert outer_metrics["name"] == "outer_workflow"
        # Should have submitted inner_workflow and add_task
        assert outer_metrics["child_count"] >= 2
        print(f"Outer workflow metrics: {outer_metrics}")


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_workflow_exception_handling(slurm_cluster):
    """Test that workflow failures are properly tracked by callbacks."""
    logging.basicConfig(level=logging.INFO)

    # Create callbacks
    benchmark = BenchmarkCallback()
    logger = LoggerCallback()
    slurm_cluster.callbacks.extend([benchmark, logger])

    # Submit workflow (should fail)
    job = slurm_cluster.submit(failing_workflow)()

    with pytest.raises(Exception):
        job.wait(timeout=180, poll_interval=5)

    # Job should not be successful
    assert not job.is_successful()

    # Benchmark should still have tracked the workflow
    metrics = benchmark.get_workflow_metrics(job.id)
    if metrics:
        assert metrics["name"] == "failing_workflow"
        print(f"Failed workflow tracked: {metrics}")


@pytest.mark.integration_test
@pytest.mark.slow_integration_test
def test_benchmark_callback_metrics_accuracy(slurm_cluster):
    """Test that BenchmarkCallback accurately measures workflow performance."""
    logging.basicConfig(level=logging.INFO)

    # Create benchmark callback
    benchmark = BenchmarkCallback()
    slurm_cluster.callbacks.append(benchmark)

    # Submit workflow
    job = slurm_cluster.submit(sequential_workflow)(10)
    result = job.wait(timeout=300, poll_interval=5)

    # Check result: (11 + 12 + 13 + 14 + 15) = 65
    assert result == 65

    # Get detailed metrics
    metrics = benchmark.get_workflow_metrics(job.id)
    assert metrics is not None, "Metrics should be available"

    # Verify metrics structure
    assert "name" in metrics
    assert "duration_seconds" in metrics
    assert "child_count" in metrics
    assert metrics["child_count"] == 5

    # Check orchestration overhead metrics
    if "orchestration_overhead_ms" in metrics:
        print(f"Average submission interval: {metrics['orchestration_overhead_ms']:.2f}ms")
        assert metrics["orchestration_overhead_ms"] > 0

    # Check throughput
    if "submission_throughput" in metrics:
        print(f"Submission throughput: {metrics['submission_throughput']:.2f} tasks/sec")
        assert metrics["submission_throughput"] > 0

    print(f"\nWorkflow performance metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
