"""Integration tests for native SLURM array job support.

These tests verify that native array submissions using the --array flag work
correctly end-to-end with a real SLURM cluster.
"""

import pytest

from slurm.examples.integration_test_task import (
    process_string_item,
    add_two_numbers,
    multiply_with_default,
    prepare_data,
    process_item_simple,
    slow_multiply_task,
)


# Skip if not in integration test environment
pytestmark = pytest.mark.integration


def test_native_array_submission_with_simple_items(slurm_cluster):
    """Test native array submission with simple string items."""

    # Use the provided cluster instance
    with slurm_cluster:
        items = ["apple", "banana", "cherry", "date", "elderberry"]
        array_job = process_string_item.map(items)

        # Verify job was submitted
        assert len(array_job) == len(items)
        assert array_job._array_job_id is not None

        # Verify array job ID format: "JOBID_[START-END]"
        assert "_[" in array_job._array_job_id

        # Wait for completion (with timeout)
        success = array_job.wait(timeout=120)

        # If failed, print job statuses for debugging
        if not success:
            print("\n=== Job Statuses ===")
            for i, job in enumerate(array_job):
                status = job.get_status()
                print(
                    f"Job {i} ({job.id}): {status.get('JobState', 'UNKNOWN')}, ExitCode: {status.get('ExitCode', 'N/A')}"
                )
                print(f"  Job dir: {job.target_job_dir}")
                print(f"  Stdout: {job.stdout_path}")
                print(f"  Stderr: {job.stderr_path}")

                # Try to get stdout and stderr if job failed
                if status.get("JobState") in [
                    "FAILED",
                    "TIMEOUT",
                    "CANCELLED",
                    "COMPLETED",
                ]:
                    try:
                        # Get stderr (most important for debugging)
                        if job.stderr_path:
                            stdout, stderr, rc = slurm_cluster.backend._run_command(
                                f"cat '{job.stderr_path}'", timeout=5
                            )
                            print(f"  STDERR (rc={rc}):\n{stdout[:2000]}")
                            if stderr:
                                print(f"  (command stderr: {stderr[:500]})")
                    except Exception as e:
                        import traceback

                        print(f"  Could not get stderr: {e}")
                        print(traceback.format_exc()[:500])

                    try:
                        # Get stdout
                        if job.stdout_path:
                            stdout, stderr, rc = slurm_cluster.backend._run_command(
                                f"cat '{job.stdout_path}'", timeout=5
                            )
                            print(f"  STDOUT (rc={rc}):\n{stdout[:2000]}")
                    except Exception as e:
                        print(f"  Could not get stdout: {e}")

            # Also check what files exist in the job directory
            try:
                job_dir = array_job.array_dir
                print("\n=== Diagnostic Info ===")
                print(f"Array dir: {job_dir}")

                # Test if directory exists
                stdout, stderr, rc = slurm_cluster.backend._run_command(
                    f"test -d {job_dir} && echo 'EXISTS' || echo 'NOT_FOUND'", timeout=5
                )
                print(f"Directory exists: {stdout.strip()}")

                # List files
                stdout, stderr, rc = slurm_cluster.backend._run_command(
                    f"ls -la {job_dir}", timeout=5
                )
                print(f"Files in dir:\n{stdout[:1000]}")
                if stderr:
                    print(f"Stderr: {stderr[:500]}")

                # Find and show job script
                stdout, stderr, rc = slurm_cluster.backend._run_command(
                    f"find {job_dir} -name '*.sh' -exec head -100 {{}} \\;", timeout=5
                )
                print(f"\n=== Job Script ===\n{stdout[:2000]}")

            except Exception as e:
                import traceback

                print(f"Error during diagnostics: {e}")
                print(traceback.format_exc()[:1000])

        assert success, "Array job did not complete within timeout"

        # Get results
        results = array_job.get_results()

        # Verify results
        assert len(results) == len(items)
        expected = [item.upper() for item in items]
        assert results == expected


def test_native_array_with_tuple_items(slurm_cluster):
    """Test native array submission with tuple items."""

    with slurm_cluster:
        items = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)]
        array_job = add_two_numbers.map(items)

        assert len(array_job) == len(items)

        success = array_job.wait(timeout=120)
        assert success, "Array job did not complete within timeout"

        results = array_job.get_results()

        assert len(results) == len(items)
        expected = [x + y for x, y in items]
        assert results == expected


def test_native_array_with_dict_items(slurm_cluster):
    """Test native array submission with dictionary items."""

    with slurm_cluster:
        items = [
            {"x": 1, "y": 3},
            {"x": 2, "y": 4},
            {"x": 3, "y": 5},
        ]
        array_job = multiply_with_default.map(items)

        assert len(array_job) == len(items)

        success = array_job.wait(timeout=120)
        assert success, "Array job did not complete within timeout"

        results = array_job.get_results()

        assert len(results) == len(items)
        expected = [item["x"] * item["y"] for item in items]
        assert results == expected


def test_native_array_with_dependencies(slurm_cluster):
    """Test native array submission with job dependencies."""

    with slurm_cluster:
        # Submit preparation job
        prep_job = prepare_data()

        # Submit array job with dependency on prep_job
        items = [1, 2, 3, 4, 5]
        array_job = process_item_simple.after(prep_job).map(items)

        assert len(array_job) == len(items)

        # Wait for prep job
        assert prep_job.wait(timeout=60)

        # Wait for array job
        success = array_job.wait(timeout=120)
        assert success, "Array job did not complete within timeout"

        # Verify results
        results = array_job.get_results()
        expected = [f"processed_{i}" for i in items]
        assert results == expected


def test_native_array_with_max_concurrent(slurm_cluster):
    """Test native array with max_concurrent throttling."""

    with slurm_cluster:
        items = list(range(10))
        # Limit to 2 concurrent tasks
        array_job = slow_multiply_task.map(items, max_concurrent=2)

        assert len(array_job) == len(items)

        # Verify array spec includes throttling (%2)
        assert array_job._array_job_id is not None
        if hasattr(array_job, "max_concurrent"):
            assert array_job.max_concurrent == 2

        success = array_job.wait(timeout=180)
        assert success, "Array job did not complete within timeout"

        results = array_job.get_results()
        expected = [x * 2 for x in items]
        assert results == expected
