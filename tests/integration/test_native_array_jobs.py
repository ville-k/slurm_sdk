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


@pytest.mark.slow_integration_test
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


@pytest.mark.slow_integration_test
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


@pytest.mark.slow_integration_test
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


@pytest.mark.slow_integration_test
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


@pytest.mark.slow_integration_test
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


@pytest.mark.slow_integration_test
def test_native_array_with_job_items(slurm_cluster):
    """Test native array where items are Job objects from previous tasks.

    This tests the Job -> JobResultPlaceholder conversion in array items.
    """

    with slurm_cluster:
        # Create preparation jobs that return strings
        prep_jobs = [prepare_data() for _ in range(3)]

        # Wait for all prep jobs to complete
        for job in prep_jobs:
            assert job.wait(timeout=60), f"Prep job {job.id} failed"

        # Map over the Job objects themselves
        # prepare_data() returns "prepared", process_string_item converts to uppercase
        # Jobs should be converted to placeholders and resolved at runtime
        array_job = process_string_item.map(prep_jobs)

        assert len(array_job) == 3
        assert array_job._array_job_id is not None

        # Verify all prep jobs are dependencies
        for prep_job in prep_jobs:
            assert prep_job in array_job.dependencies

        success = array_job.wait(timeout=120)
        assert success, "Array job did not complete within timeout"

        # Get results - should be uppercase versions of prep results
        results = array_job.get_results()
        assert len(results) == 3

        # Each prep job returns "prepared", so after uppercase: "PREPARED"
        for result in results:
            assert result == "PREPARED"


@pytest.mark.slow_integration_test
def test_native_array_pipeline_pattern(slurm_cluster):
    """Test pipeline pattern: stage1 → stage2 (using .after()).

    This tests a simplified pipeline pattern where stage2 depends on stage1
    using the .after() API rather than passing Jobs as items.
    """

    with slurm_cluster:
        # Stage 1: Process initial data
        stage1_items = ["input_a", "input_b"]  # Reduced to 2 items for faster test
        stage1_jobs = process_string_item.map(stage1_items)

        assert len(stage1_jobs) == 2
        success = stage1_jobs.wait(timeout=180)
        assert success, "Stage 1 did not complete within timeout"

        stage1_results = stage1_jobs.get_results()
        assert stage1_results == ["INPUT_A", "INPUT_B"]

        # Stage 2: Process new data with dependency on stage 1
        # Use .after() to establish dependency (simpler than Jobs as items)
        stage2_items = ["output_x", "output_y"]
        stage2_jobs = process_string_item.after(stage1_jobs).map(stage2_items)

        success = stage2_jobs.wait(timeout=180)
        assert success, "Stage 2 did not complete within timeout"

        stage2_results = stage2_jobs.get_results()
        assert len(stage2_results) == 2
        assert stage2_results == ["OUTPUT_X", "OUTPUT_Y"]


@pytest.mark.slow_integration_test
def test_metadata_json_created(slurm_cluster):
    """Test that metadata.json is created when jobs save results.

    This verifies the fix for missing metadata.json files.
    """

    with slurm_cluster:
        # Submit a simple array job
        items = ["test1", "test2", "test3"]
        array_job = process_string_item.map(items)

        assert array_job.wait(timeout=120)

        # Check that metadata.json exists in the job directory
        job_dir = array_job.array_dir
        assert job_dir is not None

        # Use the backend to check if file exists
        try:
            import json

            stdout, stderr, rc = slurm_cluster.backend._run_command(
                f"cat '{job_dir}/metadata.json'", timeout=5
            )
            assert rc == 0, f"metadata.json not found: {stderr}"

            # Parse and verify metadata structure
            metadata = json.loads(stdout)
            assert isinstance(metadata, dict)

            # Should have entries for all 3 array jobs
            # Job IDs will be like "12345_0", "12345_1", "12345_2"
            # We can't predict the exact IDs, but should have 3 entries
            assert len(metadata) >= 3

            # Each entry should have result_file and timestamp
            for _, meta in metadata.items():
                assert "result_file" in meta
                assert "timestamp" in meta
                assert meta["result_file"].endswith("_result.pkl")

        except Exception as e:
            pytest.fail(f"Failed to verify metadata.json: {e}")


@pytest.mark.slow_integration_test
def test_array_job_as_dependency_integration(slurm_cluster):
    """Test using ArrayJob in .after() dependency (integration test).

    This tests the ArrayJob expansion feature that prevents pickle errors.
    """

    with slurm_cluster:
        # Create an array job
        prep_items = ["chunk_1", "chunk_2", "chunk_3"]
        prep_array = process_string_item.map(prep_items)

        assert prep_array.wait(timeout=120)

        # Use the entire ArrayJob as a dependency for another task
        # This should expand to depend on all constituent jobs
        final_job = multiply_with_default.after(prep_array)(x=10, y=20)

        # Verify the job was created successfully (no pickle error)
        assert final_job is not None
        assert final_job.id is not None

        # Wait for completion
        assert final_job.wait(timeout=120)

        # Verify result
        result = final_job.get_result()
        assert result == 200  # 10 * 20
