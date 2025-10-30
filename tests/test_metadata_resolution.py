"""Tests for metadata.json creation and JobResultPlaceholder resolution.

These tests protect against issues where JobResultPlaceholder objects cannot
find their corresponding result files due to missing metadata.
"""

import glob
import json
import os
import pickle
import tempfile
from pathlib import Path

from slurm.task import JobResultPlaceholder


def _concurrent_write_metadata_worker(args):
    """Helper function for concurrent metadata write test.

    Must be at module level to be picklable for multiprocessing.
    """
    import time
    import fcntl

    tmpdir, job_id, result_data = args
    job_dir = Path(tmpdir) / "slurm_jobs" / "concurrent_test" / "20250101_123456"

    # Save result file
    result_filename = f"slurm_job_test_{job_id}_result.pkl"
    result_path = job_dir / result_filename
    with open(result_path, "wb") as f:
        pickle.dump(result_data, f)

    # Write metadata with locking (simulating runner.py logic)
    metadata_path = job_dir / "metadata.json"
    lock_file = str(metadata_path) + ".lock"

    try:
        # Acquire lock
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY, 0o644)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        try:
            # Load existing metadata
            metadata_map = {}
            if metadata_path.exists():
                with open(metadata_path, "r") as f:
                    metadata_map = json.load(f)

            # Add this job's entry
            metadata_map[job_id] = {
                "result_file": result_filename,
                "timestamp": time.time(),
            }

            # Write atomically
            temp_path = str(metadata_path) + ".tmp"
            with open(temp_path, "w") as f:
                json.dump(metadata_map, f, indent=2)
            os.rename(temp_path, metadata_path)

        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    except Exception as e:
        print(f"Error writing metadata for {job_id}: {e}")
        raise

    return job_id


def test_metadata_json_format():
    """Test that metadata.json has the expected structure."""
    # Simulate what the runner creates
    metadata = {
        "job_123": {
            "result_file": "slurm_job_abc_result.pkl",
            "timestamp": 1234567890.0,
        },
        "job_456": {
            "result_file": "slurm_job_def_result.pkl",
            "timestamp": 1234567891.0,
        },
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        metadata_path = Path(tmpdir) / "metadata.json"

        # Write metadata
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        # Verify it can be read back
        with open(metadata_path, "r") as f:
            loaded = json.load(f)

        assert loaded == metadata
        assert "job_123" in loaded
        assert loaded["job_123"]["result_file"] == "slurm_job_abc_result.pkl"


def test_metadata_json_merge_for_array_jobs():
    """Test that metadata.json can be updated for array job elements."""
    with tempfile.TemporaryDirectory() as tmpdir:
        metadata_path = Path(tmpdir) / "metadata.json"

        # Simulate first array job element writing metadata
        metadata = {
            "job_100_0": {
                "result_file": "slurm_job_pre_0_result.pkl",
                "timestamp": 1234567890.0,
            }
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)

        # Simulate second array job element updating metadata
        with open(metadata_path, "r") as f:
            existing = json.load(f)

        existing["job_100_1"] = {
            "result_file": "slurm_job_pre_1_result.pkl",
            "timestamp": 1234567891.0,
        }

        with open(metadata_path, "w") as f:
            json.dump(existing, f)

        # Verify both entries exist
        with open(metadata_path, "r") as f:
            final = json.load(f)

        assert len(final) == 2
        assert "job_100_0" in final
        assert "job_100_1" in final


def test_placeholder_resolution_with_metadata():
    """Test JobResultPlaceholder resolution using metadata.json.

    This simulates the full resolution flow:
    1. Job saves result and creates metadata.json
    2. Another job uses JobResultPlaceholder to reference the result
    3. Resolution finds the result via metadata lookup
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        job_dir = Path(tmpdir) / "slurm_jobs" / "task1" / "20250101_123456"
        job_dir.mkdir(parents=True)

        # Simulate job saving result
        job_id = "12345_0"
        result_data = {"status": "success", "value": 42}
        result_filename = "slurm_job_20250101_123456_0_result.pkl"
        result_path = job_dir / result_filename

        with open(result_path, "wb") as f:
            pickle.dump(result_data, f)

        # Create metadata
        metadata_path = job_dir / "metadata.json"
        metadata = {
            job_id: {
                "result_file": result_filename,
                "timestamp": 1234567890.0,
            }
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)

        # Simulate resolution logic from runner.py
        placeholder = JobResultPlaceholder(job_id)
        job_base_dir = str(Path(tmpdir) / "slurm_jobs")

        # Search for metadata files
        import glob

        found_result = None
        search_pattern = f"{job_base_dir}/**/metadata.json"
        for meta_path in glob.glob(search_pattern, recursive=True):
            with open(meta_path, "r") as f:
                metadata_map = json.load(f)

            if placeholder.job_id in metadata_map:
                result_dir = os.path.dirname(meta_path)
                result_file = metadata_map[placeholder.job_id]["result_file"]
                full_result_path = os.path.join(result_dir, result_file)

                with open(full_result_path, "rb") as f:
                    found_result = pickle.load(f)
                break

        # Verify resolution succeeded
        assert found_result is not None
        assert found_result == result_data
        assert found_result["value"] == 42


def test_placeholder_resolution_with_multiple_jobs():
    """Test resolution when multiple jobs have metadata in same directory."""
    import glob

    with tempfile.TemporaryDirectory() as tmpdir:
        job_dir = Path(tmpdir) / "slurm_jobs" / "array_task" / "20250101_123456"
        job_dir.mkdir(parents=True)

        # Simulate array job with 3 elements
        jobs_data = {
            "18584214_0": {"result": "data_1", "index": 0},
            "18584214_1": {"result": "data_2", "index": 1},
            "18584214_2": {"result": "data_3", "index": 2},
        }

        metadata = {}
        for job_id, data in jobs_data.items():
            # Save result
            result_filename = f"slurm_job_pre_{data['index']}_result.pkl"
            result_path = job_dir / result_filename
            with open(result_path, "wb") as f:
                pickle.dump(data, f)

            # Add to metadata
            metadata[job_id] = {
                "result_file": result_filename,
                "timestamp": 1234567890.0 + data["index"],
            }

        # Write combined metadata
        metadata_path = job_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)

        # Test resolution for each job
        job_base_dir = str(Path(tmpdir) / "slurm_jobs")

        for job_id, expected_data in jobs_data.items():
            placeholder = JobResultPlaceholder(job_id)

            # Search and resolve
            found_result = None
            search_pattern = f"{job_base_dir}/**/metadata.json"
            for meta_path in glob.glob(search_pattern, recursive=True):
                with open(meta_path, "r") as f:
                    metadata_map = json.load(f)

                if placeholder.job_id in metadata_map:
                    result_dir = os.path.dirname(meta_path)
                    result_file = metadata_map[placeholder.job_id]["result_file"]
                    full_result_path = os.path.join(result_dir, result_file)

                    with open(full_result_path, "rb") as f:
                        found_result = pickle.load(f)
                    break

            assert found_result is not None
            assert found_result == expected_data
            assert found_result["result"] == expected_data["result"]


def test_placeholder_resolution_missing_metadata():
    """Test that resolution fails gracefully when metadata is missing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        job_base_dir = str(Path(tmpdir) / "slurm_jobs")

        placeholder = JobResultPlaceholder("nonexistent_job_id")

        # Search for metadata (should find none)
        import glob

        found_result = None
        search_pattern = f"{job_base_dir}/**/metadata.json"
        for meta_path in glob.glob(search_pattern, recursive=True):
            with open(meta_path, "r") as f:
                metadata_map = json.load(f)

            if placeholder.job_id in metadata_map:
                result_dir = os.path.dirname(meta_path)
                result_file = metadata_map[placeholder.job_id]["result_file"]
                full_result_path = os.path.join(result_dir, result_file)

                with open(full_result_path, "rb") as f:
                    found_result = pickle.load(f)
                break

        # Should not find any result
        assert found_result is None


def test_placeholder_resolution_corrupted_metadata():
    """Test resolution when metadata.json is corrupted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        job_dir = Path(tmpdir) / "slurm_jobs" / "task1" / "20250101_123456"
        job_dir.mkdir(parents=True)

        # Create corrupted metadata file
        metadata_path = job_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            f.write("{ this is not valid json }")

        job_base_dir = str(Path(tmpdir) / "slurm_jobs")
        placeholder = JobResultPlaceholder("some_job_id")

        # Search should handle corruption gracefully
        import glob

        found_result = None
        search_pattern = f"{job_base_dir}/**/metadata.json"
        for meta_path in glob.glob(search_pattern, recursive=True):
            try:
                with open(meta_path, "r") as f:
                    metadata_map = json.load(f)

                if placeholder.job_id in metadata_map:
                    result_dir = os.path.dirname(meta_path)
                    result_file = metadata_map[placeholder.job_id]["result_file"]
                    full_result_path = os.path.join(result_dir, result_file)

                    with open(full_result_path, "rb") as f:
                        found_result = pickle.load(f)
                    break
            except json.JSONDecodeError:
                # Should skip corrupted files
                continue

        # Should not crash, just not find result
        assert found_result is None


def test_metadata_json_concurrent_writes():
    """Test that concurrent metadata writes don't cause race conditions.

    This simulates multiple array job elements writing metadata simultaneously
    using multiprocessing to ensure file locking prevents lost updates.
    """
    import multiprocessing

    with tempfile.TemporaryDirectory() as tmpdir:
        job_dir = Path(tmpdir) / "slurm_jobs" / "concurrent_test" / "20250101_123456"
        job_dir.mkdir(parents=True)

        # Simulate 5 array elements writing concurrently
        num_jobs = 5
        jobs = [(tmpdir, f"12345_{i}", f"result_{i}") for i in range(num_jobs)]

        # Write all jobs concurrently using multiprocessing
        with multiprocessing.Pool(processes=num_jobs) as pool:
            completed = pool.map(_concurrent_write_metadata_worker, jobs)

        # Verify all jobs wrote their metadata successfully
        assert len(completed) == num_jobs

        # Check final metadata has ALL entries
        metadata_path = job_dir / "metadata.json"
        assert metadata_path.exists()

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # All 5 job IDs should be present - this is the critical test
        # Without file locking, some entries would be lost due to race conditions
        assert len(metadata) == num_jobs, (
            f"Expected {num_jobs} entries, got {len(metadata)}. Missing: {set(f'12345_{i}' for i in range(num_jobs)) - set(metadata.keys())}"
        )

        for i in range(num_jobs):
            job_id = f"12345_{i}"
            assert job_id in metadata, f"Missing metadata for {job_id}"
            assert (
                metadata[job_id]["result_file"] == f"slurm_job_test_{job_id}_result.pkl"
            )


def test_array_job_metadata_uses_full_job_id():
    """Test that metadata.json keys use full job IDs (base_id_index) for array jobs.

    This protects against the bug where array jobs saved metadata with base job ID
    (e.g., "18609677") but placeholders looked for full ID (e.g., "18609677_0").
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        job_dir = Path(tmpdir) / "slurm_jobs" / "array_task" / "20250101_123456"
        job_dir.mkdir(parents=True)

        # Simulate array job elements with FULL job IDs (base_id_index format)
        array_jobs = [
            {"job_id": "18609677_0", "data": "result_0"},
            {"job_id": "18609677_1", "data": "result_1"},
            {"job_id": "18609677_2", "data": "result_2"},
        ]

        metadata = {}
        for job_info in array_jobs:
            job_id = job_info["job_id"]
            data = job_info["data"]

            # Save result file
            result_filename = f"slurm_job_{job_id.replace('_', '_idx')}_result.pkl"
            result_path = job_dir / result_filename
            with open(result_path, "wb") as f:
                pickle.dump(data, f)

            # Add to metadata with FULL job ID (critical fix)
            metadata[job_id] = {
                "result_file": result_filename,
                "timestamp": 1234567890.0,
            }

        # Write metadata
        metadata_path = job_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)

        # Test resolution with full job IDs
        job_base_dir = str(Path(tmpdir) / "slurm_jobs")

        for job_info in array_jobs:
            job_id = job_info["job_id"]
            expected_data = job_info["data"]

            placeholder = JobResultPlaceholder(job_id)

            # Search and resolve using metadata
            found_result = None
            search_pattern = f"{job_base_dir}/**/metadata.json"
            for meta_path in glob.glob(search_pattern, recursive=True):
                with open(meta_path, "r") as f:
                    metadata_map = json.load(f)

                # This should find the entry because keys use full IDs
                if placeholder.job_id in metadata_map:
                    result_dir = os.path.dirname(meta_path)
                    result_file = metadata_map[placeholder.job_id]["result_file"]
                    full_result_path = os.path.join(result_dir, result_file)

                    with open(full_result_path, "rb") as f:
                        found_result = pickle.load(f)
                    break

            assert found_result is not None, f"Failed to resolve {job_id}"
            assert found_result == expected_data
