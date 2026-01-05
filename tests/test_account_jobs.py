"""Tests for get_account_jobs SDK method."""

import pytest
from unittest.mock import patch
from slurm.api.ssh import SSHCommandBackend
from slurm.errors import BackendCommandError


class TestGetAccountJobs:
    @patch.object(
        SSHCommandBackend, "_create_remote_temp_dir", return_value="/tmp/slurm_ssh_test"
    )
    @patch.object(
        SSHCommandBackend,
        "_resolve_remote_path",
        return_value="/home/testuser/slurm_jobs",
    )
    @patch.object(SSHCommandBackend, "_connect", return_value=None)
    @patch.object(SSHCommandBackend, "_run_command")
    def test_get_account_jobs_parses_output(
        self, mock_run_cmd, mock_connect, mock_resolve_path, mock_create_temp_dir
    ):
        """Test that get_account_jobs correctly parses sacct output."""
        backend = SSHCommandBackend(
            hostname="test.cluster.com",
            username="testuser",
        )

        # Mock the SSH command execution (using new AllocTRES format)
        mock_output = (
            "12345|job1|user1|account1|COMPLETED|0:0|billing=8,cpu=8,gres/gpu=4,mem=32G,node=2|2|2024-01-01T00:00:00|2024-01-01T01:00:00|01:00:00|partition1\n"
            "12346|job2|user2|account1|FAILED|1:0|gres/gpu=2|1|2024-01-01T02:00:00|2024-01-01T02:30:00|00:30:00|partition1\n"
        )
        mock_run_cmd.return_value = (mock_output, "", 0)

        jobs = backend.get_account_jobs(
            account="account1",
            start_time="2024-01-01",
            end_time="now",
        )

        assert len(jobs) == 2
        assert jobs[0]["JobID"] == "12345"
        assert jobs[0]["JobName"] == "job1"
        assert jobs[0]["User"] == "user1"
        assert jobs[0]["Account"] == "account1"
        assert jobs[0]["State"] == "COMPLETED"
        assert jobs[0]["AllocTRES"] == "billing=8,cpu=8,gres/gpu=4,mem=32G,node=2"
        assert (
            jobs[0]["AllocGRES"] == "billing=8,cpu=8,gres/gpu=4,mem=32G,node=2"
        )  # Backwards compat
        assert jobs[0]["AllocNodes"] == "2"

        assert jobs[1]["JobID"] == "12346"
        assert jobs[1]["State"] == "FAILED"

    @patch.object(
        SSHCommandBackend, "_create_remote_temp_dir", return_value="/tmp/slurm_ssh_test"
    )
    @patch.object(
        SSHCommandBackend,
        "_resolve_remote_path",
        return_value="/home/testuser/slurm_jobs",
    )
    @patch.object(SSHCommandBackend, "_connect", return_value=None)
    @patch.object(SSHCommandBackend, "_run_command")
    def test_get_account_jobs_skips_job_steps(
        self, mock_run_cmd, mock_connect, mock_resolve_path, mock_create_temp_dir
    ):
        """Test that job steps (e.g., 12345.batch) are filtered out."""
        backend = SSHCommandBackend(
            hostname="test.cluster.com",
            username="testuser",
        )

        mock_output = (
            "12345|job1|user1|account1|COMPLETED|0:0|gres/gpu=4|2|2024-01-01T00:00:00|2024-01-01T01:00:00|01:00:00|partition1\n"
            "12345.batch|batch|user1|account1|COMPLETED|0:0||2|2024-01-01T00:00:00|2024-01-01T01:00:00|01:00:00|partition1\n"
            "12345.0|step0|user1|account1|COMPLETED|0:0||2|2024-01-01T00:00:00|2024-01-01T01:00:00|01:00:00|partition1\n"
        )
        mock_run_cmd.return_value = (mock_output, "", 0)

        jobs = backend.get_account_jobs(
            account="account1",
            start_time="2024-01-01",
            end_time="now",
        )

        # Should only include the main job, not the steps
        assert len(jobs) == 1
        assert jobs[0]["JobID"] == "12345"

    @patch.object(
        SSHCommandBackend, "_create_remote_temp_dir", return_value="/tmp/slurm_ssh_test"
    )
    @patch.object(
        SSHCommandBackend,
        "_resolve_remote_path",
        return_value="/home/testuser/slurm_jobs",
    )
    @patch.object(SSHCommandBackend, "_connect", return_value=None)
    @patch.object(SSHCommandBackend, "_run_command")
    def test_get_account_jobs_empty_result(
        self, mock_run_cmd, mock_connect, mock_resolve_path, mock_create_temp_dir
    ):
        """Test handling of no jobs found."""
        backend = SSHCommandBackend(
            hostname="test.cluster.com",
            username="testuser",
        )
        mock_run_cmd.return_value = ("", "", 0)

        jobs = backend.get_account_jobs(
            account="account1",
            start_time="2024-01-01",
            end_time="now",
        )

        assert len(jobs) == 0

    @patch.object(
        SSHCommandBackend, "_create_remote_temp_dir", return_value="/tmp/slurm_ssh_test"
    )
    @patch.object(
        SSHCommandBackend,
        "_resolve_remote_path",
        return_value="/home/testuser/slurm_jobs",
    )
    @patch.object(SSHCommandBackend, "_connect", return_value=None)
    @patch.object(SSHCommandBackend, "_run_command")
    def test_get_account_jobs_failure(
        self, mock_run_cmd, mock_connect, mock_resolve_path, mock_create_temp_dir
    ):
        """Test handling of sacct command failure."""
        backend = SSHCommandBackend(
            hostname="test.cluster.com",
            username="testuser",
        )
        mock_run_cmd.return_value = ("", "sacct: error: Invalid account", 1)

        with pytest.raises(BackendCommandError, match="Failed to get jobs for account"):
            backend.get_account_jobs(
                account="invalid_account",
                start_time="2024-01-01",
                end_time="now",
            )

    @patch.object(
        SSHCommandBackend, "_create_remote_temp_dir", return_value="/tmp/slurm_ssh_test"
    )
    @patch.object(
        SSHCommandBackend,
        "_resolve_remote_path",
        return_value="/home/testuser/slurm_jobs",
    )
    @patch.object(SSHCommandBackend, "_connect", return_value=None)
    @patch.object(SSHCommandBackend, "_run_command")
    def test_get_account_jobs_handles_empty_tres(
        self, mock_run_cmd, mock_connect, mock_resolve_path, mock_create_temp_dir
    ):
        """Test that empty AllocTRES is handled correctly."""
        backend = SSHCommandBackend(
            hostname="test.cluster.com",
            username="testuser",
        )
        mock_output = "12345|job1|user1|account1|COMPLETED|0:0||1|2024-01-01T00:00:00|2024-01-01T01:00:00|01:00:00|partition1\n"
        mock_run_cmd.return_value = (mock_output, "", 0)

        jobs = backend.get_account_jobs(
            account="account1",
            start_time="2024-01-01",
            end_time="now",
        )

        assert len(jobs) == 1
        assert jobs[0]["AllocTRES"] == ""
        assert jobs[0]["AllocGRES"] == ""
