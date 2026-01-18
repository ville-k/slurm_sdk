"""Tests for the dashboard data provider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from slurm.tui.dashboard.data import (
    DashboardDataProvider,
    JobInfo,
    PartitionInfo,
)


class TestJobInfo:
    """Tests for JobInfo dataclass."""

    def test_from_queue_entry_with_uppercase_keys(self):
        """JobInfo extracts data from uppercase SLURM keys."""
        entry = {
            "JOBID": "12345",
            "NAME": "test_job",
            "STATE": "RUNNING",
            "USER": "testuser",
            "ACCOUNT": "testaccount",
            "PARTITION": "gpu",
            "NODES": "2",
            "TIME": "1:30:00",
        }

        job = JobInfo.from_queue_entry(entry)

        assert job.job_id == "12345"
        assert job.name == "test_job"
        assert job.state == "RUNNING"
        assert job.user == "testuser"
        assert job.account == "testaccount"
        assert job.partition == "gpu"
        assert job.nodes == "2"
        assert job.time == "1:30:00"

    def test_from_queue_entry_with_mixed_keys(self):
        """JobInfo handles mixed-case keys from different SLURM outputs."""
        entry = {
            "JobID": "67890",
            "JobName": "another_job",
            "JobState": "PENDING",
            "UserId": "otheruser",
            "Account": "otheraccount",
            "Partition": "cpu",
            "NumNodes": "4",
            "RunTime": "0:00:00",
        }

        job = JobInfo.from_queue_entry(entry)

        assert job.job_id == "67890"
        assert job.name == "another_job"
        assert job.state == "PENDING"
        assert job.user == "otheruser"
        assert job.account == "otheraccount"
        assert job.partition == "cpu"
        assert job.nodes == "4"
        assert job.time == "0:00:00"

    def test_from_queue_entry_handles_missing_fields(self):
        """JobInfo handles missing fields gracefully."""
        entry = {"JOBID": "123"}

        job = JobInfo.from_queue_entry(entry)

        assert job.job_id == "123"
        assert job.name == ""
        assert job.state == ""
        assert job.user == ""

    def test_from_status(self):
        """JobInfo.from_status creates info from detailed status dict."""
        status = {
            "JobState": "RUNNING",
            "JobName": "training",
            "UserId": "vkallioniemi",
            "Account": "ml-research",
            "Partition": "gpu",
            "NumNodes": "2",
            "NumCPUs": "64",
            "SubmitTime": "2025-01-17T10:00:00",
            "StartTime": "2025-01-17T10:01:00",
            "RunTime": "2:00:00",
            "TimeLimit": "24:00:00",
            "WorkDir": "/home/user/project",
        }

        job = JobInfo.from_status("12345", status)

        assert job.job_id == "12345"
        assert job.name == "training"
        assert job.state == "RUNNING"
        assert job.partition == "gpu"
        assert job.num_cpus == "64"
        assert job.work_dir == "/home/user/project"


class TestPartitionInfo:
    """Tests for PartitionInfo dataclass."""

    def test_state_summary(self):
        """PartitionInfo generates correct state summary."""
        partition = PartitionInfo(
            name="gpu",
            avail="up",
            total_nodes=10,
            node_states={"idle": 5, "allocated": 3, "down": 2},
            time_limit="24:00:00",
            cpus="64",
            memory="256G",
        )

        # State summary should be sorted alphabetically
        assert "allocated" in partition.state_summary
        assert "idle" in partition.state_summary
        assert "down" in partition.state_summary

    def test_state_summary_empty(self):
        """PartitionInfo returns 'n/a' for empty states."""
        partition = PartitionInfo(
            name="test",
            avail="up",
            total_nodes=0,
            node_states={},
            time_limit="",
            cpus="",
            memory="",
        )

        assert partition.state_summary == "n/a"

    def test_is_up(self):
        """PartitionInfo.is_up returns correct availability."""
        up_partition = PartitionInfo(
            name="test",
            avail="up",
            total_nodes=1,
            node_states={},
            time_limit="",
            cpus="",
            memory="",
        )
        down_partition = PartitionInfo(
            name="test",
            avail="down",
            total_nodes=1,
            node_states={},
            time_limit="",
            cpus="",
            memory="",
        )

        assert up_partition.is_up is True
        assert down_partition.is_up is False


class TestDashboardDataProvider:
    """Tests for DashboardDataProvider."""

    @pytest.fixture
    def mock_cluster(self):
        """Create a mock cluster for testing."""
        cluster = MagicMock()
        cluster.get_queue.return_value = [
            {
                "JOBID": "100",
                "NAME": "job1",
                "STATE": "RUNNING",
                "USER": "testuser",
                "ACCOUNT": "testaccount",
                "PARTITION": "gpu",
                "NODES": "1",
            },
            {
                "JOBID": "101",
                "NAME": "job2",
                "STATE": "PENDING",
                "USER": "testuser",
                "ACCOUNT": "testaccount",
                "PARTITION": "cpu",
                "NODES": "2",
            },
            {
                "JOBID": "102",
                "NAME": "job3",
                "STATE": "RUNNING",
                "USER": "otheruser",
                "ACCOUNT": "testaccount",
                "PARTITION": "gpu",
                "NODES": "4",
            },
        ]
        cluster.get_cluster_info.return_value = {
            "partitions": [
                {"PARTITION": "gpu", "AVAIL": "up", "NODES": "5", "STATE": "idle"},
                {"PARTITION": "gpu", "AVAIL": "up", "NODES": "3", "STATE": "allocated"},
                {"PARTITION": "cpu", "AVAIL": "up", "NODES": "20", "STATE": "idle"},
            ]
        }
        return cluster

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_refresh_filters_my_jobs(self, mock_getuser, mock_cluster):
        """Refresh correctly filters current user's jobs."""
        mock_getuser.return_value = "testuser"

        provider = DashboardDataProvider(mock_cluster)
        data = provider.refresh()

        # Should have 2 jobs for testuser
        assert len(data.my_jobs) == 2
        assert all(job.user == "testuser" for job in data.my_jobs)

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_refresh_groups_account_jobs(self, mock_getuser, mock_cluster):
        """Refresh groups account jobs by user (excluding current user)."""
        mock_getuser.return_value = "testuser"

        provider = DashboardDataProvider(mock_cluster)
        data = provider.refresh()

        # Should have 1 other user in account jobs
        assert "otheruser" in data.account_jobs
        assert len(data.account_jobs["otheruser"]) == 1
        # testuser should NOT be in account_jobs
        assert "testuser" not in data.account_jobs

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_refresh_aggregates_partitions(self, mock_getuser, mock_cluster):
        """Refresh aggregates partition info correctly."""
        mock_getuser.return_value = "testuser"

        provider = DashboardDataProvider(mock_cluster)
        data = provider.refresh()

        # Should have 2 partitions
        assert len(data.partitions) == 2

        # Find gpu partition
        gpu = next(p for p in data.partitions if p.name == "gpu")
        assert gpu.total_nodes == 8  # 5 idle + 3 allocated
        assert gpu.node_states["idle"] == 5
        assert gpu.node_states["allocated"] == 3

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_refresh_handles_error(self, mock_getuser, mock_cluster):
        """Refresh handles errors gracefully."""
        mock_getuser.return_value = "testuser"
        mock_cluster.get_queue.side_effect = Exception("Connection failed")

        provider = DashboardDataProvider(mock_cluster)
        data = provider.refresh()

        assert data.error is not None
        assert "Connection failed" in data.error

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_refresh_sets_last_refresh(self, mock_getuser, mock_cluster):
        """Refresh updates the last_refresh timestamp."""
        mock_getuser.return_value = "testuser"

        provider = DashboardDataProvider(mock_cluster)
        data = provider.refresh()

        assert data.last_refresh is not None

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_cancel_job(self, mock_getuser, mock_cluster):
        """cancel_job calls cluster.get_job and job.cancel."""
        mock_getuser.return_value = "testuser"
        mock_job = MagicMock()
        mock_cluster.get_job.return_value = mock_job

        provider = DashboardDataProvider(mock_cluster)
        result = provider.cancel_job("12345")

        assert result is True
        mock_cluster.get_job.assert_called_once_with("12345")
        mock_job.cancel.assert_called_once()

    @patch("slurm.tui.dashboard.data.getpass.getuser")
    @patch.dict("os.environ", {"USER": "testuser"})
    def test_cancel_job_handles_error(self, mock_getuser, mock_cluster):
        """cancel_job returns False on error."""
        mock_getuser.return_value = "testuser"
        mock_cluster.get_job.side_effect = Exception("Job not found")

        provider = DashboardDataProvider(mock_cluster)
        result = provider.cancel_job("99999")

        assert result is False
