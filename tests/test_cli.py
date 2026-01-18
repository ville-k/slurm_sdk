"""Tests for the slurm CLI."""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from slurm.cli.app import app, main
from slurm.cli.formatters import (
    print_cluster_info,
    print_environments_table,
    print_job_details,
    print_jobs_table,
)
from slurm.cli.utils import list_slurmfile_environments


def _write_sample_slurmfile(tmp_path: Path) -> Path:
    """Create a sample Slurmfile for testing."""
    content = textwrap.dedent(
        """
        [default]
        hostname = "default.example.com"

        [production]
        hostname = "prod.example.com"

        [local]
        backend = "local"
        """
    )
    slurmfile = tmp_path / "Slurmfile.toml"
    slurmfile.write_text(content, encoding="utf-8")
    return slurmfile


class TestCLIHelp:
    """Test CLI help messages and basic command structure."""

    def test_main_help(self, capsys):
        """Test main help message shows subcommands."""
        with pytest.raises(SystemExit) as exc_info:
            app(["--help"])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "jobs" in captured.out
        assert "cluster" in captured.out

    def test_version(self, capsys):
        """Test --version flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["--version"])
        assert exc_info.value.code == 0

    def test_jobs_help(self, capsys):
        """Test jobs subcommand help."""
        with pytest.raises(SystemExit) as exc_info:
            app(["jobs", "--help"])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "list" in captured.out
        assert "show" in captured.out

    def test_cluster_help(self, capsys):
        """Test cluster subcommand help."""
        with pytest.raises(SystemExit) as exc_info:
            app(["cluster", "--help"])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "list" in captured.out
        assert "show" in captured.out

    def test_jobs_list_help(self, capsys):
        """Test jobs list help shows parameters."""
        with pytest.raises(SystemExit) as exc_info:
            app(["jobs", "list", "--help"])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "--env" in captured.out
        assert "--slurmfile" in captured.out

    def test_cluster_list_help(self, capsys):
        """Test cluster list help shows parameters."""
        with pytest.raises(SystemExit) as exc_info:
            app(["cluster", "list", "--help"])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "--slurmfile" in captured.out


class TestListSlurmfileEnvironments:
    """Test list_slurmfile_environments utility function."""

    def test_lists_environments_from_slurmfile(self, tmp_path):
        """Test extracting environments from Slurmfile."""
        slurmfile = _write_sample_slurmfile(tmp_path)
        envs = list_slurmfile_environments(slurmfile=str(slurmfile))

        names = [e["name"] for e in envs]
        assert "default" in names
        assert "production" in names
        assert "local" in names

    def test_includes_hostname_info(self, tmp_path):
        """Test that hostname info is included."""
        slurmfile = _write_sample_slurmfile(tmp_path)
        envs = list_slurmfile_environments(slurmfile=str(slurmfile))

        prod_env = next(e for e in envs if e["name"] == "production")
        assert prod_env["hostname"] == "prod.example.com"
        assert prod_env["has_hostname"] is True

    def test_includes_slurmfile_path(self, tmp_path):
        """Test that slurmfile path is included."""
        slurmfile = _write_sample_slurmfile(tmp_path)
        envs = list_slurmfile_environments(slurmfile=str(slurmfile))

        assert all(str(slurmfile) in e["slurmfile"] for e in envs)


class TestClusterListCommand:
    """Test 'slurm cluster list' command."""

    def test_cluster_list_with_slurmfile(self, tmp_path, capsys):
        """Test cluster list outputs environments."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        with pytest.raises(SystemExit) as exc_info:
            app(["cluster", "list", "--slurmfile", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "default" in captured.out
        assert "production" in captured.out

    def test_cluster_list_short_flag(self, tmp_path, capsys):
        """Test cluster list with -f short flag."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        with pytest.raises(SystemExit) as exc_info:
            app(["cluster", "list", "-f", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "Environments" in captured.out


class TestFormatters:
    """Test Rich output formatters."""

    def test_print_jobs_table_empty(self, capsys):
        """Test jobs table with no jobs."""
        print_jobs_table([])
        captured = capsys.readouterr()
        assert "No jobs in queue" in captured.out

    def test_print_jobs_table_with_jobs(self, capsys):
        """Test jobs table with sample jobs."""
        jobs = [
            {
                "JOBID": "12345",
                "NAME": "test_job",
                "STATE": "RUNNING",
                "USER": "testuser",
                "TIME": "00:05:00",
                "PARTITION": "gpu",
                "NODES": "1",
            },
            {
                "JOBID": "12346",
                "NAME": "pending_job",
                "STATE": "PENDING",
                "USER": "testuser",
                "TIME": "00:00:00",
                "PARTITION": "cpu",
                "NODES": "2",
            },
        ]
        print_jobs_table(jobs)
        captured = capsys.readouterr()
        assert "12345" in captured.out
        assert "test_job" in captured.out
        assert "RUNNING" in captured.out
        assert "PENDING" in captured.out

    def test_print_job_details(self, capsys):
        """Test job details panel."""
        status = {
            "JobState": "COMPLETED",
            "ExitCode": "0:0",
            "WorkDir": "/home/user/jobs",
            "Partition": "gpu",
            "Account": "myaccount",
        }
        print_job_details("12345", status)
        captured = capsys.readouterr()
        assert "12345" in captured.out
        assert "COMPLETED" in captured.out
        assert "0:0" in captured.out

    def test_print_environments_table_empty(self, capsys):
        """Test environments table with no environments."""
        print_environments_table([])
        captured = capsys.readouterr()
        assert "No environments configured" in captured.out

    def test_print_environments_table_with_envs(self, capsys):
        """Test environments table with environments."""
        envs = [
            {
                "name": "default",
                "hostname": "cluster.example.com",
                "has_hostname": True,
                "slurmfile": "/path/to/Slurmfile",
            },
            {
                "name": "local",
                "hostname": "",
                "has_hostname": False,
                "slurmfile": "/path/to/Slurmfile",
            },
        ]
        print_environments_table(envs)
        captured = capsys.readouterr()
        assert "default" in captured.out
        assert "cluster.example.com" in captured.out
        assert "local" in captured.out

    def test_print_cluster_info_empty(self, capsys):
        """Test cluster info with no partitions."""
        print_cluster_info("default", {"partitions": []})
        captured = capsys.readouterr()
        assert "No partition information" in captured.out

    def test_print_cluster_info_with_partitions(self, capsys):
        """Test cluster info with partitions."""
        info = {
            "partitions": [
                {
                    "PARTITION": "gpu",
                    "AVAIL": "up",
                    "NODES": "10",
                    "STATE": "idle",
                    "TIMELIMIT": "7-00:00:00",
                    "CPUS": "48",
                    "MEMORY": "256G",
                },
            ]
        }
        print_cluster_info("default", info)
        captured = capsys.readouterr()
        assert "gpu" in captured.out
        assert "Cluster Partitions" in captured.out


class TestErrorHandling:
    """Test CLI error handling."""

    def test_missing_slurmfile_error(self, tmp_path, capsys):
        """Test error when Slurmfile not found."""
        nonexistent = tmp_path / "nonexistent"
        nonexistent.mkdir()

        with pytest.raises(SystemExit) as exc_info:
            main_with_args(["cluster", "list", "--slurmfile", str(nonexistent)])

        assert exc_info.value.code == 1

    def test_invalid_environment_error(self, tmp_path, capsys, monkeypatch):
        """Test error when environment not found."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        mock_cluster = MagicMock()
        mock_cluster.get_queue.return_value = []

        def mock_get_cluster(env=None, slurmfile=None):
            from slurm.errors import SlurmfileEnvironmentNotFoundError

            if env == "nonexistent":
                raise SlurmfileEnvironmentNotFoundError(
                    "Environment 'nonexistent' not found"
                )
            return mock_cluster

        monkeypatch.setattr("slurm.cli.jobs.get_cluster", mock_get_cluster)

        with pytest.raises(SystemExit) as exc_info:
            main_with_args(
                ["jobs", "list", "--env", "nonexistent", "-f", str(slurmfile)]
            )

        assert exc_info.value.code == 1


class TestJobsCommands:
    """Test jobs subcommand functionality with mocked cluster."""

    def test_jobs_list(self, tmp_path, capsys, monkeypatch):
        """Test jobs list with mocked cluster."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        mock_cluster = MagicMock()
        mock_cluster.get_queue.return_value = [
            {
                "JOBID": "99999",
                "NAME": "mock_job",
                "STATE": "RUNNING",
                "USER": "user",
                "TIME": "00:01:00",
                "PARTITION": "gpu",
                "NODES": "1",
            }
        ]

        monkeypatch.setattr(
            "slurm.cli.jobs.get_cluster", lambda env=None, slurmfile=None: mock_cluster
        )

        with pytest.raises(SystemExit) as exc_info:
            app(["jobs", "list", "-f", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "99999" in captured.out
        assert "mock_job" in captured.out

    def test_jobs_show(self, tmp_path, capsys, monkeypatch):
        """Test jobs show with mocked cluster."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        mock_job = MagicMock()
        mock_job.get_status.return_value = {
            "JobState": "COMPLETED",
            "ExitCode": "0:0",
            "WorkDir": "/home/user",
        }

        mock_cluster = MagicMock()
        mock_cluster.get_job.return_value = mock_job

        monkeypatch.setattr(
            "slurm.cli.jobs.get_cluster", lambda env=None, slurmfile=None: mock_cluster
        )

        with pytest.raises(SystemExit) as exc_info:
            app(["jobs", "show", "12345", "-f", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "12345" in captured.out
        assert "COMPLETED" in captured.out


class TestClusterShowCommand:
    """Test cluster show command with mocked cluster."""

    def test_cluster_show(self, tmp_path, capsys, monkeypatch):
        """Test cluster show with mocked cluster."""
        slurmfile = _write_sample_slurmfile(tmp_path)

        mock_cluster = MagicMock()
        mock_cluster.get_cluster_info.return_value = {
            "partitions": [
                {
                    "PARTITION": "gpu",
                    "AVAIL": "up",
                    "NODES": "5",
                    "STATE": "idle",
                },
            ]
        }

        monkeypatch.setattr(
            "slurm.cli.cluster.get_cluster",
            lambda env=None, slurmfile=None: mock_cluster,
        )

        with pytest.raises(SystemExit) as exc_info:
            app(["cluster", "show", "-f", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "gpu" in captured.out
        assert "Cluster Partitions" in captured.out


def main_with_args(args):
    """Helper to run main() with specific arguments."""
    import sys

    original_argv = sys.argv
    try:
        sys.argv = ["slurm"] + args
        main()
    finally:
        sys.argv = original_argv
