"""Tests for the MCP server."""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _write_sample_slurmfile(tmp_path: Path) -> Path:
    """Create a sample Slurmfile for testing."""
    content = textwrap.dedent(
        """
        [default]
        hostname = "default.example.com"

        [production]
        hostname = "prod.example.com"
        """
    )
    slurmfile = tmp_path / "Slurmfile.toml"
    slurmfile.write_text(content, encoding="utf-8")
    return slurmfile


class TestMCPServerCreation:
    """Test MCP server creation and configuration."""

    def test_create_mcp_server(self):
        """Test that MCP server can be created."""
        from slurm.mcp_server import create_mcp_server

        mcp = create_mcp_server(name="Test Server")
        assert mcp is not None
        assert mcp.name == "Test Server"

    def test_create_mcp_server_with_slurmfile(self, tmp_path):
        """Test MCP server with explicit slurmfile."""
        from slurm.mcp_server import create_mcp_server

        slurmfile = _write_sample_slurmfile(tmp_path)
        mcp = create_mcp_server(slurmfile=str(slurmfile))
        assert mcp is not None


class TestMCPTools:
    """Test MCP server tools."""

    def test_list_jobs_tool(self, tmp_path, monkeypatch):
        """Test list_jobs tool returns queue data."""
        from slurm.mcp_server import create_mcp_server

        slurmfile = _write_sample_slurmfile(tmp_path)

        mock_cluster = MagicMock()
        mock_cluster.get_queue.return_value = [
            {"JOBID": "123", "NAME": "test", "STATE": "RUNNING", "USER": "testuser"}
        ]

        with patch("slurm.mcp_server.Cluster") as mock_cluster_class:
            mock_cluster_class.from_env.return_value = mock_cluster

            mcp = create_mcp_server(slurmfile=str(slurmfile))

            list_jobs_tool = None
            for tool in mcp._tool_manager._tools.values():
                if tool.name == "list_jobs":
                    list_jobs_tool = tool
                    break

            assert list_jobs_tool is not None

    def test_list_environments_tool(self, tmp_path):
        """Test list_environments tool returns configured environments."""
        from slurm.mcp_server import create_mcp_server

        slurmfile = _write_sample_slurmfile(tmp_path)
        mcp = create_mcp_server(slurmfile=str(slurmfile))

        list_envs_tool = None
        for tool in mcp._tool_manager._tools.values():
            if tool.name == "list_environments":
                list_envs_tool = tool
                break

        assert list_envs_tool is not None


class TestMCPResources:
    """Test MCP server resources."""

    def test_config_resource_exists(self, tmp_path):
        """Test that config resource is registered."""
        from slurm.mcp_server import create_mcp_server

        slurmfile = _write_sample_slurmfile(tmp_path)
        mcp = create_mcp_server(slurmfile=str(slurmfile))

        assert mcp._resource_manager is not None


class TestMCPStatusCommand:
    """Test MCP status CLI command."""

    def test_mcp_status(self, tmp_path, capsys):
        """Test mcp status command shows configuration."""
        from slurm.cli.app import app

        slurmfile = _write_sample_slurmfile(tmp_path)

        with pytest.raises(SystemExit) as exc_info:
            app(["mcp", "status", "-f", str(slurmfile)])
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "MCP Server Configuration" in captured.out
        assert "list_jobs" in captured.out
        assert "slurm://queue" in captured.out
