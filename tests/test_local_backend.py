"""Unit tests for LocalBackend."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
import subprocess

from slurm.api.local import LocalBackend
from slurm.errors import BackendTimeout, BackendCommandError


@pytest.fixture
def temp_job_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def backend(temp_job_dir):
    """Create a LocalBackend instance for testing."""
    return LocalBackend(job_base_dir=temp_job_dir)


def test_local_backend_init(temp_job_dir):
    """Test LocalBackend initialization."""
    backend = LocalBackend(job_base_dir=temp_job_dir)

    assert backend.job_base_dir == temp_job_dir
    assert os.path.exists(backend.job_base_dir)
    assert backend.timeout == 30


def test_local_backend_init_with_tilde():
    """Test LocalBackend initialization with ~ in path."""
    backend = LocalBackend(job_base_dir="~/test_slurm_jobs")

    # Should be expanded to absolute path
    assert not backend.job_base_dir.startswith("~")
    assert os.path.isabs(backend.job_base_dir)


def test_resolve_path(backend, temp_job_dir):
    """Test path resolution."""
    # Absolute path
    abs_path = backend._resolve_path(temp_job_dir)
    assert abs_path == temp_job_dir

    # Path with ~
    home_path = backend._resolve_path("~/test")
    assert not home_path.startswith("~")
    assert os.path.isabs(home_path)


@patch("subprocess.run")
def test_run_command_success(mock_run, backend):
    """Test successful command execution."""
    mock_run.return_value = MagicMock(stdout="output", stderr="", returncode=0)

    stdout, stderr, returncode = backend._run_command("echo test", check=False)

    assert stdout == "output"
    assert stderr == ""
    assert returncode == 0
    mock_run.assert_called_once()


@patch("subprocess.run")
def test_run_command_failure(mock_run, backend):
    """Test command execution failure."""
    mock_run.return_value = MagicMock(stdout="", stderr="error", returncode=1)

    with pytest.raises(BackendCommandError):
        backend._run_command("false", check=True)


@patch("subprocess.run")
def test_run_command_timeout(mock_run, backend):
    """Test command timeout."""
    mock_run.side_effect = subprocess.TimeoutExpired("test", 30)

    with pytest.raises(BackendTimeout):
        backend._run_command("sleep 100", timeout=1)


@patch("subprocess.run")
def test_submit_job(mock_run, backend, temp_job_dir):
    """Test job submission."""
    mock_run.return_value = MagicMock(
        stdout="Submitted batch job 12345", stderr="", returncode=0
    )

    script = "#!/bin/bash\necho hello"
    target_job_dir = os.path.join(temp_job_dir, "test_job")

    job_id = backend.submit_job(
        script=script,
        target_job_dir=target_job_dir,
        pre_submission_id="test123",
        account=None,
        partition=None,
    )

    assert job_id == "12345"
    assert os.path.exists(target_job_dir)
    mock_run.assert_called_once()


@patch("subprocess.run")
def test_submit_job_with_options(mock_run, backend, temp_job_dir):
    """Test job submission with account and partition."""
    mock_run.return_value = MagicMock(
        stdout="Submitted batch job 54321", stderr="", returncode=0
    )

    script = "#!/bin/bash\necho hello"
    target_job_dir = os.path.join(temp_job_dir, "test_job")

    job_id = backend.submit_job(
        script=script,
        target_job_dir=target_job_dir,
        pre_submission_id="test456",
        account="myaccount",
        partition="gpu",
    )

    assert job_id == "54321"

    # Verify sbatch command includes account and partition
    call_args = mock_run.call_args[0][0]
    assert "--account=" in call_args
    assert "myaccount" in call_args
    assert "--partition=" in call_args
    assert "gpu" in call_args


@patch("subprocess.run")
def test_submit_job_failure(mock_run, backend, temp_job_dir):
    """Test job submission failure."""
    mock_run.return_value = MagicMock(
        stdout="", stderr="sbatch: error: Batch job submission failed", returncode=1
    )

    script = "#!/bin/bash\necho hello"
    target_job_dir = os.path.join(temp_job_dir, "test_job")

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        backend.submit_job(
            script=script, target_job_dir=target_job_dir, pre_submission_id="test789"
        )


@patch("subprocess.run")
def test_get_job_status_success(mock_run, backend):
    """Test getting job status."""
    mock_run.return_value = MagicMock(
        stdout="JobId=12345 JobState=RUNNING UserId=user(1000) ExitCode=0:0",
        stderr="",
        returncode=0,
    )

    status = backend.get_job_status("12345")

    assert status["JobId"] == "12345"
    assert status["JobState"] == "RUNNING"
    assert "ExitCode" in status


@patch("subprocess.run")
def test_get_job_status_not_found(mock_run, backend):
    """Test getting status of non-existent job."""
    mock_run.return_value = MagicMock(
        stdout="",
        stderr="slurm_load_jobs error: Invalid job id specified",
        returncode=1,
    )

    with pytest.raises(BackendCommandError, match="Job 99999 not found in SLURM queue"):
        backend.get_job_status("99999")


@patch("subprocess.run")
def test_cancel_job_success(mock_run, backend):
    """Test job cancellation."""
    mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)

    result = backend.cancel_job("12345")

    assert result is True
    mock_run.assert_called_once()


@patch("subprocess.run")
def test_cancel_job_failure(mock_run, backend):
    """Test job cancellation failure."""
    mock_run.return_value = MagicMock(
        stdout="", stderr="scancel: error: Invalid job id specified", returncode=1
    )

    with pytest.raises(BackendCommandError):
        backend.cancel_job("99999")


@patch("subprocess.run")
def test_get_queue(mock_run, backend):
    """Test getting job queue."""
    mock_run.return_value = MagicMock(
        stdout="101|job1|RUNNING|user|0:05|1:00:00|normal|account1\n102|job2|PENDING|user|0:00|2:00:00|gpu|account2\n",
        stderr="",
        returncode=0,
    )

    queue = backend.get_queue()

    assert len(queue) == 2
    assert queue[0]["JOBID"] == "101"
    assert queue[0]["STATE"] == "RUNNING"
    assert queue[1]["JOBID"] == "102"
    assert queue[1]["STATE"] == "PENDING"


@patch("subprocess.run")
def test_get_queue_failure(mock_run, backend):
    """Test getting queue when squeue fails."""
    mock_run.return_value = MagicMock(
        stdout="", stderr="squeue: error: Connection refused", returncode=1
    )

    queue = backend.get_queue()

    # Should return empty list instead of raising
    assert queue == []


@patch("subprocess.run")
def test_get_cluster_info(mock_run, backend):
    """Test getting cluster info."""
    mock_run.return_value = MagicMock(
        stdout="normal|up|infinite|10|idle\ngpu|up|24:00:00|5|mixed\n",
        stderr="",
        returncode=0,
    )

    info = backend.get_cluster_info()

    assert "partitions" in info
    assert len(info["partitions"]) == 2
    assert info["partitions"][0]["PARTITION"] == "normal"
    assert info["partitions"][1]["PARTITION"] == "gpu"


@patch("subprocess.run")
def test_get_cluster_info_failure(mock_run, backend):
    """Test getting cluster info when sinfo fails."""
    mock_run.return_value = MagicMock(
        stdout="",
        stderr="sinfo: error: Unable to contact slurm controller",
        returncode=1,
    )

    # Should raise BackendCommandError on failure
    with pytest.raises(BackendCommandError, match="Failed to get cluster info"):
        backend.get_cluster_info()


@patch("subprocess.run")
def test_execute_command_success(mock_run, backend):
    """Test execute_command method."""
    mock_run.return_value = MagicMock(stdout="command output", stderr="", returncode=0)

    output = backend.execute_command("echo test")

    assert output == "command output"


@patch("subprocess.run")
def test_execute_command_failure(mock_run, backend):
    """Test execute_command with failure."""
    mock_run.return_value = MagicMock(stdout="", stderr="command failed", returncode=1)

    with pytest.raises(RuntimeError, match="Command failed"):
        backend.execute_command("false")


def test_backend_environment_variables(temp_job_dir):
    """Test that backend uses environment variables."""
    env_vars = {"TEST_VAR": "test_value"}
    backend = LocalBackend(job_base_dir=temp_job_dir, env=env_vars)

    assert backend.env == env_vars


@patch("subprocess.run")
def test_backend_merges_environment(mock_run, temp_job_dir):
    """Test that backend merges environment variables with os.environ."""
    env_vars = {"TEST_VAR": "test_value"}
    backend = LocalBackend(job_base_dir=temp_job_dir, env=env_vars)

    mock_run.return_value = MagicMock(stdout="output", stderr="", returncode=0)

    backend._run_command("echo test")

    # Check that env was passed to subprocess.run
    call_kwargs = mock_run.call_args[1]
    assert "env" in call_kwargs
    assert "TEST_VAR" in call_kwargs["env"]
    assert call_kwargs["env"]["TEST_VAR"] == "test_value"


@patch("subprocess.run")
def test_submit_job_persists_script(mock_run, backend, temp_job_dir):
    """Test that job script is persisted in job directory."""
    mock_run.return_value = MagicMock(
        stdout="Submitted batch job 12345", stderr="", returncode=0
    )

    script = "#!/bin/bash\necho hello"
    target_job_dir = os.path.join(temp_job_dir, "test_job")
    pre_submission_id = "test123"

    job_id = backend.submit_job(
        script=script,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
        account=None,
        partition=None,
    )

    assert job_id == "12345"
    assert os.path.exists(target_job_dir)

    # Verify script was persisted
    expected_script_path = os.path.join(
        target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"
    )
    assert os.path.exists(expected_script_path), (
        f"Script not found at {expected_script_path}"
    )

    # Verify script content
    with open(expected_script_path, "r") as f:
        persisted_script = f.read()
    assert persisted_script == script

    # Verify script is executable
    assert os.access(expected_script_path, os.X_OK)


@patch("subprocess.run")
def test_submit_job_script_filename_format(mock_run, backend, temp_job_dir):
    """Test that script filename follows expected format."""
    mock_run.return_value = MagicMock(
        stdout="Submitted batch job 12345", stderr="", returncode=0
    )

    script = "#!/bin/bash\necho hello"
    target_job_dir = os.path.join(temp_job_dir, "test_job")
    pre_submission_id = "20250109_212626_81f18a7e"

    backend.submit_job(
        script=script,
        target_job_dir=target_job_dir,
        pre_submission_id=pre_submission_id,
    )

    expected_script_path = os.path.join(
        target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"
    )
    assert os.path.exists(expected_script_path)
    assert expected_script_path.endswith("slurm_job_20250109_212626_81f18a7e_script.sh")
