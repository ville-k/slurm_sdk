import pytest

from slurm.errors import PackagingError
from slurm.packaging.container import ContainerPackagingStrategy


def _dummy_task():
    return None


def test_prepare_requires_image_or_dockerfile(tmp_path):
    strategy = ContainerPackagingStrategy({})

    with pytest.raises(PackagingError):
        strategy.prepare(task=_dummy_task, cluster=object())


def test_dockerfile_without_context_defaults_to_project_root(tmp_path, monkeypatch):
    dockerfile_dir = tmp_path / "docker"
    dockerfile_dir.mkdir()
    dockerfile = dockerfile_dir / "Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim\n", encoding="utf-8")

    strategy = ContainerPackagingStrategy({"dockerfile": str(dockerfile)})
    monkeypatch.setattr(strategy, "_resolve_project_root", lambda: tmp_path)

    dockerfile_path, context_path = strategy._resolve_build_paths()

    assert dockerfile_path == dockerfile.resolve()
    assert context_path == tmp_path
    assert strategy.context == "."


def test_prepare_builds_and_pushes(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim\n", encoding="utf-8")

    config = {
        "dockerfile": str(dockerfile),
        "context": str(tmp_path),
        "registry": "registry.example.com/team",
        "name": "demo",
        "tag": "v1",
        "push": True,
        "python_executable": "/usr/bin/python3",
        "srun_args": ["--mpi=none"],
        "platform": "linux/amd64",
        "build_secrets": [
            {"id": "demo_user", "env": "DEMO_USER"},
            {"id": "demo_pass", "env": "DEMO_PASS"},
        ],
    }

    strategy = ContainerPackagingStrategy(config)

    monkeypatch.setattr(
        ContainerPackagingStrategy,
        "_detect_runtime",
        lambda self: "docker",
    )

    captured_runs = []

    def _capture_run(self, cmd, **kwargs):
        captured_runs.append({"cmd": list(cmd), **kwargs})

    monkeypatch.setattr(
        ContainerPackagingStrategy,
        "_run_container_command",
        _capture_run,
    )

    monkeypatch.setenv("DEMO_USER", "user1")
    monkeypatch.setenv("DEMO_PASS", "pass1")

    result = strategy.prepare(task=_dummy_task, cluster=object())

    expected_image = "registry.example.com/team/demo:v1"
    assert result["image"] == expected_image
    assert result["built"] is True
    assert result["pushed"] is True
    assert result["build_secrets"] == ["demo_user", "demo_pass"]

    build_invocation = captured_runs[0]
    assert build_invocation["description"] == "build"
    assert build_invocation["cmd"][0:3] == ["docker", "build", "-t"]
    assert any("--secret" in part for part in build_invocation["cmd"])
    assert any("id=demo_user,env=DEMO_USER" in part for part in build_invocation["cmd"])
    assert any("id=demo_pass,env=DEMO_PASS" in part for part in build_invocation["cmd"])
    assert any(str(dockerfile) in part for part in build_invocation["cmd"])
    assert build_invocation["cmd"][-1] == str(tmp_path)
    env = build_invocation.get("env") or {}
    assert env.get("DOCKER_BUILDKIT") == "1"

    push_invocation = captured_runs[1]
    assert push_invocation["description"] == "push"
    # Docker doesn't support --format or --tls-verify flags (those are podman-specific)
    assert push_invocation["cmd"] == [
        "docker",
        "push",
        expected_image,
    ]

    setup_commands = strategy.generate_setup_commands(
        task=_dummy_task, job_id="job123", job_dir='"$JOB_DIR"'
    )

    assert any("CONTAINER_IMAGE" in line for line in setup_commands)
    # Single-word executable is set as a simple variable
    assert any("PY_EXEC=/usr/bin/python3" in line for line in setup_commands)

    wrapped = strategy.wrap_execution_command(
        command='"${PY_EXEC[@]:-python}" -m slurm.runner',
        task=_dummy_task,
        job_id="job123",
        job_dir='"$JOB_DIR"',
    )

    assert wrapped.startswith(
        "srun --mpi=none --container-image=registry.example.com/team/demo:v1"
    )
    assert (
        '--container-mounts="$(dirname $(dirname $JOB_DIR)):$(dirname $(dirname $JOB_DIR)):rw"'
        in wrapped
    )
    assert '--container-workdir="$JOB_DIR"' in wrapped
    # The command is passed through directly - shell expansion happens before srun
    assert '"${PY_EXEC[@]:-python}" -m slurm.runner' in wrapped


def test_uv_run_python_sets_py_exec_as_array(tmp_path):
    """Test that multi-word python executables like 'uv run python' are set as a bash array."""
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim\n", encoding="utf-8")

    config = {
        "dockerfile": str(dockerfile),
        "context": str(tmp_path),
        "name": "uv-demo",
        "tag": "latest",
        "python_executable": "uv run python",
    }

    strategy = ContainerPackagingStrategy(config)
    strategy._image_reference = "uv-demo:latest"

    setup_commands = strategy.generate_setup_commands(
        task=_dummy_task, job_id="test123", job_dir='"$JOB_DIR"'
    )

    # Multi-word executable should be set as a bash array
    setup_script = "\n".join(setup_commands)
    assert "PY_EXEC=(uv run python)" in setup_script

    wrapped = strategy.wrap_execution_command(
        command='"${PY_EXEC[@]:-python}" -m slurm.runner --module foo --function bar',
        task=_dummy_task,
        job_id="test123",
        job_dir='"$JOB_DIR"',
    )

    # The command should be passed through with the array variable reference
    assert "srun" in wrapped
    assert "--container-image=uv-demo:latest" in wrapped
    assert '"${PY_EXEC[@]:-python}" -m slurm.runner' in wrapped


def test_mount_dict_entries_render(monkeypatch):
    config = {
        "image": "demo:latest",
        "mounts": [
            {"host_path": "/data", "container_path": "/mnt/data", "mode": "ro"},
            "/opt/tools:/opt/tools:ro",
        ],
        "mount_job_dir": False,
    }

    strategy = ContainerPackagingStrategy(config)
    strategy._image_reference = "demo:latest"

    command = strategy.wrap_execution_command(
        command='"${PY_EXEC[@]:-python}" -m slurm.runner',
        task=_dummy_task,
        job_id="job456",
        job_dir='"$JOB_DIR"',
    )

    assert "--container-image=demo:latest" in command
    assert '--container-mounts="/data:/mnt/data:ro,/opt/tools:/opt/tools:ro"' in command


def test_missing_required_build_secret_raises(monkeypatch, tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim\n", encoding="utf-8")

    config = {
        "dockerfile": str(dockerfile),
        "context": str(tmp_path),
        "build_secrets": [{"id": "needs_env", "env": "MISSING_ENV"}],
    }

    strategy = ContainerPackagingStrategy(config)
    monkeypatch.setattr(
        ContainerPackagingStrategy,
        "_detect_runtime",
        lambda self: "docker",
    )
    monkeypatch.setattr(
        ContainerPackagingStrategy,
        "_run_container_command",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(PackagingError):
        strategy.prepare(task=_dummy_task, cluster=object())
