from slurm import Cluster
from slurm.decorators import workflow
from slurm.workflow import WorkflowContext


class _DummyUploadBackend:
    def __init__(self, hostname_output: str) -> None:
        self._hostname_output = hostname_output
        self.uploads: list[tuple[str, str]] = []

    def execute_command(self, command: str) -> str:
        if command == "hostname":
            return self._hostname_output
        raise RuntimeError(f"Unexpected command: {command}")

    def _upload_string_to_file(self, content: str, remote_path: str) -> None:
        self.uploads.append((remote_path, content))


@workflow(time="00:01:00")
def _wf(ctx: WorkflowContext) -> None:
    return None


def test_workflow_slurmfile_uploaded_without_source_slurmfile(tmp_path) -> None:
    job_base_dir = tmp_path / "slurm_jobs"
    cluster = Cluster(
        backend_type="local",
        job_base_dir=str(job_base_dir),
        default_packaging="wheel",
        default_partition="debug",
    )
    cluster.backend_type = "ssh"
    cluster._backend_kwargs = {
        "hostname": "127.0.0.1",
        "port": 2222,
        "username": "slurm",
        "password": "slurm",
        "look_for_keys": False,
        "allow_agent": False,
        "banner_timeout": 30,
        "job_base_dir": str(job_base_dir),
    }
    cluster.backend = _DummyUploadBackend("slurm-control\n")  # type: ignore[assignment]

    cluster._handle_workflow_slurmfile(
        task_func=_wf,
        pre_submission_id="pre123",
        target_job_dir="/home/slurm/slurm_jobs/wf/ts_pre123",
    )

    assert cluster.backend.uploads, "Expected workflow Slurmfile upload"
    remote_path, content = cluster.backend.uploads[-1]
    assert remote_path.endswith("/Slurmfile.toml")
    assert "[default.cluster]" in content
    assert "[default.cluster.backend_config]" in content
    assert 'hostname = "slurm-control"' in content
    assert "port = 22" in content
    assert 'password = "slurm"' in content
