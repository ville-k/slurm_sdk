import textwrap
from pathlib import Path

import pytest

from slurm.cluster import Cluster
from slurm.errors import SlurmfileInvalidError


class DummyBackend:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.job_base_dir = kwargs.get("job_base_dir")


def _write_sample_slurmfile(tmp_path: Path) -> Path:
    content = textwrap.dedent(
        """
        [default.cluster]
        backend = "ssh"
        job_base_dir = "/var/slurm/default"

        [default.cluster.backend_config]
        hostname = "default.example.com"
        username = "default-user"

        [default.packaging]
        type = "none"

        [local.cluster]
        job_base_dir = "/var/slurm/local"

        [local.cluster.backend_config]
        hostname = "local.example.com"
        port = 10022

        [local.packaging]
        type = "wheel"
        """
    )
    slurmfile = tmp_path / "Slurmfile.toml"
    slurmfile.write_text(content, encoding="utf-8")
    return slurmfile


def test_cluster_from_env_loads_environment(monkeypatch, tmp_path):
    slurmfile = _write_sample_slurmfile(tmp_path)

    captured = {}

    def fake_create_backend(backend_type, **kwargs):
        captured["backend_type"] = backend_type
        captured["kwargs"] = kwargs
        return DummyBackend(**kwargs)

    monkeypatch.setattr("slurm.cluster.create_backend", fake_create_backend)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SLURM_ENV", "local")

    cluster = Cluster.from_env()

    assert cluster.env_name == "local"
    assert cluster.slurmfile_path == str(slurmfile)
    assert captured["backend_type"] == "ssh"
    assert captured["kwargs"]["hostname"] == "local.example.com"
    assert captured["kwargs"]["username"] == "default-user"
    assert captured["kwargs"]["port"] == 10022
    assert cluster.backend.job_base_dir == "/var/slurm/local"
    assert cluster.packaging_defaults == {"type": "wheel"}


def test_cluster_from_env_supports_explicit_path_and_overrides(monkeypatch, tmp_path):
    slurmfile = _write_sample_slurmfile(tmp_path)

    captured = {}

    def fake_create_backend(backend_type, **kwargs):
        captured["backend_type"] = backend_type
        captured["kwargs"] = kwargs
        return DummyBackend(**kwargs)

    monkeypatch.setattr("slurm.cluster.create_backend", fake_create_backend)

    cluster = Cluster.from_env(
        str(slurmfile),
        env="local",
        overrides={"backend_config": {"port": 4242}},
    )

    assert cluster.env_name == "local"
    assert captured["backend_type"] == "ssh"
    assert captured["kwargs"]["port"] == 4242
    assert cluster.backend.job_base_dir == "/var/slurm/local"


def test_cluster_from_env_requires_cluster_section(monkeypatch, tmp_path):
    slurmfile = tmp_path / "Slurmfile.toml"
    slurmfile.write_text("[default]\nvalue = 1\n", encoding="utf-8")

    def fail_create_backend(*args, **kwargs):  # pragma: no cover - should not be called
        pytest.fail("create_backend should not be invoked when Slurmfile is invalid")

    monkeypatch.setattr("slurm.cluster.create_backend", fail_create_backend)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SlurmfileInvalidError):
        Cluster.from_env()
