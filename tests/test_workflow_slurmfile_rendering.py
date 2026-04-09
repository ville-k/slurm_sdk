"""Tests for render_workflow_slurmfile() typed TOML generation."""

import tomlkit

from slurm._workflow import render_workflow_slurmfile
from cluster_factory import make_test_cluster  # type: ignore
from local_backend import LocalBackend  # type: ignore


def _make_cluster(tmp_path, **kwargs):
    return make_test_cluster(
        backend=LocalBackend(job_base_dir=str(tmp_path)),
        job_base_dir=str(tmp_path),
        **kwargs,
    )


def test_output_is_valid_toml(tmp_path):
    """Generated Slurmfile must parse cleanly as TOML."""
    cluster = _make_cluster(tmp_path)
    result = render_workflow_slurmfile(cluster, "test_env")
    doc = tomlkit.parse(result)
    assert "test_env" in doc


def test_cluster_section_present(tmp_path):
    """Cluster section contains backend type and job_base_dir."""
    cluster = _make_cluster(tmp_path)
    result = render_workflow_slurmfile(cluster, "myenv")
    doc = tomlkit.parse(result)

    assert "backend" in doc["myenv"]["cluster"]
    assert "job_base_dir" in doc["myenv"]["cluster"]


def test_boolean_values_preserved(tmp_path):
    """Booleans must round-trip as TOML booleans, not strings."""
    cluster = _make_cluster(
        tmp_path,
        default_packaging_kwargs={"push": True, "tls_verify": False},
    )
    result = render_workflow_slurmfile(cluster, "env")
    doc = tomlkit.parse(result)

    pkg = doc["env"]["packaging"]
    assert pkg["push"] is True
    assert pkg["tls_verify"] is False


def test_list_values_preserved(tmp_path):
    """Lists must round-trip as TOML arrays, not stringified."""
    cluster = _make_cluster(
        tmp_path,
        default_packaging_kwargs={"mounts": ["/data:/data", "/models:/models"]},
    )
    result = render_workflow_slurmfile(cluster, "env")
    doc = tomlkit.parse(result)

    pkg = doc["env"]["packaging"]
    assert isinstance(pkg["mounts"], list)
    assert pkg["mounts"] == ["/data:/data", "/models:/models"]


def test_integer_values_preserved(tmp_path):
    """Integers must round-trip as TOML integers, not strings."""
    cluster = _make_cluster(
        tmp_path,
        default_packaging_kwargs={"build_timeout": 300},
    )
    result = render_workflow_slurmfile(cluster, "env")
    doc = tomlkit.parse(result)

    assert doc["env"]["packaging"]["build_timeout"] == 300


def test_submit_defaults(tmp_path):
    """Submit section captures default account and partition."""
    cluster = _make_cluster(tmp_path)
    cluster.default_account = "research"
    cluster.default_partition = "gpu"

    result = render_workflow_slurmfile(cluster, "prod")
    doc = tomlkit.parse(result)

    assert doc["prod"]["submit"]["account"] == "research"
    assert doc["prod"]["submit"]["partition"] == "gpu"


def test_none_values_excluded(tmp_path):
    """None values should not appear in the output."""
    cluster = _make_cluster(
        tmp_path,
        default_packaging_kwargs={"registry": None, "platform": "linux/amd64"},
    )
    result = render_workflow_slurmfile(cluster, "env")
    doc = tomlkit.parse(result)

    pkg = doc["env"]["packaging"]
    assert "registry" not in pkg
    assert pkg["platform"] == "linux/amd64"
