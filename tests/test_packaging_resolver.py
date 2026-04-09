"""Tests for the centralized packaging resolution logic."""

from slurm._packaging_resolver import resolve_packaging_config
from slurm.decorators import task


@task(time="00:01:00")
def sample_task(x: int) -> int:
    return x


@task(time="00:01:00", packaging="container:myimage:latest")
def container_task(x: int) -> int:
    return x


class FakeCluster:
    """Minimal cluster stub for testing packaging resolution."""

    def __init__(self, **kwargs):
        self.default_packaging = kwargs.get("default_packaging")
        self.default_packaging_kwargs = kwargs.get("default_packaging_kwargs", {})
        self._prebuilt_dependency_images = kwargs.get("prebuilt_images", {})
        self.packaging_defaults = kwargs.get("packaging_defaults")


def test_explicit_config_takes_precedence():
    """Explicit config beats everything else."""
    cluster = FakeCluster(default_packaging="wheel")
    explicit = {"type": "none"}
    result = resolve_packaging_config(cluster, sample_task, explicit_config=explicit)
    assert result == {"type": "none"}


def test_prebuilt_images_highest_priority():
    """Pre-built images beat explicit config."""
    task_name = f"{sample_task.func.__module__}.{sample_task.func.__qualname__}"
    cluster = FakeCluster(prebuilt_images={task_name: "registry.io/prebuilt:abc"})
    result = resolve_packaging_config(
        cluster, sample_task, explicit_config={"type": "wheel"}
    )
    assert result["type"] == "container"
    assert result["image"] == "registry.io/prebuilt:abc"


def test_task_level_packaging():
    """Task with concrete packaging type uses its own config."""
    cluster = FakeCluster()
    result = resolve_packaging_config(cluster, container_task)
    assert result["type"] == "container"
    assert result["image"] == "myimage:latest"


def test_cluster_default_packaging():
    """Cluster default_packaging used when task has auto packaging."""
    cluster = FakeCluster(
        default_packaging="wheel", default_packaging_kwargs={"build_tool": "uv"}
    )
    result = resolve_packaging_config(cluster, sample_task)
    assert result["type"] == "wheel"
    assert result["build_tool"] == "uv"


def test_slurmfile_packaging_defaults():
    """Legacy Slurmfile [packaging] table used as fallback."""
    cluster = FakeCluster(packaging_defaults={"type": "container", "image": "base:1.0"})
    result = resolve_packaging_config(cluster, sample_task)
    assert result["type"] == "container"
    assert result["image"] == "base:1.0"


def test_task_packaging_final_fallback():
    """Task's own packaging returned when nothing else matches."""
    cluster = FakeCluster()
    result = resolve_packaging_config(cluster, sample_task)
    assert result == sample_task.packaging
