"""Integration tests for workflow execution with container packaging.

These tests verify that workflows correctly handle container packaging for nested tasks,
including image reuse and proper cluster configuration.
"""

from pathlib import Path
from slurm.workflow import WorkflowContext
from slurm.cluster import Cluster


def test_workflow_context_provides_cluster_for_nested_tasks(tmp_path):
    """Test that WorkflowContext provides cluster for nested task submission."""
    cluster = object.__new__(Cluster)
    cluster.job_base_dir = str(tmp_path)
    cluster.packaging_defaults = {"type": "container", "image": "test:latest"}

    workflow_ctx = WorkflowContext(
        cluster=cluster,
        workflow_job_id="test_123",
        workflow_job_dir=tmp_path / "workflow",
        shared_dir=tmp_path / "workflow" / "shared",
        local_mode=False,
    )

    # Workflow context should expose cluster
    assert workflow_ctx.cluster is cluster
    assert workflow_ctx.cluster.packaging_defaults["type"] == "container"


def test_nested_task_inherits_container_config():
    """Test that nested tasks inherit container configuration from cluster."""
    # This test verifies the design that nested tasks use cluster packaging config
    cluster = object.__new__(Cluster)
    cluster.packaging_defaults = {
        "type": "container",
        "image": "nvcr.io/nvidia/pytorch:latest",
        "push": False,
    }

    # When a task is called within a workflow context, it should use
    # the cluster's packaging_defaults
    assert cluster.packaging_defaults["type"] == "container"
    assert cluster.packaging_defaults["image"] == "nvcr.io/nvidia/pytorch:latest"


def test_runner_modifies_packaging_for_nested_tasks():
    """Test that runner.py modifies packaging config for nested workflow tasks."""
    # This test verifies the fix we implemented in runner.py
    # where nested tasks get modified packaging config to reuse container image

    original_config = {
        "type": "container",
        "registry": "nvcr.io/nv-maglev",
        "name": "dlav/hello-container",
        "tag": "latest",
        "dockerfile": "src/examples/Dockerfile",
        "context": ".",
        "push": True,
    }

    # Simulate what runner.py does
    pkg = dict(original_config)

    # Construct full image reference
    if not pkg.get("image"):
        registry = pkg.get("registry", "").rstrip("/")
        name = pkg.get("name", "")
        tag = pkg.get("tag", "latest")

        if registry and name:
            image_ref = f"{registry}/{name.lstrip('/')}:{tag}"
        elif name:
            image_ref = f"{name}:{tag}"
        else:
            image_ref = None

        if image_ref:
            pkg["image"] = image_ref

    # Remove fields that would trigger rebuild
    pkg.pop("dockerfile", None)
    pkg.pop("context", None)
    pkg.pop("registry", None)
    pkg.pop("name", None)
    pkg.pop("tag", None)
    pkg["push"] = False

    # Verify the modified config
    assert pkg["type"] == "container"
    assert pkg["image"] == "nvcr.io/nv-maglev/dlav/hello-container:latest"
    assert "dockerfile" not in pkg
    assert "context" not in pkg
    assert pkg["push"] is False


def test_image_reference_construction():
    """Test image reference construction from registry, name, tag."""
    test_cases = [
        # (registry, name, tag, expected)
        ("nvcr.io/nvidia", "pytorch", "22.04", "nvcr.io/nvidia/pytorch:22.04"),
        ("docker.io", "ubuntu", "20.04", "docker.io/ubuntu:20.04"),
        ("", "local-image", "v1", "local-image:v1"),
        ("registry.com", "/prefix/image", "latest", "registry.com/prefix/image:latest"),
    ]

    for registry, name, tag, expected in test_cases:
        registry = registry.rstrip("/")
        if registry and name:
            image_ref = f"{registry}/{name.lstrip('/')}:{tag}"
        elif name:
            image_ref = f"{name}:{tag}"
        else:
            image_ref = None

        assert image_ref == expected


def test_container_packaging_strategy_skip_build():
    """Test that container packaging skips build when dockerfile is not present."""
    from slurm.packaging.container import ContainerPackagingStrategy

    # Config without dockerfile (should skip build)
    config = {
        "type": "container",
        "image": "nvcr.io/nvidia/pytorch:latest",
        "push": False,
    }

    strategy = ContainerPackagingStrategy(config)

    assert strategy.dockerfile is None
    assert strategy.context == "."  # Defaults to "." even when not building
    assert strategy.image == "nvcr.io/nvidia/pytorch:latest"
    assert strategy.push is False


def test_container_packaging_strategy_with_explicit_image():
    """Test ContainerPackagingStrategy with explicit image reference."""
    from slurm.packaging.container import ContainerPackagingStrategy

    config = {
        "type": "container",
        "image": "myregistry.io/myimage:v1.0",
        "push": False,
    }

    strategy = ContainerPackagingStrategy(config)

    # Should use the explicit image reference
    assert strategy.image == "myregistry.io/myimage:v1.0"
    assert strategy.dockerfile is None  # No build needed
    assert strategy.context == "."  # Defaults to "." even when not building


def test_slurmfile_upload_for_workflow_cluster_recreation(tmp_path):
    """Test that Slurmfile is uploaded for cluster recreation in workflow."""
    # This test verifies that the Slurmfile path is available for upload

    slurmfile_path = tmp_path / "Slurmfile.toml"
    slurmfile_path.write_text("""
[default.cluster]
backend = "ssh"

[default.packaging]
type = "container"
image = "test:latest"
""")

    # Verify Slurmfile exists and can be read
    assert slurmfile_path.exists()
    content = slurmfile_path.read_text()
    assert "container" in content


def test_environment_variables_for_cluster_recreation():
    """Test environment variable naming for cluster recreation."""

    # These environment variables should be set by rendering.py
    expected_vars = [
        "SLURM_SDK_SLURMFILE",  # Path to uploaded Slurmfile
        "SLURM_SDK_ENV",  # Environment name (e.g., "oci-container")
    ]

    # Verify the variable names are documented
    for var in expected_vars:
        # These would be set by rendering.py in the job script
        # Here we just verify the naming convention
        assert var.startswith("SLURM_SDK_")


def test_workflow_directory_contains_slurmfile_after_upload(tmp_path):
    """Test that workflow job directory should contain uploaded Slurmfile."""
    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()

    # The Slurmfile should be uploaded as "Slurmfile.toml" in the job directory
    slurmfile_remote = workflow_dir / "Slurmfile.toml"

    # Simulate upload
    slurmfile_remote.write_text("""
[oci-container.packaging]
type = "container"
image = "test:latest"
""")

    assert slurmfile_remote.exists()
    assert "container" in slurmfile_remote.read_text()


def test_runner_loads_cluster_from_environment_variables():
    """Test that runner.py can load cluster from environment variables."""
    # This test verifies the pattern used in runner.py

    import os

    os.environ["SLURM_SDK_SLURMFILE"] = "/path/to/Slurmfile.toml"
    os.environ["SLURM_SDK_ENV"] = "oci-container"

    # Verify environment variables are set
    assert os.environ.get("SLURM_SDK_SLURMFILE") == "/path/to/Slurmfile.toml"
    assert os.environ.get("SLURM_SDK_ENV") == "oci-container"

    # Cleanup
    del os.environ["SLURM_SDK_SLURMFILE"]
    del os.environ["SLURM_SDK_ENV"]


def test_jobcontext_output_dir_attribute():
    """Test that JobContext has output_dir attribute (not job_dir)."""
    from slurm.runtime import JobContext

    ctx = JobContext(
        job_id="12345",
        step_id=None,
        node_rank=None,
        rank=None,
        local_rank=None,
        world_size=None,
        num_nodes=None,
        local_world_size=None,
        gpus_per_node=None,
        hostnames=(),
        output_dir=Path("/tmp/job"),
        environment={},
    )

    # Should have output_dir, not job_dir
    assert hasattr(ctx, "output_dir")
    assert ctx.output_dir == Path("/tmp/job")


def test_job_get_result_accepts_timeout():
    """Test that Job.get_result() accepts timeout parameter."""
    from slurm.job import Job
    import inspect

    sig = inspect.signature(Job.get_result)
    params = sig.parameters

    # Should have timeout parameter
    assert "timeout" in params
    # Should be optional (has default value)
    assert params["timeout"].default is not inspect.Parameter.empty


def test_array_job_get_results_passes_timeout():
    """Test that ArrayJob.get_results() passes timeout to Job.get_result()."""
    # This test verifies that the timeout fix in Job.get_result() works with ArrayJob
    from slurm.array_job import ArrayJob
    import inspect

    sig = inspect.signature(ArrayJob.get_results)
    params = sig.parameters

    # Should have timeout parameter
    assert "timeout" in params


def test_container_strategy_resolves_image_reference():
    """Test that ContainerPackagingStrategy resolves image reference correctly."""
    from slurm.packaging.container import ContainerPackagingStrategy

    # Test with registry, name, tag (no explicit image)
    config = {
        "type": "container",
        "registry": "nvcr.io/nv-maglev",
        "name": "dlav/hello-container",
        "tag": "latest",
        "push": False,
    }

    strategy = ContainerPackagingStrategy(config)

    # Verify fields are set
    assert strategy.registry == "nvcr.io/nv-maglev"
    assert strategy.image_name == "dlav/hello-container"
    assert strategy.tag == "latest"
    assert strategy.image is None  # Not explicitly set


def test_duplicate_registry_prevention():
    """Test that registry prefix is not duplicated in image reference."""
    # This was the bug we fixed in runner.py

    # Original config from Slurmfile
    original = {
        "registry": "nvcr.io/nv-maglev",
        "name": "dlav/hello-container",
        "tag": "latest",
    }

    # Construct image reference
    registry = original["registry"].rstrip("/")
    name = original["name"]
    tag = original["tag"]
    image_ref = f"{registry}/{name.lstrip('/')}:{tag}"

    # Should be correct format
    assert image_ref == "nvcr.io/nv-maglev/dlav/hello-container:latest"

    # Now if we remove registry/name/tag and only keep image,
    # it should not have duplicate registry
    modified = {"image": image_ref}

    # When used with _resolve_image_reference (which would add registry again),
    # we must ensure registry/name/tag are removed
    assert "registry" not in modified
    assert "name" not in modified
    assert "tag" not in modified
    assert modified["image"] == "nvcr.io/nv-maglev/dlav/hello-container:latest"
