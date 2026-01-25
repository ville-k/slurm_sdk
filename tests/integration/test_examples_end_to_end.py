"""End-to-end integration tests for example scripts.

Most tests use wheel packaging for speed. Container packaging tests are marked
with @pytest.mark.container_build and kept to 1-2 smoke tests to validate
container integration without excessive build overhead.
"""

import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Optional

import pytest

pytestmark = pytest.mark.slow_integration_test

REPO_ROOT = Path(__file__).resolve().parents[2]


def _default_platform() -> str:
    import platform

    machine = platform.machine().lower()
    return "linux/arm64" if machine in {"arm64", "aarch64"} else "linux/amd64"


def _base_args(slurm_config: dict, packaging: str) -> List[str]:
    backend = slurm_config["cluster"]["backend_config"]
    return [
        "--hostname",
        backend["hostname"],
        "--port",
        str(backend["port"]),
        "--username",
        backend["username"],
        "--password",
        backend["password"],
        "--job-base-dir",
        slurm_config["cluster"]["job_base_dir"],
        "--partition",
        slurm_config["submit"]["partition"],
        "--packaging",
        packaging,
    ]


def _run_example(
    module: str,
    slurm_config: dict,
    *,
    packaging: str = "wheel",
    extra_args: Optional[Iterable[str]] = None,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    cli_args = _base_args(slurm_config, packaging)
    if extra_args:
        cli_args.extend(list(extra_args))

    code = (
        "import importlib, sys\n"
        f"sys.argv = ['{module}'] + {repr(cli_args)}\n"
        f"importlib.import_module('{module}').main()\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Example {module} failed with code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )
    return result


# ============================================================================
# Wheel Packaging Tests (fast, no container builds)
# ============================================================================


def test_hello_world_example_wheel(slurm_pyxis_cluster_config, sdk_on_pyxis_cluster):
    """Test hello_world example with wheel packaging."""
    result = _run_example(
        "slurm.examples.hello_world",
        slurm_pyxis_cluster_config,
        packaging="wheel",
    )
    assert "Result:" in result.stdout


def test_map_reduce_example(slurm_pyxis_cluster_config, sdk_on_pyxis_cluster):
    """Test map-reduce workflow with wheel packaging."""
    result = _run_example(
        "slurm.examples.map_reduce",
        slurm_pyxis_cluster_config,
        packaging="wheel",
        extra_args=["--num-chunks", "3"],
    )
    assert "Map-Reduce workflow completed successfully" in result.stdout


def test_parallelization_patterns_example(
    slurm_pyxis_cluster_config, sdk_on_pyxis_cluster
):
    """Test parallelization patterns with wheel packaging."""
    result = _run_example(
        "slurm.examples.parallelization_patterns",
        slurm_pyxis_cluster_config,
        packaging="wheel",
        extra_args=["--pattern", "all"],
        timeout=900,  # All patterns need more time
    )
    assert "Pattern 1: Fan-out/Fan-in" in result.stdout
    assert "Pattern 2: Pipeline with Parallel Stages" in result.stdout
    assert "Pattern 3: Hyperparameter Sweep" in result.stdout
    assert "Pattern 4: Dynamic Dependencies" in result.stdout


def test_workflow_graph_visualization_example(
    slurm_pyxis_cluster_config, sdk_on_pyxis_cluster
):
    """Test workflow graph visualization with wheel packaging."""
    result = _run_example(
        "slurm.examples.workflow_graph_visualization",
        slurm_pyxis_cluster_config,
        packaging="wheel",
        extra_args=["--timeout", "300"],
        timeout=600,
    )
    assert "Workflow completed!" in result.stdout


def test_parallel_train_eval_workflow_example(
    slurm_pyxis_cluster_config, sdk_on_pyxis_cluster
):
    """Test parallel train/eval workflow with wheel packaging."""
    result = _run_example(
        "slurm.examples.parallel_train_eval.workflow",
        slurm_pyxis_cluster_config,
        packaging="wheel",
        extra_args=[
            "--epochs",
            "2",
            "--epoch-steps",
            "5",
            "--steps-per-job-cap",
            "3",
        ],
        timeout=600,
    )
    output = result.stdout + result.stderr
    assert "Workflow complete. State file:" in output


# ============================================================================
# Container Packaging Smoke Tests (validates container integration)
# ============================================================================


@pytest.mark.container_build
def test_hello_container_example(
    slurm_pyxis_cluster_config, sdk_on_pyxis_cluster, local_registry
):
    """Smoke test: hello_container with container packaging.

    This test validates that container packaging works end-to-end.
    It builds an image from the example's Dockerfile and runs in Pyxis.
    """
    result = _run_example(
        "slurm.examples.hello_container",
        slurm_pyxis_cluster_config,
        packaging="container",
        extra_args=[
            "--packaging-registry",
            "registry:20002/hello-container",
            "--packaging-platform",
            _default_platform(),
            "--packaging",
            "container",
            "--packaging-tls-verify",
            "false",
        ],
    )
    assert "Result:" in result.stdout


@pytest.mark.container_build
def test_workflow_graph_visualization_container(
    slurm_pyxis_cluster_config, sdk_on_pyxis_cluster, local_registry
):
    """Smoke test: workflow graph visualization with container packaging.

    This test validates that complex workflows work with container packaging.
    """
    result = _run_example(
        "slurm.examples.workflow_graph_visualization",
        slurm_pyxis_cluster_config,
        packaging="container",
        extra_args=[
            "--packaging-registry",
            "registry:20002/workflow-graph",
            "--packaging-platform",
            _default_platform(),
            "--packaging",
            "container",
            "--packaging-tls-verify",
            "false",
        ],
        timeout=600,
    )
    assert "Workflow completed!" in result.stdout
