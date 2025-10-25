"""GPU-enabled container example that executes ``nvidia-smi`` inside PyTorch.

This example shows:
- Using GPU resources with SLURM
- Using a GPU-enabled container image (PyTorch with CUDA)
- Running nvidia-smi inside the container
"""

from __future__ import annotations

import argparse
import subprocess

from rich.console import Console

from slurm.callbacks.callbacks import BenchmarkCallback, LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


@task(
    time="01:00:00",
    nodes=1,
    gpus_per_node=8,
    ntasks_per_node=8,
    exclusive=None,
    packaging="container:pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime",
)
def hello_cuda_task() -> str:
    """Run ``nvidia-smi`` inside the container and return its output."""

    result = subprocess.run(
        ["nvidia-smi"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip()
    print(output)
    return output


def main() -> None:
    """Entry point for the CUDA container packaging example."""

    console = Console()
    parser = argparse.ArgumentParser(
        description="Submit a GPU task using a CUDA-enabled container"
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    args = parser.parse_args()

    configure_logging()

    try:
        # Create cluster from args
        cluster = Cluster.from_args(
            args,
            callbacks=[LoggerCallback(console=console), BenchmarkCallback()],
        )

        # Submit job (uses PyTorch container from task decorator)
        job = hello_cuda_task.submit(cluster)()

        job.wait()
        result = job.get_result()
    except Exception as exc:  # pragma: no cover - example script
        console.print("[bold red]CUDA example failed:[/bold red]")
        console.print(exc)
        raise SystemExit(1) from exc

    console.print("[bold green]nvidia-smi output:[/bold green]")
    console.print(result)


if __name__ == "__main__":
    main()
