"""GPU-enabled container example that executes ``nvidia-smi`` inside PyTorch."""

from __future__ import annotations

import argparse
import pathlib
import subprocess

from rich.console import Console

from slurm.callbacks.callbacks import BenchmarkCallback, LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging


DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.cuda_example.toml")


@task(
    name="hello_cuda",
    time="01:00:00",
    nodes=1,
    gpus_per_node=8,
    ntasks_per_node=8,
    exclusive=None,
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


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the CUDA example."""

    parser = argparse.ArgumentParser(
        description=(
            "Submit the hello_cuda task using a Slurmfile configured for a "
            "GPU-capable container image."
        )
    )
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load (defaults to the bundled example).",
    )
    parser.add_argument(
        "--env",
        default="default",
        help="Environment key within the Slurmfile to load (defaults to 'default').",
    )
    return parser


def main() -> None:
    """Entry point for the CUDA container packaging example."""

    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    try:
        cluster = Cluster.from_env(
            args.slurmfile,
            env=args.env,
            callbacks=[LoggerCallback(console=console), BenchmarkCallback()],
        )

        env_submit = cluster.environment_config["submit"]

        job = hello_cuda_task.submit(
            cluster=cluster,
            packaging=cluster.packaging_defaults,
            account=env_submit.get("account"),
            partition=env_submit.get("partition"),
        )()

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
