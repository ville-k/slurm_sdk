"""Example demonstrating cluster context with automatic job dependencies.

This example shows how to use the submitless execution API with the cluster
context manager. Tasks return Jobs (Futures) when called, and dependencies
are automatically tracked when Jobs are passed as arguments.

The pipeline demonstrates:
- Context manager usage (with Cluster.from_env())
- Automatic dependency detection (passing Job as argument)
- Explicit dependencies (.after())
- Container packaging
- Result retrieval
"""

from __future__ import annotations

import argparse
import pathlib
import time
from typing import Optional

from rich.console import Console

from slurm.callbacks.callbacks import RichLoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.logging import configure_logging
from slurm.runtime import JobContext


DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.container_example.toml")


@task(time="00:05:00", mem="1G")
def generate_data(size: int, ctx: Optional[JobContext] = None) -> dict:
    """Generate synthetic dataset.

    Args:
        size: Number of data points to generate
        ctx: Job context (automatically injected)

    Returns:
        dict: Dataset metadata
    """
    print(f"Generating {size} data points...")
    time.sleep(2)  # Simulate work

    dataset = {
        "size": size,
        "checksum": "abc123",
        "format": "csv",
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    if ctx and ctx.output_dir:
        print(f"Dataset stored in: {ctx.output_dir}")

    return dataset


@task(time="00:05:00", mem="2G")
def preprocess_data(dataset: dict, ctx: Optional[JobContext] = None) -> dict:
    """Preprocess the dataset.

    This task automatically depends on generate_data when the Job is passed.

    Args:
        dataset: Dataset metadata from generate_data
        ctx: Job context (automatically injected)

    Returns:
        dict: Preprocessed dataset metadata
    """
    print(f"Preprocessing dataset with {dataset['size']} points...")
    time.sleep(2)  # Simulate work

    processed = {
        **dataset,
        "preprocessed": True,
        "features": ["feature_1", "feature_2", "feature_3"],
        "normalized": True,
    }

    if ctx and ctx.output_dir:
        print(f"Processed data stored in: {ctx.output_dir}")

    return processed


@task(time="00:05:00", mem="4G")
def train_model(
    data: dict, learning_rate: float, ctx: Optional[JobContext] = None
) -> dict:
    """Train a model on the preprocessed data.

    This task automatically depends on preprocess_data when the Job is passed.

    Args:
        data: Preprocessed dataset metadata
        learning_rate: Learning rate for training
        ctx: Job context (automatically injected)

    Returns:
        dict: Training results
    """
    print(f"Training model with lr={learning_rate} on {data['size']} points...")
    time.sleep(3)  # Simulate training

    model_info = {
        "learning_rate": learning_rate,
        "accuracy": 0.95,
        "loss": 0.12,
        "epochs": 10,
        "features_used": data.get("features", []),
    }

    if ctx and ctx.output_dir:
        print(f"Model saved in: {ctx.output_dir}")

    return model_info


@task(time="00:05:00", mem="2G")
def evaluate_model(
    model: dict, test_size: int, ctx: Optional[JobContext] = None
) -> dict:
    """Evaluate the trained model.

    This task automatically depends on train_model when the Job is passed.

    Args:
        model: Model metadata from train_model
        test_size: Number of test samples
        ctx: Job context (automatically injected)

    Returns:
        dict: Evaluation results
    """
    print(f"Evaluating model on {test_size} test samples...")
    time.sleep(2)  # Simulate evaluation

    results = {
        "test_accuracy": 0.93,
        "test_loss": 0.15,
        "test_samples": test_size,
        "model_lr": model["learning_rate"],
        "passed": True,
    }

    if ctx and ctx.output_dir:
        print(f"Evaluation results saved in: {ctx.output_dir}")

    return results


@task(time="00:05:00", mem="1G")
def generate_report(
    model: dict,
    evaluation: dict,
    ctx: Optional[JobContext] = None,
) -> str:
    """Generate final report combining model and evaluation results.

    This task demonstrates explicit dependencies with .after() - it needs
    both model and evaluation to be complete, but doesn't need their data
    as direct arguments in the same way.

    Args:
        model: Model metadata
        evaluation: Evaluation results
        ctx: Job context (automatically injected)

    Returns:
        str: Report summary
    """
    print("Generating final report...")
    time.sleep(1)

    report = f"""
    ML Pipeline Report
    ==================
    Model Learning Rate: {model["learning_rate"]}
    Training Accuracy: {model["accuracy"]:.2%}
    Test Accuracy: {evaluation["test_accuracy"]:.2%}
    Status: {"PASSED" if evaluation["passed"] else "FAILED"}
    """

    print(report)
    return report.strip()


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the example script."""
    parser = argparse.ArgumentParser(
        description="Run dependent jobs pipeline demonstrating submitless execution."
    )
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load",
    )
    parser.add_argument(
        "--env",
        default="local",
        help="Environment key within the Slurmfile",
    )
    parser.add_argument(
        "--dataset-size",
        type=int,
        default=1000,
        help="Size of the dataset to generate",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.001,
        help="Learning rate for training",
    )
    return parser


def main() -> None:
    """Entry point demonstrating the submitless execution pipeline."""
    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    console.print("[bold blue]Starting Dependent Jobs Pipeline[/bold blue]")
    console.print(f"Dataset size: {args.dataset_size}")
    console.print(f"Learning rate: {args.learning_rate}\n")

    # Use cluster context manager - enables submitless execution
    with Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[RichLoggerCallback(console=console)],
    ) as cluster:
        console.print("[yellow]Stage 1: Generate Data[/yellow]")
        # Calling the task returns a Job (Future)
        data_job = generate_data(args.dataset_size)

        console.print("[yellow]Stage 2: Preprocess Data[/yellow]")
        # Pass the Job as argument - automatic dependency!
        preprocess_job = preprocess_data(data_job)

        console.print("[yellow]Stage 3: Train Model[/yellow]")
        # Another automatic dependency chain
        model_job = train_model(preprocess_job, args.learning_rate)

        console.print("[yellow]Stage 4: Evaluate Model[/yellow]")
        # Evaluate depends on the model job
        eval_job = evaluate_model(model_job, test_size=200)

        console.print("[yellow]Stage 5: Generate Report[/yellow]")
        # Report needs both model and evaluation to be complete
        # Automatic dependencies tracked through Job arguments
        report_job = generate_report(model_job, eval_job)

        console.print(
            "\n[bold green]All jobs submitted! Waiting for completion...[/bold green]"
        )

        # Get result triggers submission and waits for completion
        report = report_job.get_result()

        console.print("\n[bold green]Pipeline Complete![/bold green]")
        console.print(f"\n{report}\n")

        # Optionally retrieve intermediate results
        console.print("[dim]Intermediate results:[/dim]")
        console.print(f"- Dataset: {data_job.get_result()}")
        console.print(f"- Preprocessed: {preprocess_job.get_result()}")
        console.print(f"- Model: {model_job.get_result()}")
        console.print(f"- Evaluation: {eval_job.get_result()}")


if __name__ == "__main__":
    main()
