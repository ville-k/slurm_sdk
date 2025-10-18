"""Example demonstrating workflow orchestration with arrays and conditional logic.

This example showcases the full workflow capabilities including:
- @workflow decorator
- WorkflowContext parameter injection
- Array jobs via .map()
- Automatic dependency tracking
- Conditional logic based on results
- Container packaging
- Shared directory usage

The workflow implements a hyperparameter search pipeline:
1. Prepare shared dataset
2. Train multiple models in parallel (array job)
3. Evaluate all models in parallel (array job with dependencies)
4. Select best model based on results
5. Run final validation on best model
"""

from __future__ import annotations

import argparse
import pathlib
import time
from typing import Optional

from rich.console import Console
from rich.table import Table

from slurm.callbacks.callbacks import RichLoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task, workflow
from slurm.logging import configure_logging
from slurm.runtime import JobContext
from slurm.workflow import WorkflowContext


DEFAULT_SLURMFILE = pathlib.Path(__file__).with_name("Slurmfile.container_example.toml")


@task(time="00:05:00", mem="2G")
def prepare_dataset(dataset_name: str, ctx: Optional[JobContext] = None) -> dict:
    """Prepare the shared dataset for all training runs.

    Args:
        dataset_name: Name of the dataset to prepare
        ctx: Job context (automatically injected)

    Returns:
        dict: Dataset metadata
    """
    print(f"Preparing dataset: {dataset_name}")
    time.sleep(2)  # Simulate data preparation

    dataset = {
        "name": dataset_name,
        "samples": 10000,
        "features": 128,
        "splits": {"train": 8000, "val": 1000, "test": 1000},
        "prepared_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    if ctx and ctx.output_dir:
        # In a real workflow, data would be saved to ctx.shared_dir
        # which is accessible to all tasks in the workflow
        print(f"Dataset prepared in: {ctx.output_dir}")

    return dataset


@task(time="00:10:00", mem="4G")
def train_model(config: dict, dataset: dict, ctx: Optional[JobContext] = None) -> dict:
    """Train a model with given hyperparameters.

    This task will be called via .map() to train multiple models in parallel.

    Args:
        config: Hyperparameter configuration
        dataset: Dataset metadata from prepare_dataset
        ctx: Job context (automatically injected)

    Returns:
        dict: Training results including metrics
    """
    lr = config["learning_rate"]
    batch_size = config["batch_size"]

    print(f"Training with lr={lr}, batch_size={batch_size}")
    print(f"Dataset: {dataset['name']} ({dataset['samples']} samples)")

    # Simulate training
    time.sleep(3)

    # Simulate varying performance based on hyperparameters
    # In reality, this would be actual training metrics
    base_acc = 0.80
    lr_factor = min(0.1, lr) / 0.1  # Best around 0.1
    batch_factor = 1.0 - abs(batch_size - 64) / 128  # Best around 64

    val_accuracy = base_acc + (0.15 * lr_factor * batch_factor)

    results = {
        "config": config,
        "val_accuracy": val_accuracy,
        "val_loss": 1.0 - val_accuracy,
        "epochs_trained": 50,
        "dataset_name": dataset["name"],
    }

    if ctx and ctx.output_dir:
        print(f"Model checkpoint saved in: {ctx.output_dir}")

    return results


@task(time="00:05:00", mem="2G")
def evaluate_model(
    training_result: dict,
    test_split: str,
    ctx: Optional[JobContext] = None,
) -> dict:
    """Evaluate a trained model on test data.

    This task will be called via .map() to evaluate multiple models in parallel.
    Each evaluation automatically depends on its corresponding training job.

    Args:
        training_result: Results from train_model
        test_split: Which test split to use
        ctx: Job context (automatically injected)

    Returns:
        dict: Evaluation results
    """
    config = training_result["config"]
    val_acc = training_result["val_accuracy"]

    print(f"Evaluating model (val_acc={val_acc:.3f}) on {test_split}")
    time.sleep(2)  # Simulate evaluation

    # Test accuracy is typically slightly lower than validation
    test_accuracy = val_acc - 0.02

    results = {
        "config": config,
        "val_accuracy": val_acc,
        "test_accuracy": test_accuracy,
        "test_split": test_split,
        "passed": test_accuracy > 0.85,  # Quality threshold
    }

    if ctx and ctx.output_dir:
        print(f"Evaluation results saved in: {ctx.output_dir}")

    return results


@task(time="00:10:00", mem="4G")
def final_validation(
    best_config: dict,
    validation_type: str,
    ctx: Optional[JobContext] = None,
) -> dict:
    """Run comprehensive final validation on the best model.

    Args:
        best_config: Configuration of the best model
        validation_type: Type of validation to perform
        ctx: Job context (automatically injected)

    Returns:
        dict: Final validation results
    """
    print(f"Running {validation_type} validation on best model")
    print(
        f"Best config: lr={best_config['learning_rate']}, "
        f"batch_size={best_config['batch_size']}"
    )

    time.sleep(4)  # Simulate comprehensive validation

    results = {
        "validation_type": validation_type,
        "best_config": best_config,
        "final_accuracy": 0.94,
        "robustness_score": 0.91,
        "inference_speed_ms": 12.5,
        "passed_all_checks": True,
    }

    if ctx and ctx.output_dir:
        print(f"Final validation report saved in: {ctx.output_dir}")

    return results


@workflow(time="01:00:00", job_name="ml_hyperparameter_search")
def hyperparameter_search_workflow(
    dataset_name: str,
    configs: list[dict],
    ctx: WorkflowContext,
) -> dict:
    """Workflow orchestrating a complete hyperparameter search pipeline.

    This workflow demonstrates:
    - Sequential and parallel execution
    - Array jobs with .map()
    - Automatic dependency tracking
    - Conditional logic based on results
    - Shared directory usage via ctx.shared_dir

    Args:
        dataset_name: Name of dataset to use
        configs: List of hyperparameter configurations to try
        ctx: Workflow context (automatically injected)

    Returns:
        dict: Final results including best model
    """
    print(f"\n{'=' * 60}")
    print(f"Starting Hyperparameter Search Workflow")
    print(f"Dataset: {dataset_name}")
    print(f"Configurations to test: {len(configs)}")
    print(f"{'=' * 60}\n")

    # Stage 1: Prepare shared dataset
    print("[Stage 1] Preparing shared dataset...")
    dataset_job = prepare_dataset(dataset_name)

    # Wait for dataset preparation before starting training
    # (In reality, you might want to continue with other setup)
    dataset = dataset_job.get_result()
    print(f"✓ Dataset ready: {dataset['samples']} samples\n")

    # Stage 2: Train multiple models in parallel using array jobs
    print(f"[Stage 2] Training {len(configs)} models in parallel...")

    # Create list of (config, dataset) tuples for .map()
    training_inputs = [(config, dataset) for config in configs]

    # .map() creates an array job - all models train in parallel
    # Each training job automatically depends on dataset_job
    train_jobs = train_model.map(training_inputs)

    print(f"✓ Submitted {len(train_jobs)} training jobs\n")

    # Stage 3: Evaluate all models in parallel
    print("[Stage 3] Evaluating all trained models...")

    # Create evaluation inputs with automatic dependencies
    # Each eval job depends on its corresponding train job
    eval_inputs = [(train_job, "test") for train_job in train_jobs]

    eval_jobs = evaluate_model.map(eval_inputs)

    print(f"✓ Submitted {len(eval_jobs)} evaluation jobs\n")

    # Stage 4: Collect results and select best model
    print("[Stage 4] Collecting results and selecting best model...")

    # Wait for all evaluations to complete
    eval_results = eval_jobs.get_results()

    # Find the best performing model
    best_idx = max(
        range(len(eval_results)), key=lambda i: eval_results[i]["test_accuracy"]
    )
    best_result = eval_results[best_idx]
    best_config = best_result["config"]

    print(
        f"✓ Best model found: lr={best_config['learning_rate']}, "
        f"batch_size={best_config['batch_size']}"
    )
    print(f"  Test accuracy: {best_result['test_accuracy']:.3f}\n")

    # Conditional logic: Only run final validation if best model is good enough
    if best_result["test_accuracy"] > 0.85:
        print("[Stage 5] Running final validation on best model...")

        # Run comprehensive validation on the best model
        final_val_job = final_validation(best_config, "comprehensive")
        final_results = final_val_job.get_result()

        print(f"✓ Final validation complete!")
        print(f"  Final accuracy: {final_results['final_accuracy']:.3f}")
        print(f"  Robustness: {final_results['robustness_score']:.3f}\n")
    else:
        print("[Stage 5] Skipping final validation - no model met threshold\n")
        final_results = None

    # Stage 6: Compile final report
    print("[Stage 6] Compiling final report...")

    # In a real workflow, you might save data to ctx.shared_dir
    # shared_report_path = ctx.shared_dir / "final_report.json"
    # save_json(shared_report_path, report)

    report = {
        "dataset": dataset_name,
        "configs_tested": len(configs),
        "best_config": best_config,
        "best_result": best_result,
        "all_results": eval_results,
        "final_validation": final_results,
        "workflow_dir": str(ctx.workflow_job_dir),
    }

    print(f"✓ Workflow complete!\n")
    print(f"{'=' * 60}\n")

    return report


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the example script."""
    parser = argparse.ArgumentParser(
        description="Run ML workflow demonstrating arrays, map, and conditional logic."
    )
    parser.add_argument(
        "--slurmfile",
        default=str(DEFAULT_SLURMFILE),
        help="Path to the Slurmfile to load",
    )
    parser.add_argument(
        "--env",
        default="default",
        help="Environment key within the Slurmfile",
    )
    parser.add_argument(
        "--dataset",
        default="mnist",
        help="Name of the dataset to use",
    )
    parser.add_argument(
        "--num-configs",
        type=int,
        default=4,
        help="Number of hyperparameter configurations to test",
    )
    return parser


def main() -> None:
    """Entry point demonstrating the workflow orchestration."""
    console = Console()
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    console.print("[bold blue]ML Hyperparameter Search Workflow[/bold blue]\n")

    # Generate hyperparameter configurations to test
    learning_rates = [0.001, 0.01, 0.1]
    batch_sizes = [32, 64, 128]

    configs = []
    for i, (lr, bs) in enumerate(zip(learning_rates, batch_sizes)):
        if i >= args.num_configs:
            break
        configs.append({"learning_rate": lr, "batch_size": bs})

    # Add one more if num_configs > 3
    if args.num_configs > len(configs):
        configs.append({"learning_rate": 0.05, "batch_size": 64})

    console.print(f"Dataset: {args.dataset}")
    console.print(f"Configurations to test: {len(configs)}\n")

    # Display configurations in a table
    table = Table(title="Hyperparameter Configurations")
    table.add_column("Config #", style="cyan")
    table.add_column("Learning Rate", style="magenta")
    table.add_column("Batch Size", style="green")

    for i, config in enumerate(configs):
        table.add_row(
            str(i + 1),
            f"{config['learning_rate']:.3f}",
            str(config["batch_size"]),
        )

    console.print(table)
    console.print()

    # Execute workflow with cluster context
    with Cluster.from_env(
        args.slurmfile,
        env=args.env,
        callbacks=[RichLoggerCallback(console=console)],
    ) as cluster:
        # Call workflow - returns a Job (Future)
        workflow_job = hyperparameter_search_workflow(
            dataset_name=args.dataset,
            configs=configs,
        )

        console.print(
            "[yellow]Workflow submitted! Waiting for completion...[/yellow]\n"
        )

        # Get result triggers execution and waits
        report = workflow_job.get_result()

        # Display results
        console.print("\n[bold green]Workflow Complete![/bold green]\n")

        best = report["best_result"]
        console.print("[bold]Best Model:[/bold]")
        console.print(f"  Learning Rate: {best['config']['learning_rate']}")
        console.print(f"  Batch Size: {best['config']['batch_size']}")
        console.print(f"  Validation Accuracy: {best['val_accuracy']:.3f}")
        console.print(f"  Test Accuracy: {best['test_accuracy']:.3f}")

        if report["final_validation"]:
            final = report["final_validation"]
            console.print(f"\n[bold]Final Validation:[/bold]")
            console.print(f"  Final Accuracy: {final['final_accuracy']:.3f}")
            console.print(f"  Robustness Score: {final['robustness_score']:.3f}")
            console.print(f"  Inference Speed: {final['inference_speed_ms']:.1f}ms")
            console.print(
                f"  Status: {'PASSED' if final['passed_all_checks'] else 'FAILED'}"
            )

        # Show all results in a table
        console.print("\n[bold]All Configurations:[/bold]")
        results_table = Table()
        results_table.add_column("Config", style="cyan")
        results_table.add_column("LR", style="magenta")
        results_table.add_column("BS", style="green")
        results_table.add_column("Val Acc", style="blue")
        results_table.add_column("Test Acc", style="yellow")
        results_table.add_column("Passed", style="green")

        for i, result in enumerate(report["all_results"]):
            config = result["config"]
            passed = "✓" if result["passed"] else "✗"
            results_table.add_row(
                str(i + 1),
                f"{config['learning_rate']:.3f}",
                str(config["batch_size"]),
                f"{result['val_accuracy']:.3f}",
                f"{result['test_accuracy']:.3f}",
                passed,
            )

        console.print(results_table)
        console.print(f"\n[dim]Workflow directory: {report['workflow_dir']}[/dim]")


if __name__ == "__main__":
    main()
