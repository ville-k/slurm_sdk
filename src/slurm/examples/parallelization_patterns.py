"""Parallelization Patterns with SLURM SDK.

This example demonstrates common parallel processing patterns using array jobs
and the fluent dependency API:

1. Fan-out/Fan-in: Split data, process in parallel, merge results
2. Pipeline: Sequential stages with parallel tasks within each stage
3. Dynamic dependencies: Jobs as array items with automatic dependency tracking
4. Map-Reduce: Parallel mapping with aggregation
5. Parameter sweeps: Exploring hyperparameter combinations

Each pattern uses container packaging for reproducible execution.
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Dict, Any

from slurm.callbacks.callbacks import RichLoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task


# ============================================================================
# Pattern 1: Fan-out/Fan-in
# ============================================================================


@task(
    time="00:03:00",
    mem="256M",
)
def split_dataset(dataset_path: str, num_chunks: int) -> List[Dict[str, Any]]:
    """Split a dataset into chunks for parallel processing.

    Args:
        dataset_path: Path to the dataset
        num_chunks: Number of chunks to create

    Returns:
        List of chunk metadata dicts
    """
    print(f"Splitting {dataset_path} into {num_chunks} chunks")

    # Simulate splitting by creating chunk metadata
    chunks = []
    for i in range(num_chunks):
        chunk = {
            "chunk_id": i,
            "start_idx": i * 100,
            "end_idx": (i + 1) * 100,
            "source": dataset_path,
        }
        chunks.append(chunk)

    print(f"Created {len(chunks)} chunks")
    return chunks


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def process_chunk(
    chunk_id: int, start_idx: int, end_idx: int, source: str
) -> Dict[str, Any]:
    """Process a single data chunk.

    Args:
        chunk_id: Unique chunk identifier
        start_idx: Start index in dataset
        end_idx: End index in dataset
        source: Source dataset path

    Returns:
        Processing results for this chunk
    """
    import time
    import socket

    hostname = socket.gethostname()
    start_time = time.time()

    # Simulate processing
    time.sleep(0.5)
    num_items = end_idx - start_idx

    result = {
        "chunk_id": chunk_id,
        "items_processed": num_items,
        "sum": sum(range(start_idx, end_idx)),
        "processed_on": hostname,
        "processing_time": time.time() - start_time,
    }

    print(f"Chunk {chunk_id} processed on {hostname}")
    return result


@task(
    time="00:03:00",
    mem="256M",
)
def merge_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge results from all chunks.

    Args:
        results: List of chunk processing results

    Returns:
        Aggregated final results
    """
    import socket

    hostname = socket.gethostname()

    total_items = sum(r["items_processed"] for r in results)
    total_sum = sum(r["sum"] for r in results)
    hosts_used = set(r["processed_on"] for r in results)
    total_time = sum(r["processing_time"] for r in results)

    merged = {
        "total_chunks": len(results),
        "total_items": total_items,
        "total_sum": total_sum,
        "hosts_used": list(hosts_used),
        "parallel_speedup": total_time / max(r["processing_time"] for r in results),
        "merged_on": hostname,
    }

    print(f"Merged {len(results)} chunks on {hostname}")
    print(f"Used {len(hosts_used)} hosts, speedup: {merged['parallel_speedup']:.2f}x")
    return merged


# ============================================================================
# Pattern 2: Pipeline with Parallel Stages
# ============================================================================


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def stage1_preprocess(raw_data: str) -> str:
    """Stage 1: Preprocess raw data.

    Args:
        raw_data: Raw input data

    Returns:
        Preprocessed data
    """
    print(f"Stage 1: Preprocessing {raw_data}")
    return f"preprocessed_{raw_data}"


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def stage2_transform(data: str, transform_type: str) -> str:
    """Stage 2: Apply transformation.

    Args:
        data: Input data from stage 1
        transform_type: Type of transformation

    Returns:
        Transformed data
    """
    print(f"Stage 2: Applying {transform_type} to {data}")
    return f"{transform_type}({data})"


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def stage3_finalize(transformed: str) -> str:
    """Stage 3: Finalize processing.

    Args:
        transformed: Transformed data from stage 2

    Returns:
        Final output
    """
    print(f"Stage 3: Finalizing {transformed}")
    return f"final_{transformed}"


# ============================================================================
# Pattern 3: Parameter Sweep
# ============================================================================


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def train_model(lr: float, batch_size: int, epochs: int, seed: int) -> Dict[str, Any]:
    """Train a model with specific hyperparameters.

    Args:
        lr: Learning rate
        batch_size: Batch size
        epochs: Number of epochs
        seed: Random seed

    Returns:
        Training results
    """
    import time
    import socket
    import random

    hostname = socket.gethostname()
    random.seed(seed)

    # Simulate training
    time.sleep(0.5)
    accuracy = 0.5 + (lr * 10) * random.random()

    result = {
        "lr": lr,
        "batch_size": batch_size,
        "epochs": epochs,
        "seed": seed,
        "accuracy": accuracy,
        "trained_on": hostname,
    }

    print(f"Trained on {hostname}: lr={lr}, bs={batch_size}, acc={accuracy:.3f}")
    return result


@task(
    time="00:03:00",
    mem="256M",
)
def select_best_model(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Select the best model from hyperparameter sweep.

    Args:
        results: List of training results

    Returns:
        Best model configuration and metrics
    """
    best = max(results, key=lambda r: r["accuracy"])
    print(
        f"Best model: lr={best['lr']}, bs={best['batch_size']}, acc={best['accuracy']:.3f}"
    )
    return best


# ============================================================================
# Workflows Demonstrating Patterns
# ============================================================================


def pattern_fan_out_fan_in(cluster: Cluster) -> None:
    """Pattern 1: Fan-out/Fan-in.

    Split work, process in parallel, merge results.

    Workflow:
        split_dataset --> [process_chunk (array)] --> merge_results
    """
    print("\n" + "=" * 70)
    print("Pattern 1: Fan-out/Fan-in")
    print("=" * 70 + "\n")

    # Step 1: Split dataset into chunks
    print("[1/3] Splitting dataset...")
    split_job = split_dataset("large_dataset.csv", num_chunks=5)
    split_job.wait()
    chunks = split_job.get_result()
    print(f"✓ Created {len(chunks)} chunks\n")

    # Step 2: Process chunks in parallel (fan-out)
    print("[2/3] Processing chunks in parallel (fan-out)...")
    # Use .after() for dependency, chunks already available locally
    process_jobs = process_chunk.after(split_job).map(chunks)
    print(f"    Submitted {len(process_jobs)} parallel jobs")
    process_jobs.wait()
    chunk_results = process_jobs.get_results()
    print(f"✓ Processed {len(chunk_results)} chunks\n")

    # Step 3: Merge results (fan-in)
    print("[3/3] Merging results (fan-in)...")
    merge_job = merge_results.after(process_jobs)(chunk_results)
    merge_job.wait()
    final = merge_job.get_result()
    print("✓ Merge complete\n")

    print("=" * 70)
    print("Final Results:")
    print(f"  Total items: {final['total_items']}")
    print(f"  Total sum:   {final['total_sum']}")
    print(f"  Hosts used:  {len(final['hosts_used'])}")
    print(f"  Speedup:     {final['parallel_speedup']:.2f}x")
    print("=" * 70)


def pattern_pipeline(cluster: Cluster) -> None:
    """Pattern 2: Pipeline with Parallel Stages.

    Sequential stages where each stage processes multiple items in parallel.

    Workflow:
        [stage1 (array)] --> [stage2 (array)] --> [stage3 (array)]
    """
    print("\n" + "=" * 70)
    print("Pattern 2: Pipeline with Parallel Stages")
    print("=" * 70 + "\n")

    raw_data = ["data_1", "data_2", "data_3"]

    # Stage 1: Preprocess in parallel
    print("[Stage 1/3] Preprocessing...")
    stage1_jobs = stage1_preprocess.map(raw_data)
    stage1_jobs.wait()
    preprocessed = stage1_jobs.get_results()
    print(f"✓ Preprocessed {len(preprocessed)} items\n")

    # Stage 2: Transform in parallel (depends on stage 1)
    print("[Stage 2/3] Transforming...")
    # Jobs as array items - automatic dependency tracking!
    transform_configs = [
        {"data": job, "transform_type": f"transform_{i}"}
        for i, job in enumerate(stage1_jobs)
    ]
    stage2_jobs = stage2_transform.map(transform_configs)
    stage2_jobs.wait()
    transformed = stage2_jobs.get_results()
    print(f"✓ Transformed {len(transformed)} items\n")

    # Stage 3: Finalize in parallel (depends on stage 2)
    print("[Stage 3/3] Finalizing...")
    # Using Jobs directly as items (new feature!)
    stage3_jobs = stage3_finalize.map(stage2_jobs)
    stage3_jobs.wait()
    final = stage3_jobs.get_results()
    print(f"✓ Finalized {len(final)} items\n")

    print("=" * 70)
    print("Pipeline Results:")
    for i, result in enumerate(final):
        print(f"  Item {i}: {result}")
    print("=" * 70)


def pattern_parameter_sweep(cluster: Cluster) -> None:
    """Pattern 3: Hyperparameter Sweep.

    Explore parameter combinations in parallel, select best.

    Workflow:
        [train_model (array)] --> select_best_model
    """
    print("\n" + "=" * 70)
    print("Pattern 3: Hyperparameter Sweep")
    print("=" * 70 + "\n")

    # Generate hyperparameter grid
    learning_rates = [0.001, 0.01, 0.1]
    batch_sizes = [32, 64]
    epochs = 10
    seeds = [0, 1]

    configs = [
        {"lr": lr, "batch_size": bs, "epochs": epochs, "seed": seed}
        for lr in learning_rates
        for bs in batch_sizes
        for seed in seeds
    ]

    print(f"[1/2] Training {len(configs)} model configurations...")
    print(f"    Learning rates: {learning_rates}")
    print(f"    Batch sizes: {batch_sizes}")
    print(f"    Seeds: {seeds}")

    # Train all configurations in parallel
    train_jobs = train_model.map(configs)
    train_jobs.wait()
    results = train_jobs.get_results()
    print(f"✓ Trained {len(results)} models\n")

    # Select best model
    print("[2/2] Selecting best model...")
    best_job = select_best_model.after(train_jobs)(results)
    best_job.wait()
    best = best_job.get_result()
    print("✓ Best model selected\n")

    print("=" * 70)
    print("Best Model Configuration:")
    print(f"  Learning rate: {best['lr']}")
    print(f"  Batch size:    {best['batch_size']}")
    print(f"  Accuracy:      {best['accuracy']:.3f}")
    print(f"  Trained on:    {best['trained_on']}")
    print("=" * 70)


def pattern_dynamic_dependencies(cluster: Cluster) -> None:
    """Pattern 4: Dynamic Dependencies with Job Arrays.

    Use Job objects as array items for automatic dependency tracking.
    This pattern is useful when you need to process the outputs of
    previous jobs in parallel.

    Workflow:
        [preprocess (array)] --> [transform (map over Jobs)]
    """
    print("\n" + "=" * 70)
    print("Pattern 4: Dynamic Dependencies (Jobs as Array Items)")
    print("=" * 70 + "\n")

    datasets = ["data_a.csv", "data_b.csv", "data_c.csv"]

    # Step 1: Preprocess datasets in parallel
    print("[1/2] Preprocessing datasets...")
    prep_jobs = stage1_preprocess.map(datasets)
    print(f"    Submitted {len(prep_jobs)} preprocessing jobs")
    prep_jobs.wait()
    print("✓ Preprocessing complete\n")

    # Step 2: Transform using Job objects directly
    print("[2/2] Transforming (mapping over Job objects)...")
    print("    Each transform depends on its corresponding prep job")

    # Create items with Jobs - they'll be automatically converted to placeholders
    transform_items = [
        (prep_job, f"transform_{i}") for i, prep_job in enumerate(prep_jobs)
    ]

    # Map over items containing Jobs
    # Jobs are automatically:
    #   1. Converted to JobResultPlaceholder
    #   2. Added as dependencies
    #   3. Resolved to actual results at runtime
    transform_jobs = stage2_transform.map(transform_items)

    # Verify dependencies were extracted
    print(f"    Dependencies: {len(transform_jobs.dependencies)} jobs")
    print(f"    Array size: {len(transform_jobs)} elements")

    transform_jobs.wait()
    results = transform_jobs.get_results()
    print("✓ Transform complete\n")

    print("=" * 70)
    print("Results:")
    for i, result in enumerate(results):
        print(f"  {i}: {result}")
    print("\nKey Feature: Jobs in items automatically tracked as dependencies!")
    print("=" * 70)


# ============================================================================
# Main Entry Point
# ============================================================================


def main() -> None:
    """Run parallelization pattern demonstrations."""
    parser = argparse.ArgumentParser(
        description="Demonstrate parallelization patterns with SLURM SDK"
    )

    # Cluster configuration
    Cluster.add_argparse_args(parser)

    # Pattern selection
    parser.add_argument(
        "--pattern",
        type=str,
        choices=[
            "fan-out-fan-in",
            "pipeline",
            "parameter-sweep",
            "dynamic-deps",
            "all",
        ],
        default="all",
        help="Which pattern to demonstrate (default: all)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    cluster = Cluster.from_args(
        args,
        callbacks=[RichLoggerCallback()],
        default_packaging="container",
        default_packaging_dockerfile="src/slurm/examples/map_reduce.Dockerfile",
    )

    # Pattern dispatch
    patterns = {
        "fan-out-fan-in": pattern_fan_out_fan_in,
        "pipeline": pattern_pipeline,
        "parameter-sweep": pattern_parameter_sweep,
        "dynamic-deps": pattern_dynamic_dependencies,
    }

    with cluster:
        if args.pattern == "all":
            print("\n" + "=" * 70)
            print("Running All Parallelization Patterns")
            print("=" * 70)

            for _, pattern_func in patterns.items():
                pattern_func(cluster)
                print("\n")
        else:
            patterns[args.pattern](cluster)

        print("\n✓ All patterns completed successfully!")


if __name__ == "__main__":
    main()
