"""Map-Reduce example demonstrating array jobs with container packaging.

This example shows:
- Map-reduce pattern with array jobs
- Eager execution with dependencies: task.after(prep).map(items)
- Container packaging for reproducible execution
- Cluster-level packaging defaults to avoid repetition
- Processing data in parallel and aggregating results
"""

from __future__ import annotations

import argparse
import logging
from typing import List

from slurm.callbacks.callbacks import RichLoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.job import Job


@task(
    time="00:02:00",
    mem="256M",
    cpus_per_task=1,
)
def prepare_data(num_chunks: int) -> List[dict]:
    """Prepare data by creating chunks for parallel processing.

    Args:
        num_chunks: Number of chunks to create

    Returns:
        List of data chunks to process
    """
    chunks = []
    for i in range(num_chunks):
        # Create synthetic data chunks
        chunk = {
            "chunk_id": i,
            "data": list(range(i * 100, (i + 1) * 100)),
            "description": f"Chunk {i}",
        }
        chunks.append(chunk)

    print(f"Created {len(chunks)} data chunks")
    return chunks


@task(
    time="00:03:00",
    mem="256M",
    cpus_per_task=1,
)
def map_process_chunk(chunk_id: int, data: List[int], description: str) -> dict:
    """Process a single data chunk (MAP phase).

    Args:
        chunk_id: Unique identifier for this chunk
        data: List of integers to process
        description: Human-readable description of the chunk (metadata)

    Returns:
        Processed chunk with computed statistics
    """
    _ = description  # Metadata, not used in processing
    import socket
    import time

    hostname = socket.gethostname()
    start_time = time.time()

    # Simulate some processing
    result = {
        "chunk_id": chunk_id,
        "count": len(data),
        "sum": sum(data),
        "min": min(data),
        "max": max(data),
        "mean": sum(data) / len(data),
        "processed_on": hostname,
        "processing_time": time.time() - start_time,
    }

    print(f"Processed chunk {chunk_id} on {hostname}")
    return result


@task(
    time="00:05:00",
    mem="512M",
    cpus_per_task=1,
)
def reduce_aggregate_results(results: List[dict]) -> dict:
    """Aggregate results from all processed chunks (REDUCE phase).

    Args:
        results: List of processed chunk results

    Returns:
        Aggregated statistics across all chunks
    """
    import socket
    import time

    hostname = socket.gethostname()
    start_time = time.time()

    # Aggregate statistics
    total_count = sum(r["count"] for r in results)
    total_sum = sum(r["sum"] for r in results)
    global_min = min(r["min"] for r in results)
    global_max = max(r["max"] for r in results)
    global_mean = total_sum / total_count if total_count > 0 else 0

    # Compute processing stats
    total_processing_time = sum(r["processing_time"] for r in results)
    hosts_used = set(r["processed_on"] for r in results)

    aggregated = {
        "total_chunks": len(results),
        "total_count": total_count,
        "total_sum": total_sum,
        "global_min": global_min,
        "global_max": global_max,
        "global_mean": global_mean,
        "hosts_used": list(hosts_used),
        "num_hosts": len(hosts_used),
        "total_processing_time": total_processing_time,
        "reduced_on": hostname,
        "reduce_time": time.time() - start_time,
    }

    print(f"Reduced {len(results)} results on {hostname}")
    return aggregated


def main() -> None:
    """Entry point for the map-reduce example."""

    parser = argparse.ArgumentParser(
        description="Map-reduce example with array jobs and container packaging"
    )

    # Add standard cluster configuration arguments
    Cluster.add_argparse_args(parser)

    # Add example-specific arguments
    parser.add_argument(
        "--num-chunks",
        type=int,
        default=5,
        help="Number of data chunks to create and process (default: 5)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    cluster = Cluster.from_args(
        args,
        callbacks=[RichLoggerCallback()],
        default_packaging="container",
        default_packaging_dockerfile="src/slurm/examples/map_reduce.Dockerfile",
    )

    with cluster:
        print("\\n" + "=" * 60)
        print("Map-Reduce Example with Array Jobs")
        print("=" * 60 + "\\n")

        # PREPARE: Create data chunks
        print(f"[1/3] Preparing {args.num_chunks} data chunks...")
        prep_job: Job[List[dict]] = prepare_data(args.num_chunks)

        if not prep_job.wait():
            print("ERROR: Data preparation failed!")
            print(prep_job.get_stderr())
            return

        chunks = prep_job.get_result()
        print(f"✓ Created {len(chunks)} chunks\\n")

        # MAP: Process chunks in parallel using array job
        # NOTE: Dependencies are specified before .map() for proper ordering
        print(f"[2/3] Mapping: Processing {len(chunks)} chunks in parallel...")
        print("    Pattern: map_process_chunk.after(prep_job).map(chunks)")

        map_jobs = map_process_chunk.after(prep_job).map(chunks)

        print(f"    Submitted {len(map_jobs)} parallel jobs")
        print("    Waiting for all map jobs to complete...")

        if not map_jobs.wait():
            print("ERROR: Some map jobs failed!")
            return

        map_results = map_jobs.get_results()
        print(f"✓ Completed {len(map_results)} map jobs\\n")

        # REDUCE: Aggregate all results
        print("[3/3] Reducing: Aggregating results...")
        print("    Pattern: reduce_aggregate_results.after(*map_jobs)(map_results)")

        # Depend on all map jobs before reducing
        # Can pass map_jobs directly to .after() (it expands to all jobs)
        reduce_job: Job[dict] = reduce_aggregate_results.after(map_jobs)(map_results)

        print("    Waiting for reduce job to complete...")

        if not reduce_job.wait():
            print("ERROR: Reduce job failed!")
            print(reduce_job.get_stderr())
            return

        final_result = reduce_job.get_result()
        print("✓ Reduce complete\\n")

        # Display final results
        print("=" * 60)
        print("Final Results")
        print("=" * 60)
        print(f"  Total Chunks: {final_result['total_chunks']}")
        print(f"  Total Items:  {final_result['total_count']}")
        print(f"  Sum:          {final_result['total_sum']}")
        print(f"  Min:          {final_result['global_min']}")
        print(f"  Max:          {final_result['global_max']}")
        print(f"  Mean:         {final_result['global_mean']:.2f}")
        print(
            f"  Hosts Used:   {final_result['num_hosts']} ({', '.join(final_result['hosts_used'])})"
        )
        print(f"  Map Time:     {final_result['total_processing_time']:.3f}s")
        print(f"  Reduce Time:  {final_result['reduce_time']:.3f}s")
        print("=" * 60 + "\\n")

        print("✓ Map-Reduce workflow completed successfully!")


if __name__ == "__main__":
    main()
