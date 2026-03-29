# How to chain containerized tasks with dependencies

## Problem

You need to run a multi-phase pipeline (e.g., prepare, map, reduce) where all
tasks run in the same container and each phase depends on the previous one
completing successfully.

## Prerequisites

- A Slurm cluster with Pyxis/enroot installed
- A container registry accessible from compute nodes
- `slurm-sdk` installed locally

## Steps

### 1. Define a shared container image

Create a single Dockerfile for all tasks in the pipeline:

```dockerfile
FROM python:3.11-slim

WORKDIR /workspace

COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/

RUN pip install --no-cache-dir .
```

Set this as the cluster default so all tasks share it:

```python
from slurm import Cluster

cluster = Cluster.from_args(
    args,
    default_packaging="container",
    default_packaging_dockerfile="path/to/pipeline.Dockerfile",
)
```

### 2. Define the pipeline tasks

Define each phase as a separate task with the `@task` decorator:

```python
from slurm.decorators import task
from typing import List


@task(time="00:02:00", mem="256M", cpus_per_task=1)
def prepare_data(num_chunks: int) -> List[dict]:
    """Create data chunks for parallel processing."""
    return [
        {"chunk_id": i, "data": list(range(i * 100, (i + 1) * 100))}
        for i in range(num_chunks)
    ]


@task(time="00:03:00", mem="256M", cpus_per_task=1)
def process_chunk(chunk_id: int, data: List[int]) -> dict:
    """Process a single data chunk (map phase)."""
    return {
        "chunk_id": chunk_id,
        "count": len(data),
        "sum": sum(data),
    }


@task(time="00:05:00", mem="512M", cpus_per_task=1)
def aggregate_results(results: List[dict]) -> dict:
    """Combine results from all chunks (reduce phase)."""
    return {
        "total_chunks": len(results),
        "total_count": sum(r["count"] for r in results),
        "total_sum": sum(r["sum"] for r in results),
    }
```

### 3. Chain the tasks with dependencies

Use `.after()` for sequential dependencies and `.map()` for the parallel phase:

```python
from slurm import Job
from typing import List

with cluster:
    # Phase 1: Prepare data
    prep_job: Job[List[dict]] = prepare_data(num_chunks=5)
    prep_job.wait()
    chunks = prep_job.get_result()

    # Phase 2: Process chunks in parallel (array job)
    # .after(prep_job) ensures map tasks wait for preparation
    # .map(chunks) submits one task per chunk
    map_jobs = process_chunk.after(prep_job).map(chunks)
    map_jobs.wait()
    map_results = map_jobs.get_results()

    # Phase 3: Aggregate all results
    # .after(map_jobs) waits for ALL map tasks to complete
    reduce_job: Job[dict] = aggregate_results.after(map_jobs)(map_results)
    reduce_job.wait()
    final = reduce_job.get_result()
```

### 4. Run the built-in example

The SDK includes a complete map-reduce example:

```bash
uv run python -m slurm.examples.map_reduce \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --num-chunks 5 \
  --packaging container \
  --packaging-registry registry:5000/map-reduce \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```

Use `--num-chunks` to control the parallelism level.

## Verification

- All three phases should complete successfully in sequence.
- The map phase should show tasks distributed across available nodes.
- The final result should contain aggregated statistics:

```
Final Results:
  Total Chunks: 5
  Total Items:  500
  Sum:          124750
  Hosts Used:   3 (node001, node002, node003)
```

## Troubleshooting

- **Map tasks fail to start**: Verify the prepare task completed
  successfully before map tasks are submitted. Check that `.after(prep_job)`
  is called before `.map(chunks)`.
- **Reduce runs before map completes**: Ensure you pass the `map_jobs` array
  to `.after()`, not a single job.
- **Registry pull errors**: If compute nodes cannot pull images, configure a
  registry with `--packaging-registry`.

## See also

- [Map-reduce tutorial](../tutorials/map_reduce.md) for a guided walkthrough
  of the full example
- [Choosing a parallelization pattern](parallelization_patterns.md) for other
  orchestration patterns
- [Tasks and Workflows reference](../reference/api/tasks_workflows.md) for
  `.map()` and `.after()` API details
