# Testing: Local vs Cluster Execution

When developing tasks, you often want to test logic locally before submitting to a cluster. This guide shows patterns for writing code that runs both locally (for quick iteration) and on SLURM clusters (for production).

## Using optional JobContext parameter

The simplest pattern is to make the `job: JobContext` parameter **keyword-only and optional**. The SDK only injects it when running inside SLURM:

```python
from slurm.decorators import task
from slurm.runtime import JobContext

@task(time="00:10:00", ntasks=4)
def train_model(config: dict, *, job: JobContext | None = None) -> dict:
    """Train a model locally or on the cluster."""

    if job is not None:
        # Running on cluster - use distributed setup
        import os
        env = job.torch_distributed_env()
        os.environ.update(env)
        print(f"Running on {job.world_size} tasks across {len(job.hostnames or [])} nodes")
        distributed = True
    else:
        # Running locally - single process
        print("Running locally in single-process mode")
        distributed = False

    # Your training code adapts based on context
    model = create_model(config)

    if distributed:
        model = wrap_for_distributed(model)

    result = train(model, config)
    return result
```

**Local execution:**
```python
# Just call the function directly - no cluster needed
config = {"lr": 0.001, "epochs": 10}
result = train_model(config)
print(result)
```

**Cluster execution:**
```python
from slurm.cluster import Cluster

cluster = Cluster.from_env("Slurmfile.toml", env="production")
job = cluster.submit(train_model)(config)
result = job.wait() and job.get_result()
```

## Checking SLURM environment variables

Another pattern is to check for SLURM environment variables directly:

```python
import os
from slurm.decorators import task

@task(time="00:10:00", mem="4G")
def process_data(input_path: str) -> str:
    """Process data with environment-aware configuration."""

    # Detect if running on SLURM
    on_cluster = "SLURM_JOB_ID" in os.environ

    if on_cluster:
        # Use cluster-specific paths and parallelism
        scratch_dir = os.environ.get("SCRATCH", "/tmp")
        num_workers = int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))
    else:
        # Use local development settings
        scratch_dir = "/tmp"
        num_workers = 1

    output = do_processing(
        input_path,
        scratch=scratch_dir,
        workers=num_workers
    )

    return output
```

**Why this works:** SLURM always sets `SLURM_JOB_ID` and other environment variables when executing inside a job step. These are absent in local environments.

## Using current_job_context() with try/except

For code that needs runtime context inspection without decorator injection:

```python
from slurm.runtime import current_job_context, JobContext

def setup_distributed_backend():
    """Configure distributed backend if running on cluster."""
    try:
        job = current_job_context()
        # We're on a cluster
        env = job.torch_distributed_env()
        import os
        os.environ.update(env)
        return True
    except (KeyError, ValueError):
        # No SLURM environment - running locally
        return False

@task(time="00:30:00", ntasks=8)
def distributed_training(model_config: dict) -> dict:
    """Training that auto-detects cluster vs local."""

    is_distributed = setup_distributed_backend()

    model = build_model(model_config)

    if is_distributed:
        # Wrap model for DDP
        import torch.distributed as dist
        model = torch.nn.parallel.DistributedDataParallel(model)

    # Training code works in both modes
    results = train_loop(model, distributed=is_distributed)
    return results
```

## Testing with mock JobContext

For unit tests, you can create a mock `JobContext`:

```python
import pytest
from slurm.runtime import JobContext

def test_train_model_local():
    """Test training in local mode."""
    from my_tasks import train_model

    config = {"lr": 0.001}
    result = train_model(config, job=None)

    assert result["status"] == "completed"

def test_train_model_distributed():
    """Test training in distributed mode."""
    from my_tasks import train_model

    # Create a mock cluster context
    mock_job = JobContext(
        job_id="12345",
        world_size=4,
        rank=0,
        local_rank=0,
        node_rank=0,
        master_addr="node001",
        master_port=29500,
        hostnames=("node001", "node002"),
        environment={"SLURM_JOB_ID": "12345"},
        output_dir=None
    )

    config = {"lr": 0.001}
    result = train_model(config, job=mock_job)

    assert result["distributed"] is True
```

## Best practices

1. **Make JobContext optional** – use `job: JobContext | None = None` in signatures so code runs without it
2. **Graceful degradation** – when running locally, fall back to sensible defaults (single process, `/tmp` scratch space, etc.)
3. **Environment detection** – check `SLURM_JOB_ID` or use try/except around `current_job_context()` to detect cluster execution
4. **Test both paths** – write unit tests for both local and distributed modes
5. **Separate IO paths** – use `job.output_dir` on cluster, but allow overriding for local development

## Example: Cross-platform PyTorch script

See [`examples/hello_cuda.py`](../src/slurm/examples/hello_cuda.py) for a complete example that:
- Runs locally on macOS (CPU or MPS backend)
- Runs on SLURM clusters (CUDA GPUs)
- Adapts device selection based on environment
- Uses optional JobContext for cluster-specific features

The key pattern is:
```python
@task(gpus=1, time="00:10:00")
def gpu_task(data: dict, *, job: JobContext | None = None) -> dict:
    # Auto-detect device
    if torch.cuda.is_available():
        device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        device = "mps"
    else:
        device = "cpu"

    # Use job context if available
    if job:
        print(f"Running on SLURM job {job.job_id}")
        output_dir = job.output_dir
    else:
        output_dir = Path("./output")

    # Task logic works everywhere
    model = model.to(device)
    result = train(model, data)
    save_checkpoint(result, output_dir / "checkpoint.pt")

    return result
```

This pattern gives you the best of both worlds: fast local iteration during development and seamless cluster deployment for production workloads.
