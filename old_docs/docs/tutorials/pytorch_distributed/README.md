# PyTorch Distributed Training Tutorial

This tutorial demonstrates how to use the Slurm SDK for distributed PyTorch training that works seamlessly across:

- **Local macOS** (CPU or MPS backend)
- **Local Linux** (CPU or CUDA)
- **Slurm clusters** (multi-node CUDA with torchrun)

## What you'll learn

- Writing tasks that auto-detect the execution environment
- Using `JobContext` for distributed PyTorch setup
- Running the same code locally and on Slurm clusters
- Container packaging for reproducible environments
- Integration with `torchrun` for distributed data parallel (DDP) training

## Files in this tutorial

```
pytorch_distributed/
├── README.md              # This file
├── train.py               # Main training script with @task decorator
├── Dockerfile             # Container definition for cluster execution
├── Slurmfile.toml         # Cluster configuration
└── pyproject.toml         # Project dependencies
```

## Prerequisites

### Local development
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create project and install dependencies
uv sync
```

### Cluster execution (container packaging)

The Dockerfile uses uv for fast, reliable dependency installation. Container packaging requires:

```bash
# Install podman or docker
brew install podman  # macOS
# or: sudo apt install podman  # Linux

# Start podman machine (macOS only)
podman machine init --cpus 4 --memory 8192
podman machine start
```

If you are running on an Arm based mac and your Slurm cluster is running Intel/AMD, you need to install qemu on the machine to enable cross-builds:

```sh
podman machine ssh sudo rpm-ostree install qemu-user-static
podman machine ssh sudo systemctl reboot
podman machine stop
podman machine start
```


## Quick start

### 1. Run locally (single process)

```bash
# Train on local CPU/MPS
uv run python train.py --epochs 3 --batch-size 64
```

This runs the training task directly without SLURM, using your local PyTorch installation.

### 2. Run on SLURM cluster (distributed)

```bash
# Submit to cluster
uv run python train.py --submit --env production --epochs 10 --batch-size 128
```

This submits the job to your SLURM cluster with the configuration from `Slurmfile.toml`. The SDK automatically:
- Builds a container with the training code
- Submits the job via SSH
- Sets up distributed environment variables
- Launches training with torchrun

### 3. Monitor job progress

```bash
# The script will print the job ID
# You can monitor it with:
squeue -u $USER

# Or wait for completion and fetch results:
# (shown in the output of train.py --submit)
```

## How it works

### The training task (`train.py`)

```python
@task(time="01:00:00", ntasks=4, gpus_per_node=1, nodes=2)
def train_distributed(
    epochs: int,
    batch_size: int,
    lr: float = 0.001,
    *,
    job: JobContext | None = None
) -> dict:
    \"\"\"Train model locally or on cluster with automatic distributed setup.\"\"\"

    # Auto-detect device (CUDA, MPS, or CPU)
    device = get_device()

    # Setup distributed training if on cluster
    if job is not None:
        # We're on a SLURM cluster
        setup_distributed_pytorch(job)
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
    else:
        # Local execution
        rank = 0
        world_size = 1

    # Training code works in both modes
    model = create_model().to(device)

    if world_size > 1:
        model = DDP(model, device_ids=[rank])

    results = train_loop(model, epochs, batch_size, device)
    return results
```

###Key patterns

1. **Optional JobContext**: `job: JobContext | None = None` lets the function run with or without SLURM
2. **Auto device detection**: Works on CUDA, MPS (Apple Silicon), or CPU
3. **Conditional DDP**: Only wraps model in DistributedDataParallel when `world_size > 1`
4. **Environment setup**: Uses `job.torch_distributed_env()` to configure PyTorch distributed

### Container packaging (`Dockerfile`)

```dockerfile
FROM nvcr.io/nvidia/pytorch:24.12-py3

# Install uv for fast dependency installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies with uv
COPY requirements.txt /workspace/
RUN uv pip install --system -r requirements.txt

# Copy source code
COPY train.py /workspace/
WORKDIR /workspace

# The SDK runner will execute the task
CMD ["python", "-m", "slurm.runner"]
```

The Dockerfile uses uv's `--system` flag to install packages directly into the container's Python environment, which is much faster than pip.

### Cluster configuration (`Slurmfile.toml`)

```toml
[production.cluster]
backend = "ssh"
job_base_dir = "~/slurm_jobs"

[production.cluster.backend_config]
hostname = "cluster.example.com"
username = "your_username"
# password or key-based auth

[production.packaging]
type = "container"
dockerfile = "Dockerfile"
image = "registry.example.com/pytorch-training:latest"

[production.submit]
partition = "gpu"
account = "research"
```

## Advanced usage

### Multi-node training with torchrun

The SDK automatically sets up the environment for torchrun when `ntasks > 1`:

```python
@task(time="04:00:00", nodes=4, ntasks_per_node=4, gpus_per_node=4)
def train_large_model(config: dict, *, job: JobContext) -> dict:
    # job.torch_distributed_env() provides:
    # - MASTER_ADDR: first node hostname
    # - MASTER_PORT: communication port
    # - WORLD_SIZE: total number of processes
    # - RANK: global rank of this process
    # - LOCAL_RANK: rank on this node

    env = job.torch_distributed_env()
    os.environ.update(env)

    # Now torchrun/DDP will work correctly
    import torch.distributed as dist
    dist.init_process_group(backend="nccl")

    # Your training code here
    ...
```

### Testing distributed code locally

You can test multi-process training locally with torchrun:

```bash
# Local 2-process training
torchrun --nproc_per_node=2 train.py --epochs 1
```

## Troubleshooting

### "CUDA not available" on cluster
- Check that `gpus_per_node` is set in your `@task` decorator
- Verify your Slurmfile uses a GPU partition
- Ensure the container image includes CUDA

### "Address already in use" errors
- The MASTER_PORT (default 29500) might be in use
- Set `MASTER_PORT` explicitly in your task:
  ```python
  os.environ.setdefault("MASTER_PORT", str(29500 + job.job_id % 1000))
  ```

### Container build fails
- Check that podman/docker is running: `podman ps`
- Verify Dockerfile paths are correct
- Try building manually: `podman build -f Dockerfile -t test .`

## Next steps

- See [guides/job_context.md](../../docs/guides/job_context.md) for more on `JobContext`
- See [guides/local_cluster_testing.md](../../docs/guides/local_cluster_testing.md) for testing patterns
- Check [examples/distributed_context.py](../../src/slurm/examples/distributed_context.py) for a minimal example
- Explore [examples/hello_cuda.py](../../src/slurm/examples/hello_cuda.py) for GPU device selection
