# How to run a GPU task in a container

## Problem

You want to run a PyTorch task on GPU nodes using container packaging so the
environment is reproducible across development and production clusters.

## Prerequisites

- A Slurm cluster with GPU nodes and Pyxis/enroot installed
- A container registry accessible from compute nodes
- `slurm-sdk` installed locally
- Docker or Podman available for building images

## Steps

### 1. Create a Dockerfile with PyTorch

Create a Dockerfile that includes PyTorch and CUDA support. For GPU workloads,
use an official PyTorch image with CUDA as the base:

```dockerfile
FROM pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime

WORKDIR /workspace

COPY pyproject.toml README.md mkdocs.yml ./
COPY src/ src/
COPY docs/ docs/

RUN pip install --no-cache-dir .
```

Match the CUDA version in the base image to your cluster's GPU drivers. The
`runtime` variant is smaller than `devel` since it omits the CUDA compiler.

For CPU-only testing, a `python:3.11-slim` base with
`pip install torch --index-url https://download.pytorch.org/whl/cpu` works.

### 2. Define the task with GPU resources

Use the `@task` decorator with GPU-related parameters:

```python
from slurm import task


@task(
    nodes=1,
    ntasks_per_node=1,
    time="00:03:00",
    mem="512M",
    gpus=1,  # Request 1 GPU
)
def hello_torch() -> str:
    import torch

    cuda_available = torch.cuda.is_available()
    cuda_devices = torch.cuda.device_count() if cuda_available else 0

    if cuda_available and cuda_devices > 0:
        for i in range(cuda_devices):
            print(f"  GPU {i}: {torch.cuda.get_device_name(i)}")
        cuda_version = torch.version.cuda or "N/A"
        return f"PyTorch {torch.__version__} with CUDA {cuda_version}, {cuda_devices} GPU(s)"
    else:
        return f"PyTorch {torch.__version__} (CPU only)"
```

To request a specific GPU type, use `gres` instead of `gpus`:

```python
@task(
    nodes=1,
    time="00:03:00",
    mem="512M",
    gres="gpu:a100:1",
)
def a100_task() -> str:
    ...
```

### 3. Submit the job

Configure the cluster with container packaging and submit:

```python
from slurm import Cluster, Job

cluster = Cluster.from_args(
    args,
    default_packaging="container",
    default_packaging_dockerfile="path/to/your.Dockerfile",
)

with cluster:
    job: Job[str] = hello_torch()
    success = job.wait()
    if success:
        result = job.get_result()
        print(f"Result: {result}")
```

### 4. Run from the command line

Use the built-in example to verify your cluster setup:

```bash
uv run python -m slurm.examples.hello_torch \
  --hostname your-slurm-host \
  --username $USER \
  --partition gpu \
  --packaging container \
  --packaging-registry registry:5000/hello-torch \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```

Use a partition with GPU nodes (often named `gpu` or similar on your cluster).

## Verification

Check the job output for GPU detection:

```
INFO     Building container image...
INFO     Job submitted: 12347
INFO     Job running on gpu-node001
  GPU 0: NVIDIA A100-SXM4-40GB
INFO     Job completed successfully
Result: PyTorch 2.0.1 with CUDA 11.7, 1 GPU(s)
```

If no GPU is available, the output reports CPU-only mode instead.

## Troubleshooting

- **CUDA not detected**: Verify the partition has GPU nodes and that you
  requested GPU resources (`gpus=1` or `gres="gpu:..."` in the `@task`
  decorator).
- **Container fails to pull**: Check registry connectivity from compute nodes
  and verify the `--packaging-registry` URL is correct.
- **CUDA version mismatch**: Match the CUDA version in your Dockerfile's base
  image to the driver version installed on the cluster's GPU nodes.

## See also

- [GPU Hello Torch tutorial](../tutorials/hello_torch.md) for a guided
  walkthrough of this example
- [Container packaging explanation](../explanation/container_packaging.md) for
  how container packaging works in the SDK
- [Tasks and Workflows reference](../reference/api/tasks_workflows.md) for all
  `@task` decorator parameters
