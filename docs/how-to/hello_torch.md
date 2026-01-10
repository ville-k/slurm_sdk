# GPU Hello Torch

This guide runs a PyTorch task inside a container and reports CPU/GPU details from the compute node. It uses `slurm.examples.hello_torch` and a Dockerfile that installs PyTorch.

## What it does

- Builds a container image from `src/slurm/examples/hello_torch.Dockerfile`.
- Submits a single task that prints CPU and GPU information.
- Returns a summary string with the detected PyTorch and CUDA versions.

## Notes

- Use a GPU-capable partition if your cluster requires it.
- The task prints GPU details when CUDA is available; otherwise it reports CPU-only mode.

## Run this example

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
