# Getting Started: Hello World

Run a minimal Slurm SDK task packaged into a container image. This tutorial uses the built-in `slurm.examples.hello_world` example and shows the smallest end-to-end path from Python to a running Slurm job.

## Prerequisites
- Docker or Podman available locally to build the image.
- A Slurm cluster with Pyxis/enroot enabled for container execution.
- A Python environment with `slurm-sdk` installed.

## What you will do
- Build a container image from the example Dockerfile.
- Submit a task to Slurm and fetch its result.
- Inspect the output from the job.

## What to expect
- The SDK builds the image from `src/slurm/examples/hello_world.Dockerfile`.
- The job runs inside the container and prints a greeting with the node hostname.
- `Result:` is printed in the local terminal after the job completes.

## Run this example
```bash
uv run python -m slurm.examples.hello_world \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --packaging container \
  --packaging-registry registry:5000/hello-world \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```
