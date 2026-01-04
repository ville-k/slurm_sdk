# Container Basics

This tutorial builds on Hello World by showing how to run tasks inside a custom container image. The example uses `slurm.examples.hello_container`, which defines a simple task and packages it into a Dockerfile-built image.

## Prerequisites
- Docker or Podman available locally to build the image.
- A Slurm cluster with Pyxis/enroot enabled.

## What you will do
- Build a container image from `src/slurm/examples/hello_container.Dockerfile`.
- Submit a task that runs inside that image.
- Read the job output from your terminal.

## What to expect
- The task runs inside the container and prints basic runtime details.
- The result is printed in the local terminal once the job completes.

## Run this example
```bash
uv run python -m slurm.examples.hello_container \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --packaging container \
  --packaging-registry registry:5000/hello-container \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```
