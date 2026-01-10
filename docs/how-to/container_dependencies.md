# Container Dependencies (Map-Reduce)

This guide shows how to chain containerized tasks using job dependencies and array jobs. The `slurm.examples.map_reduce` example builds a container image, prepares input data, runs a parallel map phase, and aggregates results in a reduce phase.

## What it does
- Builds a container image from `src/slurm/examples/map_reduce.Dockerfile`.
- Uses a preparation task to generate inputs.
- Runs a parallel map phase as an array job with dependencies.
- Aggregates results in a final reduce task.

## Notes
- Use `--num-chunks` to control the parallelism level.
- If your compute nodes cannot pull local images, configure a registry with `--packaging-registry`.

## Run this example
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
