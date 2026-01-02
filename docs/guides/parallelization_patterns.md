# Parallelization Patterns

Use this guide to explore fan-out/fan-in, pipeline stages, hyperparameter sweeps, and dynamic dependencies. The `slurm.examples.parallelization_patterns` example runs multiple patterns and prints progress for each phase.

## What it does
- Builds a container image from `src/slurm/examples/map_reduce.Dockerfile`.
- Runs several orchestration patterns in a single script.
- Demonstrates array jobs, dependency chaining, and dynamic task submission.

## Notes
- Use `--pattern` to run a single pattern (e.g. `fanout`, `pipeline`, `sweep`, `dynamic-deps`).
- The example prints per-pattern summaries so you can compare behaviors.

## Run this example
```bash
uv run python -m slurm.examples.parallelization_patterns \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --pattern all \
  --packaging container \
  --packaging-registry registry:5000/parallel-patterns \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```
