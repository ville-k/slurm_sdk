# Slurm SDK

Container-first job orchestration for Slurm clusters. Define tasks in Python, package them into reproducible container images, and submit workflows with clear dependency graphs and rich callbacks.

## Why Slurm SDK

- **Pythonic workflows**: Use `@task` and `@workflow` to express dependencies without writing shell scripts.
- **Container-first**: Build and run tasks in container images for reproducible environments.
- **Cluster friendly**: Targets Slurm + Pyxis/enroot with explicit packaging and submission controls.
- **Observable runs**: Callbacks and structured logs let you see what is happening across jobs.
- **Composable patterns**: Map-reduce, fan-out/fan-in, dynamic dependencies, and more.

## Quick start (container packaging)

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

## How the docs are organized

- **Tutorials**: Learn the basics by running real examples.
- **Guides**: Solve specific problems with step-by-step recipes.
- **Reference**: API docs and deep dives on the architecture.

## Inspired by
Slurm SDK borrows ideas from modern orchestration systems while staying Slurm-native:

- [Flyte](https://flyte.org/)
- [Prefect](https://www.prefect.io/)
- [Metaflow](https://metaflow.org/)
- [Dagster](https://dagster.io/)

## Next steps

- Start with the [Hello World tutorial](tutorials/getting_started_hello_world.md).
- Visualize workflows with [Workflow Graph Visualization](tutorials/workflow_graph_visualization.md).
- Explore the [API reference](reference/api/index.md).
