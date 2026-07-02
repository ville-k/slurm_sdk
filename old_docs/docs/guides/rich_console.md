# Rich Console UX

The SDK can render richer progress indicators and syntax-highlighted errors by
passing a [`rich`](https://rich.readthedocs.io/en/latest/) console to
`Cluster`. This guide explains how to enable those improvements and what to
expect when they are active.

## When to enable the console

Provide a console when you want interactive feedback in the terminal, such as
spinners during submission or progress output while building container images.
It is optional; if you omit it the SDK falls back to standard logging.

```python
from rich.console import Console
from slurm.cluster import Cluster

console = Console()
cluster = Cluster.from_env(
    "Slurmfile.container_example.toml",
    env="default",
    console=console,
)
```

The same keyword is available when instantiating `Cluster` directly:

```python
cluster = Cluster(backend_type="ssh", hostname="login.example.com", console=console)
```

## What changes with a console

- **Submission feedback** – when `cluster.submit(...)` executes, the console
  shows a transient spinner explaining which backend is handling the job.
- **Container packaging** – docker/podman builds and pushes surface live
  progress, including step numbers and layer status updates.
- **Error reporting** – `SubmissionError` instances print syntax-highlighted
  sbatch scripts, taking advantage of Rich’s markup support.

You can see these improvements in action by running the GPU example:

```bash
uv run python -m slurm.examples.hello_cuda --slurmfile path/to/Slurmfile
```

The script constructs a console, passes it to `Cluster.from_env`, and prints
results (or exceptions) through Rich so the enhanced formatting is visible.

## Viewing the documentation

Use MkDocs to preview the documentation site locally:

```bash
uv run mkdocs serve
```

The command rebuilds the docs when files under `docs/` change and serves them at
http://127.0.0.1:8000.
