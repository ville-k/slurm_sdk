# Workflow Graph Visualization

This tutorial shows how to capture workflow structure and export it as a Graphviz graph. It uses `slurm.examples.workflow_graph_visualization`, which attaches a callback that records workflow submissions and child task relationships.

## Prerequisites

- Docker or Podman available locally to build the image.
- A Slurm cluster with Pyxis/enroot enabled.

## What you will do

- Run a small workflow inside a container image.
- Export a DOT file and JSON graph describing the workflow.
- Inspect the graph artifacts on disk.

## What to expect

- `workflow_graph.dot` and `workflow_graph.json` are written to the working directory.
- The workflow prints a summary of submitted tasks and their relationships.
- You can render the DOT file with Graphviz (optional):

  ```bash
  dot -Tpng workflow_graph.dot -o workflow_graph.png
  ```

## Run this example

```bash
uv run python -m slurm.examples.workflow_graph_visualization \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --packaging container \
  --packaging-registry registry:5000/workflow-graph \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```
