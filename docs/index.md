---
hide:
  - navigation
---

# Slurm SDK

<p class="subtitle" style="font-size: 1.3em; color: var(--md-default-fg-color--light);">
Container-first job orchestration for Slurm clusters
</p>

[Get Started](tutorials/getting_started_hello_world.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/ville-k/slurm_sdk){ .md-button }

---

## Zero Infrastructure, Maximum Control

Slurm SDK lets you define tasks in Python, package them into reproducible containers, and submit workflows to your existing Slurm cluster. No Kubernetes. No control plane. Just your code and your cluster.

<div class="grid cards" markdown>

-   :material-language-python:{ .lg .middle } __Pythonic Workflows__

    ---

    Use `@task` and `@workflow` decorators to express dependencies. No shell scripts required.

-   :material-cube-outline:{ .lg .middle } __Container-First__

    ---

    Build and run tasks in container images for reproducible environments across clusters.

-   :material-server-network:{ .lg .middle } __Slurm Native__

    ---

    Direct integration with Slurm via SSH. Native array jobs, dependencies, and resource specs.

-   :material-eye-outline:{ .lg .middle } __Observable__

    ---

    Rich callbacks and structured logs let you monitor what's happening across jobs.

</div>

---

## Quick Example

```python
from slurm import Cluster, task

@task(time="00:10:00", mem="4G", cpus_per_task=4)
def train_model(dataset: str, learning_rate: float) -> dict:
    # Your training code here
    return {"accuracy": 0.95, "model_path": "/results/model.pt"}

@task(time="00:05:00", mem="2G")
def evaluate(model_path: str) -> dict:
    # Evaluation code
    return {"test_accuracy": 0.93}

# Connect and submit
with Cluster(backend_type="ssh", hostname="login.hpc.example.com") as cluster:
    train_job = train_model(dataset="data.csv", learning_rate=0.001)
    eval_job = evaluate.after(train_job)(model_path=train_job)

    eval_job.wait()
    print(eval_job.get_result())
```

---

## Why Slurm SDK?

| Feature | Slurm SDK | Kubernetes Orchestrators |
|---------|-----------|-------------------------|
| **Infrastructure** | Your existing Slurm cluster | Requires K8s cluster |
| **Setup time** | Minutes | Hours to days |
| **Control plane** | None needed | Must deploy and maintain |
| **Resource specs** | Native Slurm (partitions, time, mem, GPUs) | Abstracted away |
| **Array jobs** | Native support | Varies by tool |
| **Learning curve** | Python decorators | New DSL + K8s concepts |

---

## Installation

```bash
# Using uv (recommended)
uv add slurm-sdk

# Using pip
pip install slurm-sdk
```

---

## Quick Start

Run the hello world example against your cluster:

```bash
uv run python -m slurm.examples.hello_world \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --packaging container \
  --packaging-registry registry:5000/hello-world \
  --packaging-platform linux/amd64
```

---

## Documentation

<div class="grid cards" markdown>

-   :material-school:{ .lg .middle } __Tutorials__

    ---

    Learn the basics by running real examples end-to-end.

    [:octicons-arrow-right-24: Start learning](tutorials/index.md)

-   :material-tools:{ .lg .middle } __How-To Guides__

    ---

    Solve specific problems with step-by-step recipes.

    [:octicons-arrow-right-24: Find solutions](how-to/index.md)

-   :material-book-open-variant:{ .lg .middle } __Explanation__

    ---

    Understand how Slurm SDK works under the hood.

    [:octicons-arrow-right-24: Learn concepts](explanation/index.md)

-   :material-api:{ .lg .middle } __API Reference__

    ---

    Technical reference for classes, functions, and configuration.

    [:octicons-arrow-right-24: Browse API](reference/api/index.md)

</div>

---

## Inspired By

Slurm SDK borrows ideas from modern orchestration systems while staying Slurm-native:

- [Flyte](https://flyte.org/) - Kubernetes-native workflow orchestration
- [Prefect](https://www.prefect.io/) - Dataflow automation
- [Metaflow](https://metaflow.org/) - ML infrastructure by Netflix
- [Dagster](https://dagster.io/) - Data orchestration platform

---

## Getting Help

- [:octicons-mark-github-16: GitHub Issues](https://github.com/ville-k/slurm_sdk/issues) - Report bugs or request features
- [:octicons-book-16: Contributing Guide](CONTRIBUTING.md) - Help improve Slurm SDK
