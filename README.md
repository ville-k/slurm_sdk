# Python Slurm SDK

A developer-friendly SDK to define, submit, and manage [Slurm](https://slurm.schedmd.com/) jobs in Python with native array job support, fluent dependency APIs, and container packaging.

## Quick Start

### Simple Task with Dependencies

```python
from slurm import Cluster, task

@task(time="00:01:00", mem="1G")
def preprocess(dataset: str) -> str:
    return f"processed_{dataset}"

@task(time="00:05:00", mem="4G", cpus_per_task=4)
def train_model(data: str, lr: float) -> dict:
    return {"model": f"trained_on_{data}", "lr": lr, "accuracy": 0.95}

@task(time="00:01:00", mem="1G")
def evaluate(model: dict) -> str:
    return f"Model accuracy: {model['accuracy']}"

if __name__ == "__main__":
    # Local backend for offline development
    with Cluster(backend_type="local") as cluster:
        # Fluent dependency API with .after()
        prep_job = preprocess("dataset.csv")
        train_job = train_model.after(prep_job)(data=prep_job, lr=0.001)
        eval_job = evaluate.after(train_job)(model=train_job)

        eval_job.wait()
        print(eval_job.get_result())
```

### Array Jobs for Parallel Processing

```python
from slurm import Cluster, task

@task(time="00:02:00", mem="2G", cpus_per_task=2)
def process_chunk(chunk_id: int, start: int, end: int) -> dict:
    return {"chunk": chunk_id, "sum": sum(range(start, end))}

@task(time="00:01:00", mem="1G")
def aggregate(results: list) -> int:
    return sum(r["sum"] for r in results)

if __name__ == "__main__":
    with Cluster(backend_type="local") as cluster:
        # Map task over items to create native SLURM array job
        chunks = [
            {"chunk_id": i, "start": i * 1000, "end": (i + 1) * 1000}
            for i in range(10)
        ]
        array_job = process_chunk.map(chunks)

        # Aggregate results from array job
        final = aggregate.after(array_job)(results=array_job.get_results())
        final.wait()
        print(f"Total: {final.get_result()}")
```

### SSH Backend with Container Packaging

```python
from slurm import Cluster, task

@task(
    time="00:10:00",
    mem="8G",
    packaging="container",
    packaging_platform="linux/amd64",
    packaging_push=True,
    packaging_registry="myregistry.io/myproject/",
)
def compute_intensive_task(n: int) -> float:
    import numpy as np
    return np.mean(np.random.random(n))

if __name__ == "__main__":
    cluster = Cluster(
        backend_type="ssh",
        hostname="login.hpc.example.com",
        username="myuser",
    )

    with cluster:
        job = compute_intensive_task(1_000_000)
        job.wait()
        print(job.get_result())
```

## Documentation

- [CONTRIBUTING.md](CONTRIBUTING.md) - Development setup, testing, and contribution guidelines
- [AGENTS.md](AGENTS.md) - Guide for AI agents working with this codebase
- [examples/](src/slurm/examples/) - Complete working examples including parallelization patterns

## Running on an ARM Mac (Apple Silicon)

You can build and publish x86_64 (``linux/amd64```) images from an ARM Mac by
enabling QEMU emulation inside your container runtime. The example below shows
the required one-time setup for Podman; Docker Desktop users can follow a
similar flow using `docker buildx` (no additional configuration usually needed).

1. Install QEMU locally. This provides the binary emulators that Podman will
   load into its virtual machine:

   ```sh
   brew install qemu
   ```

2. Make sure the Podman machine is created and running. If you have not
   initialised it yet:

   ```sh
   podman machine init --now
   ```

3. Register the QEMU static binaries inside the Podman machine so it can run
   amd64 containers. This step reboots the VM and only needs to be performed
   once (repeat if you recreate the machine):

   ```sh
   podman machine ssh sudo rpm-ostree install qemu-user-static
   podman machine ssh sudo systemctl reboot
   ```

4. After the machine restarts, confirm cross-building works:

   ```sh
   podman build --platform linux/amd64 -t test-amd64 .
   podman run --rm --platform linux/amd64 test-amd64 uname -m
   ```

   The final command should print `x86_64` even though you are on an ARM host.

With QEMU registered you can now build the example container in this project
using the provided Slurmfile (`platform = "linux/amd64"`) and push it to your
registry before submitting jobs to an x86 cluster.
