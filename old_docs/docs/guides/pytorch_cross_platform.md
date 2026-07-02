# Cross-platform PyTorch workflow (macOS → Linux GPU)

This guide walks through a pragmatic setup for iterating on a PyTorch project on
macOS while targeting an x86_64 NVIDIA cluster that runs Linux and CUDA. The
workflow keeps local inner loops fast, relies on `uv` for reproducible
environments, and uses container packaging so the same artifact can be submitted
from any laptop.

## 1. Local prerequisites

Install the tooling you need on macOS:

1. Install [uv](https://github.com/astral-sh/uv):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
2. Install a container runtime. Podman works well on macOS because it does not
   require a privileged daemon:
   ```bash
   brew install podman
   podman machine init --cpus 6 --memory 8192 --disk-size 40
   podman machine start
   ```
3. Install the project in editable mode:
   ```bash
   uv pip install -e .[dev]
   ```

## 2. Structure the project for portability

A simple directory layout keeps host-specific code out of the critical path:

```
project/
├── pyproject.toml
├── slurm/            # your tasks package
├── Slurmfile.toml
├── conf/             # Hydra or other configuration
└── docker/           # container context (Dockerfile, assets)
```

Inside `slurm/tasks.py` define your workload using the SDK:

```python
from slurm.decorators import task

@task(time="04:00:00", gpus_per_node=1, nodes=1)
def train(cfg: dict) -> None:
    import torch
    ...  # training loop that already works locally
```

## 3. Configure container packaging

The container captures the Linux/CUDA dependencies no matter where you submit
from. Add a dedicated profile to `Slurmfile.toml`:

```toml
[local]
cluster = { backend = "ssh", backend_config = { hostname = "localhost", username = "${env.USER}" } }
submit = { partition = "debug", account = "research" }
packaging = { type = "container", container = "docker/pytorch-gpu.Dockerfile" }
```

Create `docker/pytorch-gpu.Dockerfile` that targets the cluster architecture:

```Dockerfile
FROM nvcr.io/nvidia/pytorch:24.06-py3

# Install uv inside the image for dependency syncing
RUN pip install --upgrade pip uv

# Copy project metadata for uv sync
COPY pyproject.toml uv.lock /workspace/
WORKDIR /workspace
RUN uv sync --frozen --no-dev

# Copy source after dependencies to leverage caching
COPY slurm /workspace/slurm
ENTRYPOINT ["python", "-m", "slurm.runner"]
```

Build the image locally through the packaging strategy. The SDK handles the
`podman` invocation when `packaging.type = "container"` is set.

## 4. Keep local execution fast

macOS wheels for PyTorch target Metal/CPU and differ from the CUDA build. Use
conditional dependencies in `pyproject.toml` so `uv` installs the appropriate
variant automatically:

```toml
[project.optional-dependencies]
gpu = ["torch==2.4.0+cu124", "torchvision==0.19.0+cu124"]
mac = ["torch==2.4.0", "torchvision==0.19.0"]

[tool.uv]
index-url = "https://download.pytorch.org/whl/cu124"
```

Then in the container `uv sync --extra gpu` while on macOS you develop with
`uv sync --extra mac`. This keeps lock files consistent but still optimized for
each platform.

## 5. Submit from macOS

With the pieces in place you can submit directly from your laptop:

```bash
uv run python -m slurm.tasks.launch --env=gpu-cluster --config-dir conf
```

When the job is packaged, the SDK builds the container artifact locally via
Podman, uploads it through the SSH backend, and schedules it with SBATCH. The
remote nodes use the CUDA-enabled image so there is no mismatch between macOS
libraries and the cluster runtime.

### Additional tips

- Add `--mount` directives in the packaging config if you need datasets staged
  on the cluster (`packaging.mount_job_dir = true`).
- Cache container layers by pointing `packaging.container.context` to a minimal
  directory that only contains what changes between builds.
- If your cluster supports Singularity/Apptainer, use the container image built
  by Podman and convert it server-side; the packaging strategy exposes hooks to
  run custom commands after the build completes.
- Keep CI parity by reusing the same container image in GitHub Actions jobs that
  run integration tests.
