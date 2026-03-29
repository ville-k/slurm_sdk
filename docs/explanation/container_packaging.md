# Container Packaging

Container packaging is the default execution model. Tasks are built into a container image, pushed to a registry if needed, and executed on Slurm via Pyxis/enroot.

## Build and resolve flow

1. **Resolve image reference**: `ContainerPackagingStrategy._resolve_image_reference` picks a registry/name:tag.
1. **Build image**: If a Dockerfile or build context is provided, the SDK runs `docker build` or `podman build`.
1. **Push image**: Controlled by `packaging_push` and `packaging_registry`.
1. **Convert for Pyxis**: Registry references are converted to enroot format when needed.

## Runtime behavior

- The job script exports `CONTAINER_IMAGE` for Pyxis.
- `PY_EXEC` is set to the configured Python executable inside the container.
- The runner executes with `srun --container-image` under the hood.

## Multi-word Python executables

When `python_executable` is a single word like `python`, the SDK sets `PY_EXEC` as a simple shell variable. However, when it contains multiple words (e.g., `uv run python`), the SDK stores it as a **bash array**:

```bash
# Single-word executable
PY_EXEC='python'

# Multi-word executable
PY_EXEC=('uv' 'run' 'python')
```

The array is resolved with `PY_EXEC_RESOLVED="${PY_EXEC[*]}"` and expanded using `${PY_EXEC[@]}` in the execution command. This approach avoids bash word-splitting issues that would occur if a multi-word command were stored in a plain string variable -- the shell would attempt to find an executable literally named `uv run python` rather than running `uv` with arguments `run python`.

## Container mounts

The SDK automatically mounts the **job base directory** (the parent of the task-level directory tree) into the container with read-write access. This allows the runner to locate result files from dependent jobs when resolving `JobResultPlaceholder` objects.

Additional mounts can be configured via the `packaging_mounts` task option. Mounts follow the standard `source:target:options` format:

```python
packaging_mounts=["/data:/data:ro", "/scratch:/scratch:rw"]
```

The SDK resolves shell expressions in mount paths so that job directory references remain valid inside the container.

## Container working directory

The container's working directory is set to the job directory via the `--container-workdir` flag on `srun`. This means task code that uses relative paths will resolve them against the job directory inside the container. If `container_workdir` is explicitly configured, the SDK uses that value instead, and `{job_dir}` can be used as a placeholder token.

## Array job container naming

Each container gets a unique name based on the job's pre-submission identifier: `slurm-sdk-{pre_submission_id}`. For array jobs, the SLURM array task ID is appended as a suffix: `slurm-sdk-{pre_submission_id}_{task_id}`. This naming scheme prevents container name collisions across array elements and enables `slurm jobs connect` to find and attach to the correct container.

## Configuration knobs

- `packaging_dockerfile`: Dockerfile path for builds.
- `packaging_context`: Build context directory.
- `packaging_registry`: Registry host/path for pushes and pulls.
- `packaging_platform`: Target platform (e.g., `linux/amd64`).
- `packaging_tls_verify`: TLS verification for registry access.
- `packaging_runtime`: Explicit runtime (`docker` or `podman`).
- `packaging_python_executable`: Python command inside the container (supports multi-word).
- `packaging_mounts`: Additional bind mounts for the container.

### Configuration example

A complete task definition with container packaging options:

```python
@task(
    time="01:00:00",
    gpus_per_node=4,
    packaging="container:my-registry.com/training:latest",
    packaging_python_executable="uv run python",
    packaging_mounts=["/data:/data:ro"],
)
def train(config: dict) -> dict:
    return run_training(config)
```

## How workflows reuse images

Workflow jobs export packaging config into `SLURM_SDK_PACKAGING_CONFIG`. Child tasks inherit the resolved image reference so they do not rebuild containers mid-workflow.

## Design goals

- Reproducible environments with minimal host coupling.
- Explicit control over build/push/pull behavior.
- Compatibility with Slurm + Pyxis/enroot deployments.
