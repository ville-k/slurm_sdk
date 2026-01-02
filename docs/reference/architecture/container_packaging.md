# Container Packaging

Container packaging is the default execution model. Tasks are built into a container image, pushed to a registry if needed, and executed on Slurm via Pyxis/enroot.

## Build and resolve flow
1. **Resolve image reference**: `ContainerPackagingStrategy._resolve_image_reference` picks a registry/name:tag.
2. **Build image**: If a Dockerfile or build context is provided, the SDK runs `docker build` or `podman build`.
3. **Push image**: Controlled by `packaging_push` and `packaging_registry`.
4. **Convert for Pyxis**: Registry references are converted to enroot format when needed.

## Runtime behavior
- The job script exports `CONTAINER_IMAGE` for Pyxis.
- `PY_EXEC` is set to the configured Python executable inside the container.
- The runner executes with `srun --container-image` under the hood.

## Configuration knobs
- `packaging_dockerfile`: Dockerfile path for builds.
- `packaging_context`: Build context directory.
- `packaging_registry`: Registry host/path for pushes and pulls.
- `packaging_platform`: Target platform (e.g., `linux/amd64`).
- `packaging_tls_verify`: TLS verification for registry access.
- `packaging_runtime`: Explicit runtime (`docker` or `podman`).

## How workflows reuse images
Workflow jobs export packaging config into `SLURM_SDK_PACKAGING_CONFIG`. Child tasks inherit the resolved image reference so they do not rebuild containers mid-workflow.

## Design goals
- Reproducible environments with minimal host coupling.
- Explicit control over build/push/pull behavior.
- Compatibility with Slurm + Pyxis/enroot deployments.
