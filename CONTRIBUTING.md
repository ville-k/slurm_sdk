# Contributing

Thanks for contributing to the Python SLURM SDK!

## Setup

- Install uv (see `https://github.com/astral-sh/uv`)
- Run tests:

```
uv run pytest
```

## Testing

- Unit tests are offline and use the local backend; add tests under `tests/`.
- SSH integration tests are optional; prefer marking them and enabling via env vars (e.g., `SLURM_HOST`, `SLURM_USER`).
- Use the `LocalBackend` for quick end-to-end checks without external services.

### Container Packaging Integration Tests

Container packaging tests verify end-to-end container build, push, and execution via Pyxis/enroot. These tests require additional setup:

**Prerequisites:**
- Podman or Docker installed
- Docker Compose or Podman Compose

**One-time setup:**

Add registry hostname to `/etc/hosts` (required for host-to-container registry communication):

```bash
echo '127.0.0.1 registry' | sudo tee -a /etc/hosts
```

**Running the tests:**

```bash
# Run all container packaging tests
uv run pytest tests/integration/test_container_packaging*.py -v

# Run specific test
uv run pytest tests/integration/test_container_packaging_basic.py::test_basic_container_task_execution -v
```

**How it works:**
- Docker Compose starts a local registry (`registry:5000`) and Pyxis-enabled Slurm cluster
- Tests build container images on the host and push to the local registry
- Pyxis imports images from the registry using enroot
- Tasks execute inside containers and return results

**Skipping container tests:**

```bash
# Run all tests except container packaging
uv run pytest -v -m "not container_packaging"
```

## Style

- Follow the Google Python Style Guide for public API docstrings and type hints.
- Keep info-level logs concise; prefer debug level for detailed internals.
- Use `slurm.logging.configure_logging()` for a pleasant default logging setup.

## Submitting Changes

- Ensure `uv run pytest` passes.
- Update docs (`README.md`, examples) when adding features that affect public APIs.
