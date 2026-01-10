# Contributing

Thanks for contributing to the Python SLURM SDK!

## Development Setup

There are two ways to develop the SDK:

### Option 1: Devcontainer (Recommended)

The easiest way to get started is using the devcontainer, which provides a fully configured development environment with access to a local Slurm cluster.

1. **Prerequisites:**
   - Docker or Podman installed and running
   - VSCode or Cursor with the "Dev Containers" extension

2. **Open in container:**
   - Open the project in VSCode/Cursor
   - Click "Reopen in Container" when prompted (or use Command Palette: "Dev Containers: Reopen in Container")
   - Wait for the container to build and dependencies to install

3. **Run tests:**

   ```bash
   # Unit tests
   uv run pytest tests/ --ignore=tests/integration

   # Integration tests (Slurm cluster is already available)
   uv run pytest --run-integration tests/integration/
   ```

The devcontainer automatically starts:

- A Slurm cluster with Pyxis/enroot (accessible at `slurm:22`)
- A container registry (accessible at `registry:5000`)

### Option 2: Host Development

If you prefer developing on your host machine:

1. **Install uv:**

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Install dependencies:**

   ```bash
   uv sync --dev
   ```

3. **Run unit tests:**

   ```bash
   uv run pytest tests/ --ignore=tests/integration
   ```

4. **Run integration tests** (requires Docker/Podman):

   ```bash
   # Using the integration test runner script
   ./scripts/run-integration-tests.sh

   # Or manually start services and run tests
   docker compose -f containers/docker-compose.yml up -d slurm registry
   uv run pytest --run-integration tests/integration/
   ```

## Testing

### Unit Tests

Unit tests run without any external services:

```bash
uv run pytest tests/ --ignore=tests/integration
```

These use the `LocalBackend` for quick end-to-end checks.

### Integration Tests

Integration tests require a running Slurm cluster. There are several ways to run them:

**Using the test runner script (recommended for host development):**

```bash
# Run all integration tests
./scripts/run-integration-tests.sh

# Run specific tests
./scripts/run-integration-tests.sh -k "test_submit"

# Keep services running after tests
./scripts/run-integration-tests.sh --keep

# Verbose output
./scripts/run-integration-tests.sh -v
```

**Inside devcontainer:**

```bash
uv run pytest --run-integration tests/integration/
```

**Manual setup on host:**

```bash
# Start services
docker compose -f containers/docker-compose.yml up -d slurm registry

# Run tests
uv run pytest --run-integration tests/integration/

# Stop services when done
docker compose -f containers/docker-compose.yml down -v
```

### Container Packaging Tests

Container packaging tests verify end-to-end container build, push, and execution via Pyxis/enroot.

**One-time setup (when running tests from host machine):**

Add registry hostname to `/etc/hosts`:

```bash
echo '127.0.0.1 registry' | sudo tee -a /etc/hosts
```

**Why is this needed?** When you push an image as `registry:5000/myimage`, both the host (pushing) and the Slurm container (pulling) must resolve `registry` to reach the same registry service. The `/etc/hosts` entry makes `registry` resolve to `localhost` on your machine, where docker-compose exposes the registry on port 5000.

**Not needed inside devcontainer:** If you run tests from inside the devcontainer, Docker's internal DNS automatically resolves `registry` to the registry container. No `/etc/hosts` modification required.

**Running the tests:**

```bash
# Run all container packaging tests
uv run pytest --run-integration tests/integration/test_container_packaging*.py -v

# Run specific test
uv run pytest --run-integration tests/integration/test_container_packaging_basic.py::test_basic_container_task_execution -v
```

**Skipping container tests:**

```bash
uv run pytest --run-integration -v -m "not container_packaging"
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SLURM_SDK_DEV_MODE` | Execution mode: `host`, `devcontainer`, or `ci` | Auto-detected |
| `SLURM_HOST` | Slurm cluster hostname | `slurm` (devcontainer) or `127.0.0.1` (host) |
| `SLURM_PORT` | Slurm SSH port | `22` (devcontainer) or `2222` (host) |
| `REGISTRY_URL` | Container registry URL | `registry:5000` |
| `SLURM_RUN_INTEGRATION` | Enable integration tests | `0` |
| `SLURM_TEST_KEEP` | Keep docker-compose services after tests | Not set |
| `SLURM_TEST_COMPOSE_COMMAND` | Override compose command | Auto-detected |
| `SLURM_TEST_COMPOSE_NO_BUILD` | Skip image rebuilds | Not set |
| `SLURM_TEST_CONTAINER_RUNTIME` | Force container runtime (`docker`/`podman`) | Auto-detected |

## Troubleshooting

### Docker/Podman Issues

**"Cannot connect to the Docker daemon"**

- Start Docker Desktop, or
- Run `systemctl start docker` (Linux)

**"Cannot connect to Podman"**

- Run `podman machine init --now` (one-time setup)
- Run `podman machine start`

**Note:** If `docker info` shows Podman output, your `docker` CLI is a Podman shim. You still need `podman machine start`.

### Port Conflicts

**"Port 5000 is already in use"**

- Stop conflicting services, or
- Modify port mappings in `containers/docker-compose.yml`

**"Port 2222 is already in use"**

- Stop any existing Slurm containers: `docker compose -f containers/docker-compose.yml down`

### Devcontainer Issues

**Container fails to start**

- Check Docker/Podman is running
- Try rebuilding: Command Palette → "Dev Containers: Rebuild Container"

**SSH to Slurm fails inside devcontainer**

- Wait for services to be healthy (check with `docker compose ps`)
- Verify network: `getent hosts slurm` should resolve

**Tests hang**

- Check Slurm services: `docker exec slurm-test squeue`
- Check SSH: `docker exec slurm-test systemctl status sshd`

### Registry Issues

**"registry:5000" not found (host only)**

- This only happens when running tests from your host machine, not inside devcontainer
- Add to `/etc/hosts`: `echo '127.0.0.1 registry' | sudo tee -a /etc/hosts`
- Alternatively, run tests inside the devcontainer where Docker DNS handles resolution

**Push fails with connection refused**

- Ensure registry is running: `docker compose -f containers/docker-compose.yml ps registry`
- Check port: `curl -s http://localhost:5000/v2/`

## Project Structure

```text
slurm_sdk/
├── .devcontainer/           # VSCode/Cursor devcontainer config
│   └── devcontainer.json
├── containers/              # Container definitions
│   ├── docker-compose.yml   # Unified compose for dev/test
│   ├── dev/                 # Development container
│   │   └── Containerfile
│   └── slurm-pyxis-integration/  # Slurm cluster container
├── scripts/
│   └── run-integration-tests.sh  # Host integration test runner
├── src/slurm/               # SDK source code
└── tests/
    ├── integration/         # Integration tests (require Slurm)
    └── *.py                 # Unit tests
```

## Style

- Follow the Google Python Style Guide for public API docstrings and type hints.
- Keep info-level logs concise; prefer debug level for detailed internals.
- Use `slurm.logging.configure_logging()` for a pleasant default logging setup.

## Submitting Changes

1. Ensure all tests pass:

   ```bash
   uv run pytest
   ```

2. For integration changes, also run:

   ```bash
   ./scripts/run-integration-tests.sh
   ```

3. Update docs (`README.md`, examples) when adding features that affect public APIs.
