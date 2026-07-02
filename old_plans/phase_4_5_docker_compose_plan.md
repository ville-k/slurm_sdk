# Phase 4.5: Docker Compose Refactoring Plan

## Overview

Refactor the container packaging integration tests to use docker-compose for managing services. This solves the networking issue by creating a shared network where services can communicate using DNS names.

## Goals

1. **Fix networking issue**: Registry and Pyxis container on same network
2. **Simplify infrastructure**: Declarative service definitions vs imperative Python code
3. **Improve maintainability**: Standard docker-compose.yml configuration
4. **Enable DNS resolution**: Services accessible by name (e.g., `registry:5000`)

## Architecture

### Before (Current)
```
Host Machine
├── Registry container (localhost:5000) - Started by Python fixture
├── Pyxis container (random SSH port) - Started by Python fixture
└── Problem: Containers cannot communicate
```

### After (docker-compose)
```
Host Machine
└── Docker Compose Stack
    ├── registry service (registry:5000)
    ├── slurm-pyxis service (slurm-pyxis:22)
    └── Shared network: slurm-test-network
        → Registry accessible as "registry:5000" from Pyxis container
        → Pyxis accessible as "slurm-pyxis:22" from host
```

## Implementation Plan

### Step 1: Create docker-compose.yml

**Location**: `tests/integration/docker-compose.yml`

```yaml
version: '3.8'

services:
  # Local container registry for testing
  registry:
    image: registry:2
    container_name: slurm-test-registry
    ports:
      - "5000:5000"  # Expose to host for building/pushing
    networks:
      - slurm-test-network
    restart: unless-stopped
    environment:
      REGISTRY_STORAGE_DELETE_ENABLED: "true"
    volumes:
      - registry-data:/var/lib/registry

  # Pyxis-enabled Slurm cluster
  slurm-pyxis:
    build:
      context: ../../containers/slurm-pyxis-integration
      dockerfile: Containerfile
    image: slurm-pyxis-integration:latest
    container_name: slurm-test-pyxis
    hostname: slurm-control
    privileged: true
    cgroupns: host
    ports:
      - "2222:22"  # Fixed SSH port for easier access
    networks:
      - slurm-test-network
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
      - pyxis-home:/home/slurm
    depends_on:
      - registry
    # Use systemd as init
    command: ["/sbin/init"]

networks:
  slurm-test-network:
    driver: bridge
    name: slurm-test-network

volumes:
  registry-data:
    name: slurm-test-registry-data
  pyxis-home:
    name: slurm-test-pyxis-home
```

### Step 2: Refactor conftest.py Fixtures

**Changes to `tests/integration/conftest.py`**:

#### 2.1 Remove Custom Container Management Code

**Delete**:
- `local_registry()` fixture - Replace with docker-compose
- `pyxis_container_image()` fixture - docker-compose handles building
- `pyxis_container()` fixture - Replace with docker-compose

**Keep**:
- `container_runtime()` - Still needed for detecting podman/docker
- `sdk_on_pyxis_cluster()` - Still needed to install SDK
- `slurm_pyxis_cluster_config()` - Minimal changes (use fixed port)
- `slurm_pyxis_cluster()` - No changes needed

#### 2.2 Add Docker Compose Fixtures

```python
import subprocess
import time
from pathlib import Path

COMPOSE_FILE = Path(__file__).parent / "docker-compose.yml"

@pytest.fixture(scope="session")
def docker_compose_project(request):
    """
    Start docker-compose services and ensure they're ready.

    This fixture:
    1. Starts registry and slurm-pyxis services
    2. Waits for services to be healthy
    3. Returns service connection info
    4. Tears down services on test completion
    """
    # Start services
    subprocess.run(
        ["docker-compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"],
        check=True,
        capture_output=True
    )

    # Wait for services to be ready
    max_retries = 30
    for i in range(max_retries):
        result = subprocess.run(
            ["docker-compose", "-f", str(COMPOSE_FILE), "ps", "--services", "--filter", "status=running"],
            capture_output=True,
            text=True
        )
        running_services = result.stdout.strip().split('\n')
        if 'registry' in running_services and 'slurm-pyxis' in running_services:
            break
        time.sleep(1)
    else:
        # Cleanup on failure
        subprocess.run(["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"], check=False)
        pytest.fail("Docker compose services did not start within timeout")

    # Wait for SSH to be ready in Pyxis container
    time.sleep(5)
    for i in range(30):
        result = subprocess.run(
            ["docker-compose", "-f", str(COMPOSE_FILE), "exec", "-T", "slurm-pyxis",
             "systemctl", "is-active", "sshd"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and "active" in result.stdout:
            break
        time.sleep(1)
    else:
        subprocess.run(["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"], check=False)
        pytest.fail("SSH service did not start in Pyxis container")

    # Return connection info
    info = {
        "registry_url": "localhost:5000",  # For host (building/pushing)
        "registry_url_container": "registry:5000",  # For container runtime (pulling)
        "pyxis_hostname": "127.0.0.1",
        "pyxis_port": 2222,  # Fixed SSH port
        "pyxis_username": "slurm",
        "pyxis_password": "slurm",
    }

    # Cleanup on test completion
    def cleanup():
        if not os.environ.get("SLURM_TEST_KEEP"):
            subprocess.run(
                ["docker-compose", "-f", str(COMPOSE_FILE), "down", "-v"],
                check=False,
                capture_output=True
            )

    request.addfinalizer(cleanup)

    return info


@pytest.fixture(scope="session")
def local_registry(docker_compose_project):
    """Return registry URL for host (building/pushing)."""
    return docker_compose_project["registry_url"]


@pytest.fixture(scope="session")
def registry_for_containers(docker_compose_project):
    """Return registry URL for containers (pulling via service name)."""
    return docker_compose_project["registry_url_container"]


@pytest.fixture(scope="session")
def pyxis_container(docker_compose_project):
    """Return Pyxis container connection info."""
    return {
        "hostname": docker_compose_project["pyxis_hostname"],
        "port": docker_compose_project["pyxis_port"],
        "username": docker_compose_project["pyxis_username"],
        "password": docker_compose_project["pyxis_password"],
        "name": "slurm-test-pyxis",
        "runtime": "podman",  # or docker
    }
```

### Step 3: Update Tests

**Changes to `tests/integration/test_container_packaging_basic.py`**:

```python
@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_basic_container_task_execution(
    slurm_pyxis_cluster,
    local_registry,  # localhost:5000 for building
    registry_for_containers,  # registry:5000 for runtime
    tmp_path
):
    """Test that a task runs successfully in a container via Pyxis."""

    # Create a simple Dockerfile for this test
    dockerfile_path = tmp_path / "basic_test.Dockerfile"
    dockerfile_path.write_text("""FROM python:3.11-slim

WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .
""")

    # Define task with container packaging
    @task(
        time="00:02:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=str(dockerfile_path),
        packaging_registry=f"{registry_for_containers}/test/",  # Use service name!
        packaging_platform="linux/arm64",
        packaging_tls_verify=False,
    )
    def hello_container():
        import socket
        return f"Hello from {socket.gethostname()}"

    # Submit and verify
    with slurm_pyxis_cluster:
        job = slurm_pyxis_cluster.submit(hello_container)()
        assert job.wait(timeout=300), f"Job did not complete: {job.get_stderr()}"
        result = job.get_result()
        assert "Hello from" in result, f"Unexpected result: {result}"
```

### Step 4: Update SDK Installation Fixture

**Update `sdk_on_pyxis_cluster()` in conftest.py**:

```python
@pytest.fixture(scope="session")
def sdk_on_pyxis_cluster(docker_compose_project) -> Path:
    """Install the SDK on the Pyxis cluster container."""
    container_name = "slurm-test-pyxis"

    # Copy SDK to container using docker-compose
    sdk_root = Path(__file__).resolve().parents[2]

    # Create temp directory in container
    subprocess.run(
        ["docker-compose", "-f", str(COMPOSE_FILE), "exec", "-T", container_name,
         "mkdir", "-p", "/tmp/slurm_sdk"],
        check=True
    )

    # Copy files using tar pipe
    tar_cmd = f"tar -cf - -C {sdk_root} src pyproject.toml README.md | docker-compose -f {COMPOSE_FILE} exec -T {container_name} tar -xf - -C /tmp/slurm_sdk"
    subprocess.run(tar_cmd, shell=True, check=True)

    # Install SDK
    subprocess.run(
        ["docker-compose", "-f", str(COMPOSE_FILE), "exec", "-T", container_name,
         "python3", "-m", "pip", "install", "-e", "/tmp/slurm_sdk",
         "--break-system-packages", "--quiet"],
        check=True
    )

    return Path("/tmp/slurm_sdk")
```

### Step 5: Update Configuration Fixture

**Update `slurm_pyxis_cluster_config()` in conftest.py**:

```python
@pytest.fixture
def slurm_pyxis_cluster_config(docker_compose_project, sdk_on_pyxis_cluster) -> Dict:
    """Provides configuration dict for Cluster.from_env()."""
    return {
        "backend": "ssh",
        "hostname": docker_compose_project["pyxis_hostname"],
        "port": docker_compose_project["pyxis_port"],
        "username": docker_compose_project["pyxis_username"],
        "password": docker_compose_project["pyxis_password"],
        "job_base_dir": "/home/slurm/slurm_jobs",
        "partition": "debug",
    }
```

## Benefits

### 1. **Networking Fixed** ✅
- Registry accessible as `registry:5000` from Pyxis container
- No more localhost confusion
- Automatic DNS resolution

### 2. **Simplified Code** ✅
- ~200 lines of Python fixture code → ~50 lines YAML
- Declarative vs imperative
- Easier to understand

### 3. **Better Developer Experience** ✅
- Can manually start services: `docker-compose up -d`
- Can inspect services: `docker-compose ps`, `docker-compose logs`
- Can shell into containers: `docker-compose exec slurm-pyxis bash`
- Standard tooling everyone knows

### 4. **Reproducible** ✅
- Same setup on all machines
- Version-controlled configuration
- No hidden state

### 5. **Extensible** ✅
- Easy to add more services (e.g., database, monitoring)
- Can override with docker-compose.override.yml
- Environment-specific configurations

## Migration Steps

1. ✅ Review this plan
2. Create `tests/integration/docker-compose.yml`
3. Create new fixtures using docker-compose
4. Update tests to use `registry_for_containers` fixture
5. Remove old fixture code
6. Test with: `pytest tests/integration/test_container_packaging_basic.py -v`
7. Update documentation
8. Mark Phase 4.5 as complete

## Compatibility

- ✅ Works with Podman (via podman-compose or docker-compose with podman socket)
- ✅ Works with Docker
- ✅ Existing non-container tests unaffected
- ✅ Can run manually outside pytest for debugging

## Testing the Changes

```bash
# Manual testing
cd tests/integration
docker-compose up -d --build
docker-compose ps
docker-compose logs slurm-pyxis
docker-compose exec slurm-pyxis sinfo
docker-compose exec slurm-pyxis enroot version
docker-compose down -v

# Pytest testing
pytest tests/integration/test_container_packaging_basic.py -v
```

## Rollback Plan

If issues arise:
1. Keep old fixtures commented out in conftest.py
2. Can quickly revert to old approach
3. docker-compose.yml is isolated, doesn't affect other code

## Success Criteria

- [ ] docker-compose.yml created and working
- [ ] Fixtures refactored to use docker-compose
- [ ] Tests updated to use `registry_for_containers`
- [ ] Both container packaging tests pass
- [ ] Existing Phase 1-3 tests still pass
- [ ] Documentation updated
- [ ] Can run services manually with docker-compose

## Files to Change

**New Files**:
- `tests/integration/docker-compose.yml` (~70 lines)

**Modified Files**:
- `tests/integration/conftest.py` (simplify, ~150 lines removed, ~100 added)
- `tests/integration/test_container_packaging_basic.py` (add `registry_for_containers` parameter)

**Unchanged**:
- All SDK source code
- Phase 1-3 tests
- Container Dockerfile

## Estimated Effort

- Implementation: 1-2 hours
- Testing: 30 minutes
- Documentation: 15 minutes
- **Total: ~2-3 hours**

## Questions to Consider

1. Should we use `docker-compose` or `podman-compose` command?
   - **Recommendation**: Use `docker-compose` with DOCKER_HOST=unix:///run/podman/podman.sock

2. Should we auto-build the Pyxis image or use pre-built?
   - **Recommendation**: Auto-build for flexibility (as shown in plan)

3. Should we use named volumes or bind mounts for SDK?
   - **Recommendation**: Named volumes (as shown) - cleaner, more portable

4. Should we expose enroot's registry config?
   - **Recommendation**: Not initially - default config should work with `registry:5000`
