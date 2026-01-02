# Container Packaging Integration Test Design

## Overview

Add comprehensive integration tests for container packaging using Pyxis and enroot, without disrupting existing integration tests. This will enable testing of real container-based task execution in a Slurm environment.

## Goals

1. **Test Container Packaging**: Verify container builds, pushes, and execution via Pyxis/enroot
2. **Separate Infrastructure**: New container separate from existing `slurm-integration` container
3. **No Disruption**: Existing integration tests continue working unchanged
4. **Production-like**: Test with real container registry and image pulling
5. **Comprehensive Coverage**: Test both basic and advanced container packaging scenarios

## Architecture

### Current State

```
tests/integration/
├── conftest.py              # Fixtures for slurm-integration container
└── test_*.py                # Tests using basic Slurm (no containers)

containers/slurm-integration/
├── Containerfile            # Basic Slurm + SSH
└── slurm.conf              # Slurm configuration
```

### Target State

```
tests/integration/
├── conftest.py                          # Existing fixtures (unchanged)
├── conftest_container_packaging.py     # New fixtures for Pyxis/enroot tests
├── test_*.py                            # Existing tests (unchanged)
├── test_container_packaging_basic.py   # New: Basic container execution test
└── test_container_packaging_advanced.py # New: Advanced scenarios

containers/
├── slurm-integration/                   # Existing (unchanged)
│   ├── Containerfile
│   └── slurm.conf
└── slurm-pyxis-integration/            # New: Slurm + Pyxis + enroot
    ├── Containerfile
    ├── slurm.conf
    ├── install-enroot.sh               # enroot installation script
    └── install-pyxis.sh                # Pyxis installation script
```

## Design Decisions

### 1. Separate Container Image

**Decision**: Create new `slurm-pyxis-integration` container instead of modifying existing

**Rationale**:
- ✅ Zero risk to existing tests
- ✅ Cleaner separation of concerns
- ✅ Can iterate on Pyxis setup without breaking existing tests
- ✅ Different systemd services and dependencies
- ❌ Slight duplication (but manageable with base image if needed later)

**Alternative Considered**: Single container with optional Pyxis
- Rejected due to complexity and risk of breaking existing tests

### 2. Fixture Naming Strategy

**Decision**: Use separate conftest file with distinct fixture names

**Rationale**:
- ✅ No fixture name conflicts
- ✅ Clear which tests use which infrastructure
- ✅ Can run both test suites independently
- ✅ Easy to understand and maintain

**Fixtures**:
- `slurm_cluster` → existing basic cluster (unchanged)
- `slurm_pyxis_cluster` → new cluster with Pyxis/enroot support

### 3. Container Registry Strategy

**Decision**: Use local registry container for tests (no external dependencies)

**Rationale**:
- ✅ No external service dependencies
- ✅ Fast and reliable
- ✅ No authentication needed for tests
- ✅ Can be started/stopped with test infrastructure

**Implementation**: Start Docker registry container as part of pytest setup

### 4. Test Markers

**Decision**: Use dedicated pytest markers for container packaging tests

```python
@pytest.mark.container_packaging
@pytest.mark.integration_test
@pytest.mark.slow_integration_test
```

**Rationale**:
- ✅ Can run/skip container packaging tests independently
- ✅ Clear test categorization
- ✅ Supports CI filtering (run fast tests first, slow tests later)

## Technical Implementation

### Component 1: New Container Image

**Base**: Debian 12 (same as existing)

**Additional Software**:
1. **enroot** (v3.4.1+): Container runtime for HPC
   - Source: https://github.com/NVIDIA/enroot
   - Dependencies: squashfs-tools, parallel, curl, fuse-overlayfs

2. **Pyxis** (v0.18.0+): Slurm plugin for container support
   - Source: https://github.com/NVIDIA/pyxis
   - Integration: Slurm SPANK plugin

3. **Container runtime** (for building): Podman or Docker in the container
   - Used for building test images
   - Configured for rootless operation

**Directory Structure**:
```
/opt/enroot/           # enroot installation
/opt/pyxis/            # Pyxis plugin
/etc/slurm/plugstack.conf  # Pyxis plugin config
/var/lib/enroot/       # enroot data
/tmp/registry/         # Local container registry data
```

### Component 2: Local Registry Container

**Image**: `registry:2`

**Configuration**:
```yaml
Port: 5000
Storage: /tmp/registry
No authentication
```

**Lifecycle**: Started before tests, stopped after

### Component 3: New Fixtures (conftest_container_packaging.py)

```python
@pytest.fixture(scope="session")
def local_registry(request):
    """Start local Docker registry for tests."""
    # Start registry:2 container
    # Returns: "localhost:5000"

@pytest.fixture(scope="session")
def pyxis_container(request, local_registry):
    """Build and start slurm-pyxis-integration container."""
    # Build image with Pyxis/enroot
    # Start container with registry link
    # Wait for services (sshd, slurmctld, slurmd)
    # Returns: container info dict

@pytest.fixture(scope="session")
def sdk_on_pyxis_cluster(pyxis_container):
    """Install SDK on Pyxis cluster."""
    # Copy SDK source to container
    # Install in editable mode
    # Returns: SDK path

@pytest.fixture
def slurm_pyxis_cluster(pyxis_container, sdk_on_pyxis_cluster, tmp_path):
    """Create Cluster instance connected to Pyxis container."""
    # Create SSH backend
    # Configure to use local registry
    # Returns: Cluster instance
```

### Component 4: Test Cases

#### Test 1: Basic Container Execution (test_container_packaging_basic.py)

```python
@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_basic_container_task_execution(slurm_pyxis_cluster, local_registry):
    """Test that a task runs successfully in a container via Pyxis."""

    # Define task with container packaging
    @task(
        time="00:02:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=...,
        packaging_registry=f"{local_registry}/test/",
    )
    def hello_container():
        import socket
        return f"Hello from {socket.gethostname()}"

    # Submit and verify
    with slurm_pyxis_cluster:
        job = hello_container()
        assert job.wait(timeout=120)
        result = job.get_result()
        assert "Hello from" in result

@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_container_with_dependencies(slurm_pyxis_cluster, local_registry):
    """Test container task with Python dependencies."""

    @task(
        time="00:02:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile="test_container.Dockerfile",
        packaging_registry=f"{local_registry}/test/",
    )
    def numpy_task():
        import numpy as np
        return np.array([1, 2, 3]).sum()

    with slurm_pyxis_cluster:
        job = numpy_task()
        assert job.wait(timeout=180)  # Build + execute
        result = job.get_result()
        assert result == 6
```

#### Test 2: Advanced Scenarios (test_container_packaging_advanced.py)

```python
@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_array_job_with_containers(slurm_pyxis_cluster, local_registry):
    """Test array jobs using container packaging."""

    @task(
        time="00:01:00",
        mem="512M",
        packaging="container",
        packaging_dockerfile=...,
        packaging_registry=f"{local_registry}/test/",
    )
    def process_item(item: int) -> int:
        return item * 2

    with slurm_pyxis_cluster:
        items = [1, 2, 3, 4, 5]
        array_job = slurm_pyxis_cluster.map(process_item, items)
        results = array_job.get_results(timeout=180)
        assert results == [2, 4, 6, 8, 10]

@pytest.mark.container_packaging
@pytest.mark.integration_test
def test_container_with_mounts(slurm_pyxis_cluster, local_registry, tmp_path):
    """Test container with volume mounts."""

    # Create test data
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "input.txt").write_text("test data")

    @task(
        time="00:02:00",
        mem="1G",
        packaging="container",
        packaging_dockerfile=...,
        packaging_registry=f"{local_registry}/test/",
        packaging_mounts=[f"{data_dir}:/data:ro"],
    )
    def read_mounted_file():
        with open("/data/input.txt") as f:
            return f.read()

    with slurm_pyxis_cluster:
        job = read_mounted_file()
        assert job.wait(timeout=120)
        result = job.get_result()
        assert result == "test data"
```

## Installation Scripts

### install-enroot.sh

```bash
#!/bin/bash
set -e

ENROOT_VERSION="3.4.1"

# Install dependencies
apt-get update
apt-get install -y \
    squashfs-tools \
    fuse-overlayfs \
    parallel \
    curl \
    jq

# Download and install enroot
cd /tmp
curl -fSsL -O "https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot_${ENROOT_VERSION}-1_amd64.deb"
curl -fSsL -O "https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot+caps_${ENROOT_VERSION}-1_amd64.deb"

dpkg -i enroot_${ENROOT_VERSION}-1_amd64.deb
dpkg -i enroot+caps_${ENROOT_VERSION}-1_amd64.deb

# Configure enroot
mkdir -p /etc/enroot
cat > /etc/enroot/enroot.conf << 'EOF'
ENROOT_RUNTIME_PATH /run/enroot/user-$(id -u)
ENROOT_CACHE_PATH /var/lib/enroot/cache/user-$(id -u)
ENROOT_DATA_PATH /var/lib/enroot/data/user-$(id -u)
ENROOT_TEMP_PATH /tmp
EOF

mkdir -p /var/lib/enroot
chmod 1777 /var/lib/enroot

# Cleanup
rm -f /tmp/enroot*.deb
```

### install-pyxis.sh

```bash
#!/bin/bash
set -e

PYXIS_VERSION="0.18.0"

# Install build dependencies
apt-get update
apt-get install -y \
    git \
    gcc \
    make \
    libslurm-dev

# Clone and build Pyxis
cd /tmp
git clone --depth 1 --branch "v${PYXIS_VERSION}" https://github.com/NVIDIA/pyxis.git
cd pyxis

make
make install

# Configure Pyxis SPANK plugin
mkdir -p /etc/slurm
cat > /etc/slurm/plugstack.conf << 'EOF'
include /usr/local/share/pyxis/pyxis.conf
EOF

# Cleanup
cd /
rm -rf /tmp/pyxis
```

## Implementation Plan

### Phase 1: Infrastructure Setup ✅ COMPLETED
- [x] Create `containers/slurm-pyxis-integration/` directory
- [x] Write `install-enroot.sh` script (with ARM64/AMD64 architecture detection)
- [x] Write `install-pyxis.sh` script (builds from source v0.19.0)
- [x] Create `Containerfile` based on existing slurm-integration
- [x] Add enroot and Pyxis installation to Containerfile
- [x] Copy and adapt `slurm.conf` for Pyxis
- [x] Test container builds successfully (690 MB image, enroot 3.4.1, Pyxis 0.19.0 installed)

### Phase 2: Registry Setup ✅ COMPLETED
- [x] Add local registry fixture to conftest (registry:2 container on localhost:5000)
- [x] Test registry starts and is accessible (HTTP-only, use --tls-verify=false)
- [x] Verify container can push/pull from registry (tests pass)
- [x] Document registry configuration (fixtures documented in conftest.py)

### Phase 3: Fixtures Implementation ✅ COMPLETED
- [x] Add fixtures to `tests/integration/conftest.py` (with PYXIS_ prefix for constants)
- [x] Implement `local_registry` fixture (already done in Phase 2)
- [x] Implement `pyxis_container` fixture (starts container with SSH port mapping)
- [x] Implement `sdk_on_pyxis_cluster` fixture (copies SDK and installs with pip)
- [x] Implement `slurm_pyxis_cluster_config` fixture (creates config dict)
- [x] Implement `slurm_pyxis_cluster` fixture (creates Cluster instance)
- [x] Add pytest markers for container packaging tests (already done in Phase 2)
- [x] Test fixtures work end-to-end (4/4 tests passing in test_pyxis_cluster_fixture.py)

### Phase 4: Basic Test Implementation ⚠️ PARTIAL
- [x] Create `tests/integration/test_container_packaging_basic.py` (2 tests created)
- [x] Create test Dockerfiles (generated dynamically in tests)
- [x] Implement `test_basic_container_task_execution`
- [x] Implement `test_container_with_dependencies`
- [x] Fix ARM64 platform issue (added `packaging_platform` parameter)
- [x] Add TLS verification disable support (`packaging_tls_verify` parameter)
- [x] Container build and push working correctly
- [ ] **BLOCKED**: Resolve container networking for registry access (see notes below)

**Status**: Core SDK functionality implemented and working. Container build/push succeeds. Job submission works but times out due to networking limitation.

**Networking Issue**:
- Registry runs on host at `localhost:5000`
- Pyxis container cannot access host's `localhost:5000` (refers to itself)
- Attempted solutions:
  - `--network=host`: Port conflicts with SSH
  - `--add-host=localregistry:host-gateway`: Requires host to also resolve `localregistry`
- **Resolution needed**: Either shared podman network or skip these tests on local dev machines

### Phase 5: Advanced Test Implementation ✓
- [ ] Create `tests/integration/test_container_packaging_advanced.py`
- [ ] Implement `test_array_job_with_containers`
- [ ] Implement `test_container_with_mounts`
- [ ] Run tests and verify they pass
- [ ] Fix any issues

### Phase 6: Documentation & Cleanup ✓
- [ ] Update main README with container packaging test info
- [ ] Add documentation to conftest_container_packaging.py
- [ ] Add troubleshooting guide
- [ ] Ensure existing tests still pass
- [ ] Run full test suite
- [ ] Clean up temporary code/comments

## Testing the Implementation

### Verification Steps

1. **Existing tests still work**:
   ```bash
   pytest tests/integration/test_*.py -v --ignore=tests/integration/test_container_packaging*.py
   ```

2. **New container builds**:
   ```bash
   cd containers/slurm-pyxis-integration
   podman build -t slurm-pyxis-integration:test .
   ```

3. **Registry works**:
   ```bash
   pytest tests/integration/conftest_container_packaging.py::test_local_registry -v
   ```

4. **Pyxis cluster starts**:
   ```bash
   pytest tests/integration/conftest_container_packaging.py::test_pyxis_container -v
   ```

5. **Basic tests pass**:
   ```bash
   pytest tests/integration/test_container_packaging_basic.py -v
   ```

6. **Advanced tests pass**:
   ```bash
   pytest tests/integration/test_container_packaging_advanced.py -v
   ```

## Success Criteria

- [ ] All existing integration tests pass unchanged
- [ ] New Pyxis container builds successfully
- [ ] Local registry starts and is accessible
- [ ] At least 2 new integration tests pass
- [ ] Tests verify container build, push, and execution
- [ ] Can run existing and new tests independently
- [ ] Documentation is complete
- [ ] No regression in existing functionality

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Pyxis/enroot installation complex | High | Use well-tested installation scripts from official repos |
| Container build takes too long | Medium | Cache layers, use efficient base image |
| Registry authentication issues | Medium | Use unauthenticated local registry for tests |
| Fixture conflicts | High | Use separate conftest file with unique names |
| Breaking existing tests | Critical | Never modify existing fixtures or containers |
| Systemd issues in container | Medium | Use same proven approach as existing container |

## Future Enhancements

After initial implementation:

1. **GPU Support**: Add NVIDIA GPU support to test GPU containers
2. **Multi-node**: Test container execution across multiple nodes
3. **Performance**: Optimize container build caching
4. **CI Integration**: Add to CI pipeline with appropriate timeouts
5. **Digest Testing**: Add tests specifically for digest-based references
6. **Build Secrets**: Test container builds with secrets

## References

- Pyxis: https://github.com/NVIDIA/pyxis
- enroot: https://github.com/NVIDIA/enroot
- Docker Registry: https://docs.docker.com/registry/
- Slurm SPANK Plugins: https://slurm.schedmd.com/spank.html
- Current Integration Tests: `designs/integration_test_design.md`
