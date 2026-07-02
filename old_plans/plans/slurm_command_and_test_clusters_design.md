# Slurm CLI Command & Test Clusters Design

This document outlines the design for a new `slurm` CLI command and supporting infrastructure that enables SDK users to easily create and manage test Slurm clusters for integration testing their code.

---

## Motivation

Testing Slurm workloads has historically been painful:

1. **No local testing** - Developers must deploy to real clusters to test, leading to slow iteration
2. **Complex setup** - Setting up a local Slurm cluster requires significant infrastructure knowledge
3. **No GPU simulation** - Testing GPU workloads requires actual GPU hardware
4. **Fragile tests** - Integration tests that depend on external clusters are unreliable

The Slurm SDK already has internal infrastructure for running integration tests against a containerized Slurm cluster. This design proposes generalizing that infrastructure into:

1. A `slurm` CLI command for cluster lifecycle management
2. A `slurm.testing` package providing pytest fixtures and utilities
3. Configurable cluster templates (CPU-only, GPU, multi-node, etc.)

---

## Goals

1. **Zero-config testing** - `slurm cluster create && pytest` should just work
2. **Realistic environments** - Clusters should behave like real Slurm installations
3. **GPU simulation** - Support fake GPU resources for testing GPU-aware code
4. **Flexible configurations** - Support single-node, multi-node, and mixed CPU/GPU setups
5. **CI-friendly** - Work seamlessly in CI pipelines (GitHub Actions, GitLab CI, etc.)
6. **Pytest integration** - First-class pytest fixture support via `slurm.testing`

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           User's Project                                 │
│                                                                         │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │   @task code     │    │   @workflow code │    │   conftest.py    │  │
│  │                  │    │                  │    │                  │  │
│  │  from slurm ...  │    │  from slurm ...  │    │  from slurm.     │  │
│  │                  │    │                  │    │  testing import  │  │
│  │                  │    │                  │    │  slurm_cluster   │  │
│  └──────────────────┘    └──────────────────┘    └────────┬─────────┘  │
│                                                           │            │
└───────────────────────────────────────────────────────────┼────────────┘
                                                            │
                                                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         slurm.testing Package                           │
│                                                                         │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  TestCluster     │    │  Pytest Fixtures │    │  Mock Backends   │  │
│  │                  │    │                  │    │                  │  │
│  │  .create()       │◄───│  slurm_cluster   │    │  MockBackend     │  │
│  │  .start()        │    │  slurm_config    │    │  LocalBackend    │  │
│  │  .stop()         │    │                  │    │                  │  │
│  │  .destroy()      │    │                  │    │                  │  │
│  └────────┬─────────┘    └──────────────────┘    └──────────────────┘  │
│           │                                                             │
└───────────┼─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         slurm CLI Command                               │
│                                                                         │
│  $ slurm cluster create [--preset cpu|gpu|multi-node] [--nodes N]       │
│  $ slurm cluster start                                                  │
│  $ slurm cluster stop                                                   │
│  $ slurm cluster destroy                                                │
│  $ slurm cluster status                                                 │
│  $ slurm cluster list                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Container Infrastructure                              │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    docker-compose.yml (generated)                │   │
│  │                                                                  │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │   │
│  │  │ slurmctld│  │ slurmd-0 │  │ slurmd-1 │  │ registry         │ │   │
│  │  │ (head)   │  │ (cpu)    │  │ (gpu)    │  │ (container imgs) │ │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ │   │
│  │                                                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Container Images:                                                      │
│  - slurm-test-controller: slurmctld, munge, ssh                        │
│  - slurm-test-compute: slurmd, pyxis/enroot, podman                    │
│  - slurm-test-compute-gpu: slurmd + fake GPU support                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## CLI Command Design

### Command Structure

```
slurm
├── cluster                    # Test cluster management
│   ├── create                 # Create a new test cluster
│   ├── start                  # Start a stopped cluster
│   ├── stop                   # Stop a running cluster
│   ├── destroy                # Remove cluster completely
│   ├── status                 # Show cluster status
│   ├── list                   # List all clusters
│   ├── logs                   # View cluster logs
│   └── ssh                    # SSH into cluster node
│
├── init                       # Initialize project with Slurmfile
│
└── run                        # (future) Run a task locally
```

### `slurm cluster create`

Creates a new test cluster with configurable topology.

```bash
# Basic usage - single node CPU cluster
slurm cluster create

# Named cluster with preset
slurm cluster create --name my-cluster --preset gpu

# Custom configuration
slurm cluster create \
  --name dev-cluster \
  --cpu-nodes 2 \
  --gpu-nodes 1 \
  --gpus-per-node 4 \
  --cpus-per-node 8 \
  --memory-per-node 16384

# With container registry
slurm cluster create --preset gpu --with-registry

# Start immediately
slurm cluster create --start
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--name` | `slurm-test` | Cluster name (used in container names) |
| `--preset` | `cpu` | Preset configuration: `cpu`, `gpu`, `multi-node`, `full` |
| `--cpu-nodes` | 1 | Number of CPU-only compute nodes |
| `--gpu-nodes` | 0 | Number of GPU compute nodes |
| `--cpus-per-node` | 4 | CPUs per compute node |
| `--gpus-per-node` | 2 | GPUs per GPU node |
| `--memory-per-node` | 4096 | Memory per node (MB) |
| `--with-registry` | false | Include container registry service |
| `--with-pyxis` | false | Include Pyxis/enroot for container jobs |
| `--port` | 2222 | SSH port mapping on host |
| `--start` | false | Start cluster after creation |
| `--output-dir` | `.slurm-cluster/` | Directory for generated files |

**Presets:**

| Preset | Description |
|--------|-------------|
| `cpu` | Single node, 4 CPUs, 4GB RAM, no GPU |
| `gpu` | Single node, 4 CPUs, 2 fake GPUs, 8GB RAM |
| `multi-node` | 1 head + 2 compute nodes, CPU only |
| `full` | 1 head + 2 CPU + 1 GPU node, with registry and Pyxis |

### `slurm cluster start/stop/destroy`

```bash
# Start the default cluster
slurm cluster start

# Start a named cluster
slurm cluster start --name my-cluster

# Stop without destroying
slurm cluster stop

# Destroy completely (removes volumes)
slurm cluster destroy --name my-cluster
```

### `slurm cluster status`

```bash
$ slurm cluster status

Cluster: slurm-test
Status:  Running
Uptime:  2h 15m

Nodes:
  NAME          STATE   CPUS   GPUS   MEMORY   PARTITION
  controller    up      2      -      2048     -
  compute-0     idle    4      -      4096     cpu
  compute-1     idle    4      2      8192     gpu

Partitions:
  NAME    NODES       STATE   DEFAULT
  cpu     compute-0   up      yes
  gpu     compute-1   up      no

Connection:
  SSH:      ssh slurm@localhost -p 2222
  Password: slurm

Registry: localhost:5000 (if enabled)
```

### `slurm cluster ssh`

```bash
# SSH to head node
slurm cluster ssh

# SSH to specific node
slurm cluster ssh compute-0

# Run command
slurm cluster ssh -- sinfo
```

---

## Cluster Configuration Presets

### Preset: `cpu` (Default)

Single-node cluster for basic testing.

```yaml
# Generated docker-compose.yml structure
nodes:
  - name: slurm
    role: all-in-one
    cpus: 4
    memory: 4096
    partitions: [debug]

partitions:
  - name: debug
    default: true
    max_time: "1:00:00"
```

### Preset: `gpu`

Single-node with simulated GPU resources.

```yaml
nodes:
  - name: slurm
    role: all-in-one
    cpus: 4
    memory: 8192
    gpus: 2
    gpu_type: "fake"  # Uses SLURM's fake GPU support
    partitions: [debug, gpu]

partitions:
  - name: debug
    default: true
    max_time: "1:00:00"
  - name: gpu
    default: false
    features: [gpu]
```

### Preset: `multi-node`

Separate controller and compute nodes.

```yaml
nodes:
  - name: controller
    role: controller
    cpus: 2
    memory: 2048

  - name: compute-0
    role: compute
    cpus: 4
    memory: 4096
    partitions: [cpu]

  - name: compute-1
    role: compute
    cpus: 4
    memory: 4096
    partitions: [cpu]

partitions:
  - name: cpu
    default: true
    nodes: [compute-0, compute-1]
```

### Preset: `full`

Production-like setup with CPU and GPU partitions.

```yaml
nodes:
  - name: controller
    role: controller
    cpus: 2
    memory: 2048

  - name: cpu-0
    role: compute
    cpus: 8
    memory: 8192
    partitions: [cpu]

  - name: cpu-1
    role: compute
    cpus: 8
    memory: 8192
    partitions: [cpu]

  - name: gpu-0
    role: compute
    cpus: 8
    memory: 16384
    gpus: 4
    gpu_type: "fake"
    partitions: [gpu]

partitions:
  - name: cpu
    default: true
    nodes: [cpu-0, cpu-1]
  - name: gpu
    default: false
    nodes: [gpu-0]
    features: [gpu]

services:
  registry: true
  pyxis: true
```

---

## GPU Simulation

Test clusters can simulate GPU resources using Slurm's GRES (Generic RESource) system with fake GPUs.

### How It Works

1. **slurm.conf configuration:**
   ```
   GresTypes=gpu
   NodeName=gpu-0 ... Gres=gpu:fake:4
   ```

2. **gres.conf:**
   ```
   NodeName=gpu-0 Name=gpu Type=fake Count=4
   ```

3. **Job submission works normally:**
   ```python
   @task(gpus=2)
   def train_model():
       # SLURM_GPUS_ON_NODE=2 is set
       # CUDA_VISIBLE_DEVICES is NOT set (no real GPUs)
       pass
   ```

### GPU Environment Variables

The fake GPU setup provides these environment variables:

| Variable | Value | Description |
|----------|-------|-------------|
| `SLURM_GPUS_ON_NODE` | N | Number of GPUs allocated |
| `SLURM_GPUS` | N | Total GPUs for job |
| `SLURM_JOB_GPUS` | 0,1,... | GPU indices (fake) |

This allows testing GPU-aware code paths without actual hardware.

---

## `slurm.testing` Package Design

The `slurm.testing` package provides testing utilities for SDK users.

### Package Structure

```
src/slurm/testing/
├── __init__.py           # Public API exports
├── cluster.py            # TestCluster class
├── fixtures.py           # Pytest fixtures
├── backends.py           # Mock and local backends
├── config.py             # Test configuration helpers
├── containers/           # Container image definitions
│   ├── controller/
│   │   └── Containerfile
│   ├── compute/
│   │   └── Containerfile
│   └── compute-gpu/
│       └── Containerfile
└── templates/            # Jinja2 templates
    ├── docker-compose.yml.j2
    ├── slurm.conf.j2
    └── gres.conf.j2
```

### TestCluster Class

```python
from slurm.testing import TestCluster

# Programmatic cluster management
cluster = TestCluster(
    name="my-test-cluster",
    preset="gpu",
    gpu_nodes=1,
    gpus_per_node=2,
)

# Create and start
cluster.create()
cluster.start()

# Get connection info
config = cluster.get_config()
# Returns: {"hostname": "localhost", "port": 2222, ...}

# Use with SDK
from slurm import Cluster
slurm = Cluster.from_config(config)

# Cleanup
cluster.stop()
cluster.destroy()

# Context manager support
with TestCluster(preset="cpu") as cluster:
    slurm = Cluster.from_config(cluster.get_config())
    job = slurm.submit(my_task)()
    result = job.wait()
```

### Pytest Fixtures

```python
# conftest.py in user's project
from slurm.testing import slurm_cluster, slurm_cluster_config

# That's it! The fixtures are now available.
```

```python
# test_my_tasks.py
import pytest
from slurm import Cluster
from my_project import process_data

@pytest.mark.integration_test
def test_process_data(slurm_cluster):
    """Test with a real Slurm cluster."""
    job = slurm_cluster.submit(process_data)(input_file="data.csv")
    result = job.wait(timeout=60)
    assert result["status"] == "success"

@pytest.mark.integration_test
def test_gpu_training(slurm_cluster_gpu):
    """Test with GPU cluster."""
    job = slurm_cluster_gpu.submit(train_model)(epochs=1)
    result = job.wait()
    assert "model" in result
```

### Available Fixtures

| Fixture | Description |
|---------|-------------|
| `slurm_cluster` | CPU-only single-node cluster |
| `slurm_cluster_gpu` | Single-node with fake GPUs |
| `slurm_cluster_multi` | Multi-node CPU cluster |
| `slurm_cluster_full` | Full setup with GPU partition and registry |
| `slurm_cluster_config` | Just the cluster config (no cluster started) |
| `slurm_registry` | Container registry URL |

### Mock Backend for Unit Tests

```python
from slurm.testing import MockBackend, mock_cluster

# For pure unit tests (no containers)
def test_task_definition():
    """Test task without running anything."""
    with mock_cluster() as cluster:
        job = cluster.submit(my_task)(x=1, y=2)

        # Job is "submitted" but not executed
        assert job.job_id == "mock-1"

        # Can inspect rendered script
        assert "#!/bin/bash" in job.script
        assert "#SBATCH --cpus-per-task=4" in job.script
```

### Configuration Helpers

```python
from slurm.testing import (
    default_test_config,
    gpu_test_config,
    create_slurmfile,
)

# Get a test configuration dict
config = default_test_config()
config = gpu_test_config(gpus=4)

# Create a temporary Slurmfile for testing
with create_slurmfile(config) as slurmfile_path:
    cluster = Cluster.from_slurmfile(slurmfile_path)
```

---

## Container Images

### Base Image: `slurm-test-base`

Common base for all test cluster images.

- Debian 12 slim
- Slurm packages (slurmctld, slurmd, slurm-client)
- Munge authentication
- SSH server
- Python 3.11+
- Basic utilities

### Controller Image: `slurm-test-controller`

Extends base with:
- slurmctld service
- Accounting (optional)
- Configured as head node

### Compute Image: `slurm-test-compute`

Extends base with:
- slurmd service
- Pyxis/enroot (optional)
- Podman for container builds
- Configured as compute node

### GPU Compute Image: `slurm-test-compute-gpu`

Extends compute with:
- Fake GPU GRES configuration
- nvidia-container-toolkit stubs (for compatibility)
- GPU-related environment setup

---

## Generated Files

When `slurm cluster create` runs, it generates:

```
.slurm-cluster/
├── docker-compose.yml      # Main compose file
├── config/
│   ├── slurm.conf          # Slurm configuration
│   ├── gres.conf           # GPU resources (if applicable)
│   ├── cgroup.conf         # Cgroup configuration
│   └── plugstack.conf      # Pyxis configuration (if applicable)
├── cluster.yaml            # Cluster definition (for recreation)
└── .env                    # Environment variables
```

Users can inspect and modify these files for custom configurations.

---

## Integration with Existing SDK

### Cluster.from_test_cluster()

```python
from slurm import Cluster
from slurm.testing import TestCluster

# Create test cluster
test_cluster = TestCluster(preset="gpu")
test_cluster.start()

# Connect SDK cluster to test cluster
cluster = Cluster.from_test_cluster(test_cluster)

# Or use convenience method
cluster = Cluster.for_testing(preset="gpu")
```

### Environment-Based Configuration

```python
# Automatically detect and use test cluster if available
cluster = Cluster.from_env()

# This checks:
# 1. SLURM_TEST_CLUSTER environment variable
# 2. .slurm-cluster/ directory in current/parent directories
# 3. Slurmfile in current directory
```

---

## CI Integration

### GitHub Actions Example

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install slurm-sdk[testing]
          pip install -e .

      - name: Create test cluster
        run: slurm cluster create --preset cpu --start

      - name: Run tests
        run: pytest --run-integration tests/

      - name: Cleanup
        if: always()
        run: slurm cluster destroy
```

### GitLab CI Example

```yaml
# .gitlab-ci.yml
test:
  image: python:3.11
  services:
    - docker:dind
  variables:
    DOCKER_HOST: tcp://docker:2375
  script:
    - pip install slurm-sdk[testing]
    - slurm cluster create --preset cpu --start
    - pytest --run-integration tests/
  after_script:
    - slurm cluster destroy
```

---

## Migration Path for SDK Internal Tests

The SDK's current test infrastructure will be migrated to use `slurm.testing`:

### Current State

```
tests/integration/
├── conftest.py          # Complex fixtures, docker-compose management
├── docker-compose.yml   # Symlink to containers/docker-compose.yml
└── test_*.py

containers/
├── docker-compose.yml   # Unified compose
└── slurm-pyxis-integration/
    ├── Containerfile
    └── slurm.conf
```

### After Migration

```
src/slurm/testing/
├── __init__.py
├── cluster.py           # TestCluster (moved from conftest logic)
├── fixtures.py          # Pytest fixtures (extracted from conftest)
├── backends.py          # Mock backends
└── containers/          # Container definitions (moved from containers/)

tests/integration/
├── conftest.py          # Simple: just import slurm.testing fixtures
└── test_*.py            # Unchanged
```

---

## Engineering Plan

### Phase 1: CLI Infrastructure (Week 1)

**Goal:** Basic `slurm` CLI with cluster subcommands.

**Tasks:**
1. Create CLI entry point using Click or Typer
2. Implement `slurm cluster create` with basic presets
3. Implement `slurm cluster start/stop/destroy`
4. Implement `slurm cluster status/list`
5. Add `slurm cluster ssh` for debugging
6. Generate docker-compose.yml from templates
7. Add to pyproject.toml as console script

**Deliverables:**
- `slurm cluster create --preset cpu` creates and starts a cluster
- `slurm cluster status` shows running clusters
- `slurm cluster destroy` cleans up

**Milestone:** User can create a test cluster with one command.

---

### Phase 2: Container Images & Templates (Week 1-2)

**Goal:** Production-quality container images for test clusters.

**Tasks:**
1. Refactor existing Containerfile into modular base/controller/compute
2. Create Jinja2 templates for slurm.conf generation
3. Create docker-compose.yml.j2 template with multi-node support
4. Implement preset configurations (cpu, gpu, multi-node, full)
5. Add container registry service support
6. Add Pyxis/enroot support as optional feature

**Deliverables:**
- All preset configurations working
- Multi-node clusters functional
- Container registry integration working

**Milestone:** All cluster presets (`cpu`, `gpu`, `multi-node`, `full`) working.

---

### Phase 3: slurm.testing Package (Week 2)

**Goal:** Extract test utilities into reusable package.

**Tasks:**
1. Create `slurm.testing` package structure
2. Implement `TestCluster` class with full lifecycle management
3. Extract pytest fixtures from SDK's conftest.py
4. Implement mock backend for unit tests
5. Add configuration helper functions
6. Write comprehensive docstrings and type hints

**Deliverables:**
- `from slurm.testing import slurm_cluster` works
- TestCluster context manager works
- Mock backend for unit tests works

**Milestone:** SDK's own tests migrated to use `slurm.testing`.

---

### Phase 4: GPU Simulation (Week 2-3)

**Goal:** Fake GPU support for testing GPU-aware code.

**Tasks:**
1. Implement GRES configuration for fake GPUs
2. Create GPU compute node container image
3. Configure GPU partition in multi-node setups
4. Test GPU job submission and environment variables
5. Document GPU simulation limitations

**Deliverables:**
- `slurm cluster create --preset gpu` provides fake GPUs
- Jobs can request GPUs via `--gpus` or `@task(gpus=N)`
- `SLURM_GPUS_ON_NODE` and related vars set correctly

**Milestone:** GPU-aware tasks can be tested without real GPUs.

---

### Phase 5: Documentation & Examples (Week 3)

**Goal:** Comprehensive documentation and examples.

**Tasks:**
1. Write CLI reference documentation
2. Write `slurm.testing` API documentation
3. Create tutorial: "Testing Your Slurm Code"
4. Create example project with tests
5. Add troubleshooting guide
6. Update SDK documentation with testing section

**Deliverables:**
- Complete CLI reference in docs/
- Complete API reference for slurm.testing
- Example project in examples/testing-example/

**Milestone:** Documentation complete, ready for users.

---

### Phase 6: CI Templates & Polish (Week 3)

**Goal:** Make CI integration trivial.

**Tasks:**
1. Create GitHub Actions reusable workflow
2. Create GitLab CI template
3. Add `slurm cluster create --ci` optimizations
4. Performance optimizations (parallel container builds, caching)
5. Error messages and user experience polish
6. Release preparation

**Deliverables:**
- Copy-paste GitHub Actions workflow
- Copy-paste GitLab CI configuration
- Polished error messages and help text

**Milestone:** Ready for public release.

---

## Summary

| Phase | Description | Duration | Dependencies |
|-------|-------------|----------|--------------|
| 1 | CLI Infrastructure | 1 week | None |
| 2 | Container Images & Templates | 1 week | Phase 1 |
| 3 | slurm.testing Package | 1 week | Phase 2 |
| 4 | GPU Simulation | 0.5 week | Phase 2 |
| 5 | Documentation & Examples | 0.5 week | Phase 3, 4 |
| 6 | CI Templates & Polish | 0.5 week | Phase 5 |

**Total estimated time:** 4-5 weeks

---

## Open Questions

1. **CLI Framework:** Click vs Typer vs argparse?
   - Recommendation: Typer (modern, good UX, type hints)

2. **Cluster state storage:** Where to store cluster metadata?
   - Recommendation: `.slurm-cluster/cluster.yaml` in project directory

3. **Multiple clusters:** Support running multiple clusters simultaneously?
   - Recommendation: Yes, via `--name` option with unique port mappings

4. **Windows support:** WSL2 only or native Docker Desktop?
   - Recommendation: WSL2 initially, Docker Desktop as stretch goal

5. **Resource limits:** Should we enforce memory/CPU limits in containers?
   - Recommendation: Yes, to catch resource-related bugs early

---

## Appendix: Example User Workflow

### New User Getting Started

```bash
# Install SDK with testing support
pip install slurm-sdk[testing]

# Create a simple task
cat > my_task.py << 'EOF'
from slurm import task

@task(cpus=2, memory="1G")
def hello():
    return "Hello from Slurm!"
EOF

# Create and start test cluster
slurm cluster create --start

# Test interactively
python -c "
from slurm import Cluster
from my_task import hello

cluster = Cluster.for_testing()
job = cluster.submit(hello)()
print(job.wait())  # Hello from Slurm!
"

# Or write proper tests
cat > test_my_task.py << 'EOF'
from slurm.testing import slurm_cluster
from my_task import hello

def test_hello(slurm_cluster):
    job = slurm_cluster.submit(hello)()
    assert job.wait() == "Hello from Slurm!"
EOF

# Run tests
pytest --run-integration test_my_task.py

# Cleanup when done
slurm cluster destroy
```

### CI Pipeline

```bash
# In CI, everything is automated
slurm cluster create --preset cpu --start
pytest --run-integration
slurm cluster destroy
```

This workflow is simple, discoverable, and mirrors how real Slurm clusters work.
