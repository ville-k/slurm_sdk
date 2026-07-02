# Slurm SDK Code Analysis: Issues and Improvements

This document contains a prioritized list of potential bugs, dead code, and improvements identified in the Slurm SDK codebase.

---

## Critical Priority (Security & Data Integrity)

### 1. Pickle Deserialization Vulnerability
**Files:** `src/slurm/job.py:486`, `src/slurm/runner.py:368,435`

**Issue:** The SDK uses `pickle.load()` to deserialize task results and arguments from remote files. Pickle can execute arbitrary code during deserialization, making this a security risk if the cluster filesystem is compromised or in MITM scenarios.

**Impact:** Remote code execution on client machine.

**Fix:**
- Add signature verification for result files
- Consider using safer serialization (JSON with type hints, msgpack)
- At minimum, document the trust model clearly

---

### 2. Thread-Unsafe Status Cache in Job Class
**File:** `src/slurm/job.py:143-164`

**Issue:** `_status_cache` and `_status_cache_time` are modified without synchronization in `_update_status_cache()`, but `get_status()` can be called from multiple threads (including the `_JobStatusPoller` background thread).

```python
# job.py:160-164 - No lock protection
self._status_cache = status
self._status_cache_time = timestamp or time.time()
self._update_status_telemetry(status, self._status_cache_time)
if status.get("JobState") in self.TERMINAL_STATES:
    self._completed = True
```

**Impact:** Race conditions can cause inconsistent state reads.

**Fix:** Use `threading.Lock` to protect `_status_cache`, `_status_cache_time`, and `_completed`.

---

## High Priority (Functional Bugs)

### 3. SSH Connection Leak on SFTP Operations
**File:** `src/slurm/api/ssh.py:800-811,823-847`

**Issue:** `_get_sftp_client()` opens a new SFTP session each call but `upload_file()` closes it while other methods don't. The class maintains `self.sftp` but also creates new sessions.

```python
# ssh.py:807-808 - Creates new SFTP session
def _get_sftp_client(self):
    return self.client.open_sftp()  # New session each time

# ssh.py:845-847 - Only upload_file closes it
finally:
    if sftp:
        sftp.close()
```

**Impact:** SFTP connection exhaustion on long-running workflows.

**Fix:** Reuse the single `self.sftp` connection or implement proper connection pooling.

---

### 4. Double-Checked Locking Race in Completed Context Emission
**File:** `src/slurm/cluster.py:1827-1833` (referenced in exploration)

**Issue:** The `_emit_completed_context()` method uses a lock but mutates `Job` attributes (`finished_at`, `started_at`) after checking/emitting. If called concurrently from both `_JobStatusPoller.run()` and `Job.get_status()`, the timestamps can be overwritten.

**Impact:** Incorrect telemetry data (timestamps).

**Fix:** Move all Job attribute mutations inside the lock.

---

### 5. Result Filename Mismatch Risk
**Files:** `src/slurm/job.py:147-151`, `src/slurm/rendering.py:290-294`

**Issue:** Result filename is constructed in two places with different patterns:
- `job.py`: `f"slurm_job_{self.pre_submission_id}_{RESULT_FILENAME}"`
- `rendering.py`: `f"slurm_job_{pre_submission_id}_%a_{RESULT_FILENAME}"` (array) or similar

If these patterns diverge during refactoring, result files won't be found.

**Impact:** `get_result()` fails with FileNotFoundError.

**Fix:** Centralize result filename generation in a single utility function.

---

### 6. Missing Timeout on `recv_exit_status()` in `execute_command()`
**File:** `src/slurm/api/ssh.py:753-754`

**Issue:** While `_run_command()` sets channel timeouts, `execute_command()` doesn't:

```python
# ssh.py:753-754 - No timeout set
stdin, stdout, stderr = self.client.exec_command(command)
exit_status = stdout.channel.recv_exit_status()  # Can hang indefinitely
```

**Impact:** Workflow can hang if remote command stalls.

**Fix:** Apply the same timeout pattern from `_run_command()`.

---

### 7. Array Job Index Parsing Fragility
**File:** `src/slurm/array_job.py:283-292`

**Issue:** The regex for parsing array job IDs assumes specific format:

```python
match = re.match(r"(\d+)_?\[?(\d+)-(\d+)\]?", array_job_id)
```

This doesn't handle:
- `%` in array specs (e.g., `12345_[0-99%10]`)
- Step sizes (e.g., `12345_[0-99:2]`)

**Impact:** Incorrect Job object creation for complex array specs.

**Fix:** Parse the `array_spec` passed to `submit_job()` instead of parsing the returned ID.

---

## Medium Priority (Reliability Issues)

### 8. No Transaction Semantics for Job Submission
**File:** `src/slurm/cluster.py:1056-1099` (setup), `src/slurm/api/ssh.py:388-452` (submit)

**Issue:** Job directories are created before submission, but if `sbatch` fails, the directory remains as an orphan. No cleanup or rollback is performed.

**Impact:** Orphaned directories accumulate on cluster filesystem.

**Fix:** Implement cleanup on submission failure, or use a two-phase commit pattern.

---

### 9. Hardcoded Master Port in Runtime
**File:** `src/slurm/runtime.py` (search for `_DEFAULT_MASTER_PORT`)

**Issue:** The default master port (29500) is hardcoded. In multi-workflow scenarios or shared clusters, this can cause port conflicts.

**Impact:** PyTorch distributed training conflicts.

**Fix:** Make configurable via environment variable or Slurmfile.

---

### 10. Silent Failure in _JobStatusPoller Error Handling
**File:** `src/slurm/cluster.py:75-76`

**Issue:** Exceptions in the poller are caught and stored in status but not logged:

```python
except Exception as exc:
    status = {"JobState": "UNKNOWN", "Error": str(exc)}
```

**Impact:** Debugging production issues is difficult.

**Fix:** Add `logger.warning()` or `logger.debug()` for the exception.

---

### 11. Workflow Slurmfile Generation Doesn't Validate Output
**File:** `src/slurm/cluster.py:437-504`

**Issue:** `_render_workflow_slurmfile()` constructs TOML by string concatenation without validating the result is valid TOML. Special characters in values could break parsing.

**Impact:** Runner fails to load Slurmfile, workflow dies.

**Fix:** Use `tomli_w` or similar library to generate valid TOML, or validate after generation.

---

### 12. Backend Abstraction Leakage in Job Class
**File:** `src/slurm/job.py:470,798`

**Issue:** `Job._read_remote_file()` and `get_result()` contain `isinstance(self.cluster.backend, SSHCommandBackend)` checks. This violates the backend abstraction and will break for new backend types.

```python
if isinstance(self.cluster.backend, SSHCommandBackend):
    # SSH-specific code
else:
    # Assumed to be local
```

**Impact:** New backends require modification to Job class.

**Fix:** Add abstract methods to `BackendBase` for these operations.

---

## Low Priority (Code Quality)

### 13. Duplicate `LocalBackend` Implementations
**Files:** `src/slurm/api/local.py`, `tests/helpers/local_backend.py`

**Issue:** Two different `LocalBackend` classes exist. Tests sometimes import from one, sometimes the other, creating confusion and potential inconsistencies.

**Impact:** Test reliability, maintainability.

**Fix:** Consolidate into single implementation; tests should use the same one as production.

---

### 14. Fragile Test Pattern: `object.__new__(Cluster)`
**Files:** Multiple test files (11+ occurrences)

**Issue:** Tests create `Cluster` instances by bypassing `__init__`:

```python
cluster = object.__new__(Cluster)
cluster.job_base_dir = str(tmp_path)
# Only set specific attributes needed for test
```

This is fragile because adding required attributes to `__init__` will break tests silently.

**Impact:** Tests don't fail when they should.

**Fix:** Create proper test fixtures or mock factories that construct valid objects.

---

### 15. Magic String Keys for Job Status
**Files:** `src/slurm/job.py`, `src/slurm/cluster.py`, `src/slurm/api/ssh.py`

**Issue:** Status dictionary keys like `"JobState"`, `"ExitCode"`, `"WorkDir"` are scattered throughout the codebase as magic strings.

**Impact:** Typos cause silent failures; no IDE autocomplete.

**Fix:** Define an enum or constants class for scheduler status keys.

---

### 16. Callback Method Names as Strings
**File:** `src/slurm/cluster.py:93,1001` etc.

**Issue:** Callback method names are passed as strings to `should_run_on_client()`:

```python
if not callback.should_run_on_client("on_job_status_update_ctx"):
```

Typos in method names are not caught at compile time.

**Impact:** Callbacks silently not invoked.

**Fix:** Use `Protocol` with proper method signatures or decorator-based registration.

---

### 17. Circular Import Fragility
**Files:** `src/slurm/task.py`, `src/slurm/context.py`

**Issue:** These modules have cross-dependencies mitigated with `TYPE_CHECKING` guards. Refactoring can easily break these.

**Impact:** Import errors on module load.

**Fix:** Document the dependency graph; consider restructuring.

---

### 18. Redundant Output/Error Path Setting in Rendering
**File:** `src/slurm/rendering.py:120-165`

**Issue:** Output/error paths are set twice - once at lines 121-139, then again at 148-165 with `shlex.quote()`. The first setting is overwritten.

**Impact:** Wasted computation, confusing code.

**Fix:** Remove the first setting block or consolidate logic.

---

### 19. Unused `_pending_dependencies` in SlurmTask
**File:** `src/slurm/task.py:384`

**Issue:** `SlurmTask._pending_dependencies` is initialized but the `.after()` method returns a new `SlurmTaskWithDependencies` wrapper instead of mutating this field. The field appears unused except for compatibility.

**Impact:** Dead code confusion.

**Fix:** Remove if truly unused, or document its purpose.

---

### 20. Incomplete Error Context in PackagingError
**File:** `src/slurm/cluster.py:1027-1031`

**Issue:** `PackagingError` is raised with basic info, but the `effective_packaging_config` dict may contain sensitive info (registry credentials) that gets logged.

**Impact:** Potential credential exposure in logs.

**Fix:** Sanitize sensitive fields before including in error messages.

---

## Dead Code Candidates

### 21. `SlurmTask.slurm_options`
**File:** `src/slurm/task.py:381`

**Issue:** `slurm_options` is stored but appears unused throughout the codebase. It's passed through `with_options()` but never read for actual functionality.

**Recommendation:** Verify usage or remove.

---

### 22. `_container_dependencies` in SlurmTask
**File:** `src/slurm/task.py:387`

**Issue:** Initialized and preserved in `with_options()` but may not be used in actual container building flow.

**Recommendation:** Trace usage; appears to be infrastructure for Phase 2 features.

---

### 23. Commented-Out Nested Structure Tests
**File:** `tests/test_dependencies.py` (per exploration notes)

**Issue:** Tests for Job objects in nested structures are commented out with a note about Phase 1 lazy submission.

**Recommendation:** Either implement lazy submission or remove dead test code.

---

## Test Coverage Gaps

### 24. SSH Backend Edge Cases
- Timeouts during file transfer
- Partial upload recovery
- Connection drop mid-operation

### 25. Workflow Context in Nested Scenarios
- Nested workflow calling child workflows
- Workflow context scope isolation

### 26. Container Runtime Unavailability
- Graceful handling when Podman/Docker not available
- Clear error messages for container build failures

---

## Summary

| Priority | Count | Estimated Effort |
|----------|-------|------------------|
| Critical | 2 | Medium |
| High | 5 | Medium-High |
| Medium | 5 | Low-Medium |
| Low | 8 | Low |
| Dead Code | 3 | Low |
| Test Gaps | 3 | Medium |

**Recommended Order:**
1. #2 (Thread-unsafe cache) - Quick fix, prevents intermittent bugs
2. #6 (Missing timeout) - Quick fix, prevents hangs
3. #3 (SFTP leak) - Medium effort, prevents resource exhaustion
4. #1 (Pickle security) - Document trust model immediately, long-term migration
5. #8 (Transaction semantics) - Medium effort, improves reliability

---
---

# Development Infrastructure: Design & Engineering Plan

This section outlines the design and implementation plan for unified development and testing infrastructure based on the requirements in `old_plans/container_plan.md`.

## Requirements Summary

1. **Unified Docker Compose** - Single docker-compose setup for both integration tests and devcontainer
2. **Development Container** - Devcontainer with uv and Python tooling for SDK development
3. **Integrated Slurm Cluster** - Docker-compose managed Slurm cluster available inside devcontainer, wired up like a private cluster with container registry
4. **Flexible Test Execution** - Integration tests run from containerized host (when on host) or inside devcontainer (when developing inside devcontainer)

---

## Current State Analysis

### Existing Infrastructure

```
tests/integration/
├── docker-compose.yml          # Slurm + Registry services
├── conftest.py                 # pytest fixtures for docker-compose orchestration

containers/slurm-pyxis-integration/
├── Containerfile               # Debian 12 + Slurm + Pyxis + Podman
├── slurm.conf
├── install-enroot.sh
└── install-pyxis.sh
```

### What Works
- Docker-compose starts Slurm cluster with Pyxis/enroot for container jobs
- Registry service available at `registry:5000`
- SSH accessible at `localhost:2222` from host
- Integration tests use fixtures to manage lifecycle

### What's Missing
- No devcontainer configuration (`.devcontainer/`)
- No development container image with uv tooling
- No networking setup for devcontainer ↔ Slurm cluster communication
- No test execution mode detection (host vs devcontainer)

---

## Architecture Design

### Container Network Topology

```
┌─────────────────────────────────────────────────────────────────────┐
│  Docker Network: slurm-dev-network                                  │
│                                                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │   devcontainer   │  │      slurm       │  │     registry     │  │
│  │                  │  │                  │  │                  │  │
│  │  - uv            │  │  - slurmctld     │  │  - registry:2    │  │
│  │  - python 3.11+  │  │  - slurmd        │  │                  │  │
│  │  - pytest        │  │  - pyxis/enroot  │  │  Port: 5000      │  │
│  │  - SDK source    │  │  - podman        │  │                  │  │
│  │                  │  │                  │  │                  │  │
│  │  Mounts:         │  │  SSH: port 22    │  │                  │  │
│  │  - /workspace    │  │  (internal)      │  │                  │  │
│  │  - docker.sock   │  │                  │  │                  │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│           │                     │                     │            │
│           └─────────────────────┴─────────────────────┘            │
│                           DNS resolution                            │
│                   (slurm, registry hostnames)                       │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Port mappings (host access)
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Host Machine                                                       │
│                                                                     │
│  localhost:2222  → slurm:22      (SSH to Slurm cluster)            │
│  localhost:5000  → registry:5000 (Container registry)              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Execution Modes

| Mode | Unit Tests | Integration Tests | Slurm Access |
|------|------------|-------------------|--------------|
| **Host Development** | Run on host | Run via devcontainer image | SSH to localhost:2222 |
| **Devcontainer Development** | Run in devcontainer | Run in devcontainer | SSH to slurm:22 (internal) |
| **CI Pipeline** | Run in container | Run in container | SSH to slurm:22 (internal) |

---

## File Structure Plan

```
slurm_sdk/
├── .devcontainer/
│   ├── devcontainer.json           # VSCode/Cursor devcontainer config
│   └── Containerfile.dev           # Development container image
│
├── containers/
│   ├── docker-compose.yml          # MOVED: Unified compose (dev + test)
│   ├── slurm-pyxis-integration/    # Existing Slurm cluster image
│   │   ├── Containerfile
│   │   ├── slurm.conf
│   │   └── ...
│   └── dev/                        # NEW: Development container
│       └── Containerfile
│
├── tests/
│   ├── integration/
│   │   ├── docker-compose.yml      # DEPRECATED: Symlink to containers/
│   │   └── conftest.py             # Updated for mode detection
│   └── conftest.py                 # Root conftest
│
└── scripts/
    ├── run-integration-tests.sh    # Host mode test runner
    └── setup-dev-environment.sh    # One-time setup helper
```

---

## Implementation Plan

### Phase 1: Development Container Image

**Goal:** Create a container image suitable for SDK development with all required tooling.

#### 1.1 Create `containers/dev/Containerfile`

```dockerfile
# syntax=docker/dockerfile:1.4
FROM python:3.11-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    curl \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Install development tools
RUN uv tool install ruff && \
    uv tool install pytest && \
    uv tool install mypy

# Configure SSH for Slurm cluster access
RUN mkdir -p /root/.ssh && \
    echo "Host slurm slurm-control\n\
    StrictHostKeyChecking no\n\
    UserKnownHostsFile /dev/null\n\
    LogLevel ERROR" > /root/.ssh/config && \
    chmod 600 /root/.ssh/config

WORKDIR /workspace

# Default command for interactive development
CMD ["bash"]
```

#### 1.2 Tasks
- [x] Create `containers/dev/Containerfile`
- [x] Test image builds successfully
- [x] Verify uv, pytest, ruff are accessible
- [x] Test SSH connectivity to Slurm service

**Status: COMPLETED**

---

### Phase 2: Unified Docker Compose

**Goal:** Single docker-compose.yml that serves both development and testing.

#### 2.1 Create `containers/docker-compose.yml`

```yaml
# Unified compose for development and integration testing
# Usage:
#   Development: docker compose up -d slurm registry
#   Full stack:  docker compose up -d (includes devcontainer)

services:
  # Development container (optional - use with devcontainer or CI)
  devcontainer:
    build:
      context: ./dev
      dockerfile: Containerfile
    container_name: slurm-sdk-dev
    volumes:
      - ..:/workspace:cached
      - /var/run/docker.sock:/var/run/docker.sock  # For container builds
    networks:
      - slurm-dev-network
    depends_on:
      slurm:
        condition: service_healthy
      registry:
        condition: service_started
    environment:
      - SLURM_SDK_DEV_MODE=devcontainer
      - SLURM_HOST=slurm
      - SLURM_PORT=22
      - REGISTRY_URL=registry:5000
    stdin_open: true
    tty: true
    # Not started by default - use profiles
    profiles:
      - dev
      - ci

  # Local container registry
  registry:
    image: registry:2
    container_name: slurm-test-registry
    ports:
      - "5000:5000"
    networks:
      - slurm-dev-network
    restart: unless-stopped
    environment:
      REGISTRY_STORAGE_DELETE_ENABLED: "true"
    volumes:
      - registry-data:/var/lib/registry

  # Slurm cluster with Pyxis/enroot
  slurm:
    build:
      context: ./slurm-pyxis-integration
      dockerfile: Containerfile
    image: slurm-integration:latest
    container_name: slurm-test
    hostname: slurm-control
    privileged: true
    ports:
      - "2222:22"  # SSH from host
    networks:
      slurm-dev-network:
        aliases:
          - slurm
          - slurm-pyxis
          - slurm-control
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
      - slurm-home:/home/slurm
    depends_on:
      - registry
    command: ["/sbin/init"]
    healthcheck:
      test: ["CMD", "systemctl", "is-active", "sshd"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 10s

networks:
  slurm-dev-network:
    driver: bridge
    name: slurm-dev-network

volumes:
  registry-data:
    name: slurm-test-registry-data
  slurm-home:
    name: slurm-test-home
```

#### 2.2 Tasks
- [x] Create `containers/docker-compose.yml`
- [x] Move Slurm container context to `containers/slurm-pyxis-integration/` (already in place)
- [x] Add symlink: `tests/integration/docker-compose.yml` → `../../containers/docker-compose.yml`
- [x] Test `docker compose up slurm registry` works
- [x] Test `docker compose --profile dev up` starts devcontainer

**Status: COMPLETED**

---

### Phase 3: Devcontainer Configuration

**Goal:** VSCode/Cursor devcontainer integration for seamless development.

#### 3.1 Create `.devcontainer/devcontainer.json`

```json
{
  "name": "Slurm SDK Development",
  "dockerComposeFile": ["../containers/docker-compose.yml"],
  "service": "devcontainer",
  "workspaceFolder": "/workspace",

  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },

  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "charliermarsh.ruff",
        "ms-python.mypy-type-checker"
      ],
      "settings": {
        "python.defaultInterpreterPath": "/root/.local/bin/python",
        "python.testing.pytestEnabled": true,
        "python.testing.pytestArgs": ["tests"]
      }
    }
  },

  "postCreateCommand": "uv sync --dev",
  "postStartCommand": "echo 'Slurm cluster available at slurm:22'",

  "remoteEnv": {
    "SLURM_SDK_DEV_MODE": "devcontainer",
    "SLURM_HOST": "slurm",
    "SLURM_PORT": "22",
    "REGISTRY_URL": "registry:5000"
  },

  "forwardPorts": [5000, 2222],

  "runServices": ["slurm", "registry"]
}
```

#### 3.2 Tasks
- [x] Create `.devcontainer/devcontainer.json`
- [ ] Test "Reopen in Container" workflow in VSCode/Cursor (manual verification needed)
- [x] Verify Slurm cluster is accessible from devcontainer
- [ ] Verify SDK installs correctly with `uv sync --dev` (manual verification needed)
- [ ] Test running unit tests inside devcontainer (manual verification needed)

**Status: COMPLETED** (core implementation done, manual IDE testing recommended)

---

### Phase 4: Test Execution Mode Detection

**Goal:** Automatically detect execution environment and configure tests accordingly.

#### 4.1 Update `tests/integration/conftest.py`

Add mode detection at the top of the file:

```python
import os

def _detect_execution_mode() -> str:
    """Detect whether running on host, in devcontainer, or in CI.

    Returns:
        str: One of "host", "devcontainer", "ci"
    """
    # Explicit override
    if mode := os.environ.get("SLURM_SDK_DEV_MODE"):
        return mode

    # CI detection
    if os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"):
        return "ci"

    # Devcontainer detection (set by devcontainer.json)
    if os.environ.get("REMOTE_CONTAINERS"):
        return "devcontainer"

    # Check if we're inside docker
    if os.path.exists("/.dockerenv"):
        return "devcontainer"

    return "host"

EXECUTION_MODE = _detect_execution_mode()

def _get_slurm_connection_info() -> dict:
    """Get Slurm connection info based on execution mode."""
    if EXECUTION_MODE in ("devcontainer", "ci"):
        # Inside container network - use internal DNS
        return {
            "hostname": os.environ.get("SLURM_HOST", "slurm"),
            "port": int(os.environ.get("SLURM_PORT", "22")),
        }
    else:
        # On host - use port mapping
        return {
            "hostname": "127.0.0.1",
            "port": 2222,
        }
```

#### 4.2 Update Fixtures

Modify `pyxis_container` fixture to use detected mode:

```python
@pytest.fixture(scope="session")
def pyxis_container(docker_compose_project):
    """Return Pyxis container connection info based on execution mode."""
    conn = _get_slurm_connection_info()
    return {
        "runtime": docker_compose_project["container_runtime"],
        "hostname": conn["hostname"],
        "port": conn["port"],
        "username": SLURM_USER,
        "password": SLURM_USER,
        "name": docker_compose_project["pyxis_container_name"],
        "job_base_dir": f"/home/{SLURM_USER}/slurm_jobs",
        "partition": "debug",
    }
```

#### 4.3 Tasks
- [x] Add `_detect_execution_mode()` function to conftest.py
- [x] Update `_get_slurm_connection_info()` to use mode
- [x] Update fixtures to use dynamic connection info
- [x] Test integration tests work from host
- [x] Test integration tests work from devcontainer (network connectivity verified)
- [ ] Document the execution modes (Phase 6)

**Status: COMPLETED**

---

### Phase 5: Host-Mode Test Runner Script

**Goal:** When developing on host, provide easy way to run integration tests inside a container.

#### 5.1 Create `scripts/run-integration-tests.sh`

```bash
#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/containers/docker-compose.yml"

# Ensure services are running
echo "Starting Slurm cluster and registry..."
docker compose -f "$COMPOSE_FILE" up -d slurm registry

# Wait for Slurm to be healthy
echo "Waiting for Slurm cluster..."
docker compose -f "$COMPOSE_FILE" exec slurm systemctl is-active sshd || {
    echo "Waiting for SSH..."
    sleep 10
}

# Run tests in devcontainer
echo "Running integration tests..."
docker compose -f "$COMPOSE_FILE" run --rm \
    -e SLURM_SDK_DEV_MODE=ci \
    -e PYTEST_ARGS="${*:---run-integration}" \
    devcontainer \
    bash -c 'cd /workspace && uv sync --dev && uv run pytest $PYTEST_ARGS tests/integration/'

echo "Tests complete!"
```

#### 5.2 Tasks
- [x] Create `scripts/run-integration-tests.sh`
- [x] Make executable: `chmod +x scripts/run-integration-tests.sh`
- [x] Test running from host machine
- [ ] Add to documentation (Phase 6)

**Status: COMPLETED**

---

### Phase 6: Documentation & Cleanup

#### 6.1 Update README/CONTRIBUTING

Document the development workflows:

```markdown
## Development Setup

### Option 1: Devcontainer (Recommended)

1. Open project in VSCode/Cursor
2. Click "Reopen in Container" when prompted
3. Wait for container to build and dependencies to install
4. Run tests: `uv run pytest tests/`

### Option 2: Host Development

1. Start the Slurm cluster:
   ```bash
   docker compose -f containers/docker-compose.yml up -d slurm registry
   ```

2. Add registry to /etc/hosts:
   ```bash
   echo '127.0.0.1 registry' | sudo tee -a /etc/hosts
   ```

3. Run unit tests locally:
   ```bash
   uv run pytest tests/ --ignore=tests/integration
   ```

4. Run integration tests (containerized):
   ```bash
   ./scripts/run-integration-tests.sh
   ```
```

#### 6.2 Tasks
- [x] Update CONTRIBUTING.md with development workflows
- [x] Remove/deprecate old integration test documentation
- [x] Add troubleshooting section
- [x] Document environment variables

---

## Implementation Phases Summary

| Phase | Description | Effort | Dependencies | Status |
|-------|-------------|--------|--------------|--------|
| 1 | Development Container Image | Low | None | **DONE** |
| 2 | Unified Docker Compose | Medium | Phase 1 | **DONE** |
| 3 | Devcontainer Configuration | Low | Phase 2 | **DONE** |
| 4 | Test Mode Detection | Medium | Phase 2 | **DONE** |
| 5 | Host Test Runner Script | Low | Phase 2, 4 | **DONE** |
| 6 | Documentation | Low | All above | **DONE** |

**Total Estimated Effort:** 2-3 days
**Actual Progress:** All phases completed

---

## Validation Checklist

After implementation, verify:

- [x] `docker compose -f containers/docker-compose.yml up -d slurm registry` starts services
- [ ] "Reopen in Container" works in VSCode/Cursor (manual IDE verification needed)
- [x] `uv run pytest tests/` works inside devcontainer (unit tests pass - 8/8)
- [x] `uv run pytest --run-integration tests/integration/` works inside devcontainer (verified - `test_submit_job_over_ssh` passes)
- [x] `./scripts/run-integration-tests.sh` works from host
- [ ] CI pipeline can run integration tests (CI setup not configured yet)
- [x] Documentation is complete and accurate (CONTRIBUTING.md updated)
