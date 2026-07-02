# Docker Compose Consolidation Plan

## Current State

### Two Separate Container Setups

1. **Regular Slurm Container** (`podman/docker run`)
   - **Location**: `conftest.py` lines 174-232 (`slurm_container` fixture)
   - **Container**: Built from `containers/slurm-integration/Containerfile`
   - **Usage**: General integration tests (6 test files)
     - `test_slurm_container.py`
     - `test_workflow_callbacks_integration.py`
     - `test_output_dir.py`
     - `test_job_script_persistence.py`
     - `test_native_array_jobs.py`
     - `test_workflow_integration.py`
   - **Features**: Basic Slurm cluster, SSH access

2. **Docker Compose + Pyxis** (`docker-compose`)
   - **Location**: `conftest.py` lines 362-513 (`docker_compose_project` fixture)
   - **Compose File**: `tests/integration/docker-compose.yml`
   - **Services**:
     - `registry`: Local Docker registry (port 5000)
     - `slurm-pyxis`: Slurm with Pyxis/enroot support (SSH port 2222)
   - **Container**: Built from `containers/slurm-pyxis-integration/Containerfile`
   - **Usage**: Container packaging tests (2 test files)
     - `test_container_packaging_basic.py`
     - `test_container_packaging_advanced.py`
   - **Features**: Pyxis, enroot, container registry

## Problems with Current Setup

1. **Duplication**: Two separate Slurm containers with similar configuration
2. **Complexity**: Different startup mechanisms (direct podman run vs docker-compose)
3. **Resource Waste**: Running separate containers when one could serve both purposes
4. **Maintenance**: Two Containerfiles to maintain
5. **Confusion**: Developers need to understand which setup to use for which tests

## Proposed Solution

### Single Unified Docker Compose Setup

Create one docker-compose environment that supports **all** integration tests.

### Architecture

```
docker-compose.yml
├── registry (existing)
│   └── Local container registry for image storage
└── slurm (unified)
    ├── Pyxis + enroot (for container packaging tests)
    ├── Basic Slurm features (for general integration tests)
    └── SSH access on port 2222
```

## Implementation Steps

### Phase 1: Enhance Pyxis Container to Support All Tests

**Goal**: Make the Pyxis container capable of running all existing integration tests.

1. **Review Feature Parity**
   - Compare `slurm-integration/Containerfile` vs `slurm-pyxis-integration/Containerfile`
   - Identify any features in the basic container not in Pyxis container
   - Document differences

2. **Update Pyxis Containerfile** (if needed)
   - Add any missing packages/configuration from basic container
   - Ensure systemd, SSH, and Slurm are properly configured
   - Test that basic Slurm operations work

### Phase 2: Update Docker Compose Configuration

**Goal**: Update `docker-compose.yml` to serve all test scenarios.

1. **Rename Service**: `slurm-pyxis` → `slurm`
   - Update container name: `slurm-test-pyxis` → `slurm-test`
   - Keep all existing Pyxis/enroot functionality

2. **Add Service Aliases** (optional)
   - `slurm-pyxis` as alias for backward compatibility during transition

### Phase 3: Refactor Pytest Fixtures

**Goal**: Consolidate fixtures to use single docker-compose setup.

1. **Create Unified Cluster Fixture**
   ```python
   @pytest.fixture(scope="session")
   def docker_compose_cluster(request):
       """Start docker-compose and return unified cluster info."""
       # Start docker-compose (registry + slurm)
       # Wait for services to be ready
       # Return connection info for both services

   @pytest.fixture(scope="session")
   def slurm_cluster_info(docker_compose_cluster):
       """Extract Slurm cluster connection info."""
       return {
           "hostname": "127.0.0.1",
           "port": 2222,
           "username": "slurm",
           "password": "slurm",
           ...
       }
   ```

2. **Deprecate Old Fixtures**
   - Mark `slurm_container` (podman run) as deprecated
   - Keep `docker_compose_project` fixture but simplify it
   - Ensure `pyxis_container` uses the same underlying service

3. **Update Fixture Dependencies**
   - Change `slurm_cluster` fixture to use docker-compose backend
   - Update `slurm_pyxis_cluster` to use same backend
   - Both fixtures should point to the same running container

### Phase 4: Update Test Files

**Goal**: Migrate all tests to use unified fixtures.

1. **No Changes Needed**
   - Tests using `slurm_cluster` fixture continue to work
   - Tests using `slurm_pyxis_cluster` fixture continue to work
   - Only the backend implementation changes

2. **Verify Test Compatibility**
   - Run full integration test suite
   - Ensure all tests pass with new setup

### Phase 5: Cleanup

**Goal**: Remove deprecated code and containers.

1. **Remove Old Container**
   - Delete `containers/slurm-integration/Containerfile`
   - Remove related build files

2. **Remove Old Fixtures**
   - Remove `slurm_container` fixture (podman run implementation)
   - Remove `_build_image`, `_start_container`, etc. helper functions
   - Clean up conftest.py

3. **Update Documentation**
   - Update test README with new setup instructions
   - Document single docker-compose command for all tests

## Benefits

1. **Simplification**: One docker-compose command runs all integration tests
2. **Resource Efficiency**: Single Slurm container instead of two
3. **Faster Tests**: Reuse same container across test suites
4. **Easier Maintenance**: One Containerfile to update
5. **Better Developer Experience**: Clear, consistent setup

## Migration Strategy

### Gradual Migration (Recommended)

1. **Week 1**: Implement Phase 1 & 2 (enhance Pyxis, update docker-compose)
2. **Week 2**: Implement Phase 3 (new fixtures, keep old ones)
3. **Week 3**: Implement Phase 4 (verify all tests work)
4. **Week 4**: Implement Phase 5 (cleanup old code)

### Testing at Each Phase

- Run full integration suite after each phase
- Use `SLURM_TEST_KEEP=1` to inspect running containers
- Verify both basic and container packaging tests work

## Rollback Plan

If issues are discovered:
1. Keep old fixtures in conftest.py as fallback
2. Tests can selectively use old fixtures via pytest markers
3. Full rollback: revert conftest.py changes

## File Changes Summary

### Modified
- `tests/integration/docker-compose.yml` - Rename service, keep all features
- `tests/integration/conftest.py` - Consolidate fixtures
- `containers/slurm-pyxis-integration/Containerfile` - Add any missing features

### Deleted
- `containers/slurm-integration/Containerfile` - No longer needed
- `containers/slurm-integration/` directory - No longer needed

### Unchanged
- All test files (no changes needed)
- Test logic remains the same

## Success Criteria

1. ✅ All existing integration tests pass
2. ✅ Single `docker-compose up` command starts all services
3. ✅ Tests run faster (container reuse across suites)
4. ✅ Reduced code complexity in conftest.py
5. ✅ Documentation updated and clear

## Open Questions

1. **Pyxis Performance**: Does having Pyxis/enroot slow down basic tests?
   - **Mitigation**: Pyxis is only used when requested, shouldn't affect basic tests

2. **Port Conflicts**: SSH port 2222 vs dynamic ports?
   - **Decision**: Use fixed port 2222 for consistency (already in docker-compose)

3. **Container Startup Time**: Will unified container be slower to start?
   - **Answer**: No, docker-compose starts services in parallel

4. **Backward Compatibility**: Should we keep old fixtures?
   - **Recommendation**: Deprecate but keep for one release cycle
