# Workflow Packaging and Submission Design

## Executive Summary

The current implementation of workflow task submission has evolved incrementally, resulting in complex inheritance logic for packaging strategies. This document proposes a cleaner model inspired by Unix process forking, where child tasks inherit execution context from their parent workflow in a well-defined, predictable manner.

## Background: Current Implementation

### How It Works Today

When a workflow submits child tasks, the system currently:

1. **Parent Workflow Job Submission** (e.g., `simple_workflow`):
   - Builds a wheel containing SDK + user code
   - Uploads wheel to cluster
   - Creates venv in job directory: `{job_dir}/.slurm_venv_{job_id}/`
   - Installs wheel into venv
   - Sets `$VIRTUAL_ENV` and `$PY_EXEC` environment variables
   - Executes workflow function

2. **Child Task Submission** (from within workflow):
   - Workflow code calls `cluster.submit(simple_task)(5)`
   - Cluster has `packaging_defaults = {"type": "none"}`
   - Runner sets this based on detecting non-container packaging
   - Child task job script is rendered with `NonePackagingStrategy`
   - Child task setup commands must somehow activate parent venv
   - Child imports SDK code from parent's venv

### The Problem

The current approach has several issues:

1. **Implicit State Transfer**: Parent venv path is passed via `$VIRTUAL_ENV` environment variable, which only works if child job runs on same node
2. **Packaging Strategy Confusion**: `{"type": "none"}` doesn't clearly express "inherit parent environment"
3. **Environment Variable Coupling**: Child tasks depend on specific env vars being set by parent
4. **No Clear Contract**: What exactly does a child task inherit? Venv? Container? PYTHONPATH?
5. **Ad-hoc Configuration**: `parent_venv_path` added to `NonePackagingStrategy` config breaks the abstraction

### Current Code Flow

```python
# runner.py (parent workflow execution)
parent_venv_path = os.environ.get("VIRTUAL_ENV")
cluster.packaging_defaults = {
    "type": "none",
    "parent_venv_path": parent_venv_path  # Ad-hoc addition
}

# none.py (child task setup)
if parent_venv_path:
    commands.append(f"source {parent_venv_path}/bin/activate")
    commands.append(f"export PY_EXEC={parent_venv_path}/bin/python")
```

This works but is conceptually unclear and fragile.

## Design Philosophy: Fork/Exec Model

### Unix Process Model as Inspiration

In Unix, when a process forks:

1. **Fork**: Child inherits parent's memory, file descriptors, environment
2. **Exec**: Child optionally loads new program, but keeps inherited environment
3. **Clear Semantics**: Parent and child share resources with copy-on-write

We can apply similar principles to Slurm job submission:

1. **Submit (Fork)**: Child task inherits parent's execution environment
2. **Package (Exec)**: Child optionally creates new environment, or reuses parent's
3. **Clear Semantics**: Inheritance rules are explicit and predictable

### Core Principles

1. **Explicit Inheritance**: Child tasks explicitly declare what they inherit
2. **Environment Immutability**: Parent environment is a stable base for children
3. **Locality**: Inherited environments should work across cluster nodes
4. **Composability**: Container and wheel packaging should compose naturally

## Design Alternative 1: Explicit Inheritance Strategy (Recommended)

### Concept

Introduce a new packaging type `"inherit"` that explicitly represents inheriting parent's environment. The parent workflow metadata is stored in a well-known location that children can access.

### Architecture

```
Parent Workflow Submission
  └─> Package with wheel/container
      └─> Create execution environment (venv or container)
          └─> Store environment metadata in job directory
              └─> Execute workflow function
                  └─> Child task submission
                      └─> packaging_type="inherit"
                          └─> Read parent metadata
                              └─> Activate same environment
```

### Implementation

#### 1. Environment Metadata File

Parent workflow stores environment metadata:

```python
# runner.py - after venv/container setup
metadata = {
    "packaging_type": "wheel",  # or "container"
    "environment": {
        "venv_path": "/home/slurm/slurm_jobs/workflow/timestamp_id/.slurm_venv_id",
        "python_executable": "/path/to/venv/bin/python",
        "container_image": None,  # or image reference for containers
    },
    "shared_paths": {
        "job_dir": "/home/slurm/slurm_jobs/workflow/timestamp_id",
        "shared_dir": "/home/slurm/slurm_jobs/workflow/timestamp_id/shared",
    }
}

# Write to well-known location
with open(f"{job_dir}/.slurm_environment.json", "w") as f:
    json.dump(metadata, f)

# Also export for same-node children
os.environ["SLURM_SDK_PARENT_ENV"] = job_dir
```

#### 2. Inherit Packaging Strategy

New strategy that reads parent metadata:

```python
class InheritPackagingStrategy(PackagingStrategy):
    """Inherit execution environment from parent workflow."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.parent_job_dir = self.config.get("parent_job_dir")
        self.parent_metadata = None

    def prepare(self, task, cluster):
        """Load parent environment metadata."""
        if not self.parent_job_dir:
            raise PackagingError(
                "InheritPackagingStrategy requires parent_job_dir in config"
            )

        metadata_path = f"{self.parent_job_dir}/.slurm_environment.json"

        # Read metadata via cluster backend
        try:
            metadata_content = cluster.backend.read_file(metadata_path)
            self.parent_metadata = json.loads(metadata_content)
        except Exception as e:
            raise PackagingError(
                f"Could not load parent environment metadata from {metadata_path}: {e}"
            )

        return {
            "status": "success",
            "message": f"Will inherit from parent: {self.parent_job_dir}",
            "parent_metadata": self.parent_metadata,
        }

    def generate_setup_commands(self, task, job_id=None, job_dir=None):
        """Generate commands to activate parent environment."""
        if not self.parent_metadata:
            raise RuntimeError("prepare() must be called before generate_setup_commands()")

        commands = []
        env = self.parent_metadata["environment"]
        packaging_type = self.parent_metadata["packaging_type"]

        if packaging_type == "wheel":
            venv_path = env["venv_path"]
            python_exec = env["python_executable"]

            commands.append(f"echo '--- Inheriting Parent Wheel Environment ---'")
            commands.append(f"echo 'Parent job dir: {self.parent_job_dir}'")
            commands.append(f"echo 'Parent venv: {venv_path}'")
            commands.append(f"source {shlex.quote(venv_path)}/bin/activate")
            commands.append(f"export PY_EXEC={shlex.quote(python_exec)}")
            commands.append(f'echo "Activated parent venv: $VIRTUAL_ENV"')

        elif packaging_type == "container":
            # Container tasks already running in parent's container
            # Just set python executable
            python_exec = env["python_executable"]
            commands.append(f"echo '--- Inheriting Parent Container Environment ---'")
            commands.append(f"export PY_EXEC={shlex.quote(python_exec)}")

        return commands
```

#### 3. Workflow Context Setup

Runner automatically configures inheritance:

```python
# runner.py - workflow context creation
if _function_wants_workflow_context(func):
    cluster = Cluster.from_env(slurmfile_path, env=env_name)

    # Configure child tasks to inherit this workflow's environment
    cluster.packaging_defaults = {
        "type": "inherit",
        "parent_job_dir": job_dir,  # Clear, explicit parameter
    }

    logger.info(
        f"Configured child tasks to inherit environment from {job_dir}"
    )
```

### Advantages

- ✅ **Explicit Intent**: `"inherit"` clearly expresses what's happening
- ✅ **Self-Documenting**: Metadata file makes environment inspectable
- ✅ **Cross-Node**: Metadata stored on shared filesystem works across nodes
- ✅ **Debuggable**: Can inspect `.slurm_environment.json` to understand inheritance
- ✅ **Type Safety**: Each packaging type has clear semantics
- ✅ **Composable**: Works uniformly for wheel and container packaging

### Disadvantages

- ❌ **Extra I/O**: Requires writing/reading metadata file
- ❌ **Shared Filesystem Dependency**: Assumes job directories are network-mounted
- ❌ **New Abstraction**: Adds another packaging type to understand

## Design Alternative 2: Fork-Style Environment Cloning

### Concept

Like Unix fork, child tasks get a "snapshot" of parent environment passed via job script. No metadata files - everything is embedded.

### Architecture

```python
# Parent workflow exports environment as shell variables
export SLURM_SDK_PARENT_VENV="/path/to/venv"
export SLURM_SDK_PARENT_PYTHON="/path/to/python"
export SLURM_SDK_PARENT_PACKAGING="wheel"

# Child job inherits these via sbatch --export
# No need for special packaging type - rendering.py checks env vars
```

### Implementation

```python
# rendering.py - enhanced to detect parent environment
def render_job_script(...):
    # Before packaging setup commands
    script_lines.append("# Check for inherited parent environment")
    script_lines.append("if [ -n \"$SLURM_SDK_PARENT_VENV\" ]; then")
    script_lines.append("  echo 'Inheriting parent venv: $SLURM_SDK_PARENT_VENV'")
    script_lines.append("  source \"$SLURM_SDK_PARENT_VENV/bin/activate\"")
    script_lines.append("  export PY_EXEC=\"$SLURM_SDK_PARENT_PYTHON\"")
    script_lines.append("else")

    # Normal packaging setup
    setup_commands = packaging_strategy.generate_setup_commands(...)
    script_lines.extend(setup_commands)

    script_lines.append("fi")
```

### Advantages

- ✅ **Simple**: No new packaging type, no metadata files
- ✅ **Fast**: No I/O for metadata
- ✅ **Unix-like**: Familiar fork/exec semantics

### Disadvantages

- ❌ **Hidden Logic**: Inheritance logic buried in rendering.py
- ❌ **Environment Variable Pollution**: Many SLURM_SDK_* variables
- ❌ **Not Explicit**: Workflow code doesn't show inheritance happening
- ❌ **Fragile**: Depends on sbatch --export behavior

## Design Alternative 3: Container-Only Packaging (Simplified)

### Concept

**If we only supported containers**, the problem becomes much simpler. Container images are inherently reusable and location-independent.

### Why Containers Are Simpler

1. **Immutable Environment**: Container image is built once, tagged, pushed to registry
2. **Location Independent**: Any node can pull `registry/image:tag`
3. **No Installation**: No venv to create/activate
4. **Explicit Lineage**: Image tag encodes version/build

### Architecture

```
Parent Workflow Submission
  └─> Build container image (if not cached)
      └─> Tag as registry/project:workflow-abc123
      └─> Push to registry (if configured)
      └─> Submit job with --container=registry/project:workflow-abc123
          └─> Slurm runs job in container
              └─> Execute workflow function
                  └─> Child task submission
                      └─> Reuse same image: registry/project:workflow-abc123
                          └─> Slurm runs child in same container
```

### Implementation

```python
# runner.py - container packaging for workflows
if cluster.packaging_defaults.get("type") == "container":
    # Get the image reference we're currently running in
    current_image = os.environ.get("SINGULARITY_NAME") or \
                   os.environ.get("SLURM_CONTAINER_IMAGE")

    if current_image:
        # Child tasks reuse exact same image
        cluster.packaging_defaults = {
            "type": "container",
            "image": current_image,  # Explicit image reference
            "build": False,  # Never rebuild
            "push": False,   # Already in registry
        }
        logger.info(f"Child tasks will reuse container image: {current_image}")
```

### Advantages

- ✅ **No Environment Activation**: Container is the environment
- ✅ **Reproducible**: Same image = same environment everywhere
- ✅ **Simple Semantics**: Image reference is the only state to pass
- ✅ **No Metadata Files**: Image reference is in Slurm job config
- ✅ **Cacheable**: Registry caches images across cluster
- ✅ **Debuggable**: `singularity inspect` shows image contents

### Disadvantages

- ❌ **Container-Only**: Doesn't support wheel packaging
- ❌ **Build Time**: Initial image build can be slow
- ❌ **Registry Required**: Needs container registry (or local cache)
- ❌ **Less Flexible**: Can't easily modify environment on-the-fly

### Simplified System

If we **only** supported containers:

1. **Remove** `WheelPackagingStrategy`
2. **Remove** `NonePackagingStrategy`
3. **Remove** `InheritPackagingStrategy`
4. **Keep** `ContainerPackagingStrategy` with image reuse logic
5. **Simplify** `runner.py` - no venv activation logic
6. **Simplify** `rendering.py` - no `$PY_EXEC` fallbacks

Result: ~500 lines of code eliminated, clearer mental model.

## Comparison Matrix

| Aspect | Alt 1: Inherit Strategy | Alt 2: Fork-Style | Alt 3: Container-Only |
|--------|------------------------|-------------------|----------------------|
| **Conceptual Clarity** | ⭐⭐⭐⭐⭐ Explicit | ⭐⭐⭐ Hidden | ⭐⭐⭐⭐⭐ Simple |
| **Implementation Complexity** | ⭐⭐⭐ Moderate | ⭐⭐⭐⭐ Low | ⭐⭐⭐⭐⭐ Very Low |
| **Debuggability** | ⭐⭐⭐⭐⭐ Inspectable | ⭐⭐ Opaque | ⭐⭐⭐⭐⭐ Transparent |
| **Performance** | ⭐⭐⭐⭐ Fast | ⭐⭐⭐⭐⭐ Fastest | ⭐⭐⭐ Moderate |
| **Flexibility** | ⭐⭐⭐⭐⭐ Both wheel+container | ⭐⭐⭐⭐⭐ Both | ⭐⭐ Container only |
| **Robustness** | ⭐⭐⭐⭐ Reliable | ⭐⭐⭐ Fragile | ⭐⭐⭐⭐⭐ Very Robust |

## Recommendation

### Short Term: Alternative 1 (Inherit Strategy)

**Implement the explicit `InheritPackagingStrategy`** because:

1. It's conceptually clear and self-documenting
2. Works for both wheel and container packaging
3. Metadata file provides debuggability
4. Clean abstraction with minimal coupling

**Migration Path**:
1. Implement `InheritPackagingStrategy` in new file `src/slurm/packaging/inherit.py`
2. Update `runner.py` to create `.slurm_environment.json` metadata
3. Update `runner.py` to set `packaging_defaults = {"type": "inherit", "parent_job_dir": ...}`
4. Remove ad-hoc `parent_venv_path` logic from `NonePackagingStrategy`
5. Update tests to verify inheritance works correctly

### Long Term: Consider Alternative 3 (Container-Only)

**Evaluate moving to container-only packaging** because:

1. Containers are more reproducible and portable
2. Eliminates ~500 lines of complex wheel/venv logic
3. Better aligns with modern HPC practices (Singularity/Apptainer)
4. Simplifies user mental model: "one workflow = one image"

**Decision Criteria**:
- Survey users: How many use wheel packaging vs containers?
- Benchmark: Is container build time acceptable for iterative development?
- Infrastructure: Do target clusters support Singularity/Apptainer?

## Implementation Details: Alternative 1

### File Structure

```
src/slurm/packaging/
├── base.py              # Base strategy class
├── wheel.py             # Wheel packaging
├── container.py         # Container packaging
├── inherit.py           # NEW: Inherit packaging
└── none.py              # DEPRECATED: To be removed
```

### Metadata Schema

```json
{
  "version": "1.0",
  "packaging_type": "wheel",
  "environment": {
    "venv_path": "/home/slurm/slurm_jobs/workflow/20251018_120000_abc123/.slurm_venv_abc123",
    "python_executable": "/home/slurm/slurm_jobs/workflow/20251018_120000_abc123/.slurm_venv_abc123/bin/python",
    "python_version": "3.11.2",
    "container_image": null,
    "activated": true
  },
  "shared_paths": {
    "job_dir": "/home/slurm/slurm_jobs/workflow/20251018_120000_abc123",
    "shared_dir": "/home/slurm/slurm_jobs/workflow/20251018_120000_abc123/shared",
    "tasks_dir": "/home/slurm/slurm_jobs/workflow/20251018_120000_abc123/tasks"
  },
  "parent_job": {
    "slurm_job_id": "12345",
    "pre_submission_id": "20251018_120000_abc123",
    "workflow_name": "simple_workflow"
  },
  "created_at": "2025-10-18T12:00:00Z"
}
```

### Error Handling

```python
class InheritPackagingStrategy(PackagingStrategy):
    def prepare(self, task, cluster):
        try:
            # Try to read metadata
            metadata_content = cluster.backend.read_file(metadata_path)
            self.parent_metadata = json.loads(metadata_content)

            # Validate schema
            required_keys = ["packaging_type", "environment"]
            if not all(k in self.parent_metadata for k in required_keys):
                raise PackagingError("Invalid metadata schema")

            return {"status": "success", ...}

        except FileNotFoundError:
            raise PackagingError(
                f"Parent environment metadata not found at {metadata_path}.\n"
                f"This usually means:\n"
                f"  1. The parent workflow job failed before creating metadata\n"
                f"  2. The parent_job_dir path is incorrect\n"
                f"  3. The shared filesystem is not mounted correctly\n"
                f"Expected metadata at: {metadata_path}"
            )
        except json.JSONDecodeError as e:
            raise PackagingError(f"Invalid metadata JSON: {e}")
        except Exception as e:
            raise PackagingError(f"Unexpected error reading metadata: {e}")
```

### Backward Compatibility

```python
# Keep NonePackagingStrategy for one release cycle with deprecation warning
class NonePackagingStrategy(PackagingStrategy):
    def __init__(self, config=None):
        super().__init__(config)
        warnings.warn(
            "NonePackagingStrategy is deprecated. Use InheritPackagingStrategy instead.",
            DeprecationWarning,
            stacklevel=2
        )

    # Rest of implementation unchanged
```

## Testing Strategy

### Unit Tests

```python
def test_inherit_strategy_reads_metadata(tmp_path, mock_cluster):
    """Test that InheritPackagingStrategy correctly reads parent metadata."""
    # Create mock metadata file
    metadata = {
        "packaging_type": "wheel",
        "environment": {"venv_path": "/path/to/venv", ...},
        ...
    }
    metadata_path = tmp_path / ".slurm_environment.json"
    metadata_path.write_text(json.dumps(metadata))

    # Create strategy
    strategy = InheritPackagingStrategy({"parent_job_dir": str(tmp_path)})
    result = strategy.prepare(mock_task, mock_cluster)

    assert result["status"] == "success"
    assert strategy.parent_metadata == metadata

def test_inherit_strategy_generates_activation_commands():
    """Test that activation commands correctly reference parent venv."""
    strategy = InheritPackagingStrategy({"parent_job_dir": "/parent"})
    strategy.parent_metadata = {
        "packaging_type": "wheel",
        "environment": {
            "venv_path": "/parent/.slurm_venv_123",
            "python_executable": "/parent/.slurm_venv_123/bin/python",
        }
    }

    commands = strategy.generate_setup_commands(mock_task)

    assert any("source /parent/.slurm_venv_123/bin/activate" in cmd for cmd in commands)
    assert any("PY_EXEC=/parent/.slurm_venv_123/bin/python" in cmd for cmd in commands)
```

### Integration Tests

```python
@pytest.mark.integration_test
def test_workflow_child_tasks_inherit_environment(slurm_cluster):
    """Test that child tasks correctly inherit parent workflow environment."""

    @workflow(time="00:05:00")
    def parent_workflow(ctx: WorkflowContext):
        # Submit child task
        child_job = simple_task(5)
        return child_job.get_result()

    # Submit parent workflow
    job = slurm_cluster.submit(parent_workflow)()
    job.wait()

    # Check that child task's job script has inherit packaging
    child_jobs = find_child_jobs(job)
    assert len(child_jobs) == 1

    child_script = read_job_script(child_jobs[0])
    assert ".slurm_environment.json" in child_script
    assert "Inheriting Parent" in child_script
```

## Migration Guide for Users

### Before (Current Code)

```python
@workflow(time="00:05:00")
def my_workflow(ctx: WorkflowContext):
    # Child tasks implicitly use packaging_type="none"
    job = my_task(42)
    return job.get_result()
```

### After (With Inherit Strategy)

```python
# No user-facing changes required!
# The workflow automatically configures inheritance

@workflow(time="00:05:00")
def my_workflow(ctx: WorkflowContext):
    # Child tasks now explicitly use packaging_type="inherit"
    # This is set automatically by the runner
    job = my_task(42)
    return job.get_result()
```

### Advanced: Manual Override

```python
@workflow(time="00:05:00")
def my_workflow(ctx: WorkflowContext):
    # Override packaging for specific child task
    with ctx.cluster.packaging_override({"type": "wheel"}):
        # This child will build its own wheel instead of inheriting
        job = special_task(99)

    # Other children still inherit
    job2 = normal_task(42)

    return job.get_result(), job2.get_result()
```

## Conclusion

The **InheritPackagingStrategy** (Alternative 1) provides the best balance of clarity, flexibility, and robustness for the current system. It makes the implicit explicit, enables better debugging, and maintains support for both wheel and container packaging.

However, the **container-only** approach (Alternative 3) represents a potential future direction that could dramatically simplify the system while improving reproducibility. This should be evaluated based on user needs and cluster capabilities.

The key insight from the Unix fork/exec model is that **explicit inheritance with clear semantics** beats implicit environment coupling. By making packaging inheritance a first-class concept with its own strategy type, we create a system that is easier to understand, debug, and maintain.
