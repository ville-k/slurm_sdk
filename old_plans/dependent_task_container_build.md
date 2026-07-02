# Dependent Task Container Build Design

## Problem Statement

When containerized workflows submit child tasks, those tasks fail because they don't have proper packaging configuration. The specific error is:

```
ERROR:slurm.rendering:Error generating packaging setup commands: Invalid metadata: wheel packaging requires venv_path and python_executable
```

### Root Cause Analysis

1. **Default Packaging Mismatch**: Tasks without explicit `packaging="inherit"` default to wheel packaging, but there's no wheel environment (venv) inside containers.

2. **Cluster Configuration Loss**: When the workflow runs inside a container:
   - It creates a new Cluster from `SLURM_SDK_SLURMFILE`
   - This cluster doesn't have the original container packaging defaults
   - The cluster's `packaging_defaults` gets lost in the remote environment

3. **Inherit Packaging Incomplete**: Even tasks with `packaging="inherit"` fail because:
   - The parent's container image reference isn't properly passed to child job scripts
   - The rendering doesn't generate Pyxis/srun commands for inherited container packaging

4. **Environment Detection**: The `_write_environment_metadata()` function writes metadata, but:
   - It tries to detect `SINGULARITY_NAME` or `SLURM_CONTAINER_IMAGE` for container image
   - These may not be set by Pyxis/enroot

## Troubleshooting Plan

### Phase 1: Diagnostic (1-2 hours)

1. **Inspect written metadata**: Add debug output to see what `.slurm_environment.json` contains:
   ```python
   # In runner.py, after metadata is written
   logger.info(f"Metadata content: {json.dumps(metadata, indent=2)}")
   ```

2. **Check environment variables**: Log all SLURM/container env vars in the workflow:
   ```python
   for k, v in os.environ.items():
       if 'SLURM' in k or 'CONTAINER' in k or 'PYXIS' in k or 'ENROOT' in k:
           logger.info(f"{k}={v}")
   ```

3. **Verify cluster loading**: Log what the loaded cluster contains:
   ```python
   logger.info(f"Cluster packaging_defaults: {cluster.packaging_defaults}")
   ```

### Phase 2: Fix Inherit Packaging (2-4 hours)

1. **Fix metadata collection** in `runner.py`:
   - Add `SLURM_CONTAINER` or custom env var for image reference
   - Include the full Pyxis image reference in metadata

2. **Update inherit strategy** in `packaging/inherit.py`:
   - For container packaging, generate proper Pyxis/srun commands
   - Include `--container-image` directive for child jobs

3. **Fix cluster configuration propagation**:
   - Store container packaging config in the environment metadata
   - Restore it when loading cluster in runner

### Phase 3: Update Job Script Rendering (2-4 hours)

When `packaging_type == "container"` and task uses inherit:
- Don't generate `#SBATCH` directives for container (parent handles it)
- OR generate proper `--container-image` for nested srun

## Alternative Design: Pre-Build Dependencies

### Concept

Instead of trying to build containers at runtime inside other containers (docker-in-docker), pre-build all required containers before submitting the workflow.

### API Design

```python
# Option 1: Method chaining
job = cluster.submit(my_workflow).with_dependencies([
    task_a,
    task_b,
    task_c
])(input_data)

# Option 2: Decorator parameter
@workflow(
    time="02:00:00",
    packaging="container",
    dependencies=[task_a, task_b, task_c]
)
def my_workflow(data, ctx: WorkflowContext):
    job_a = task_a(data)
    job_b = task_b(job_a.get_result())
    return job_b.get_result()

# Option 3: Context manager
with cluster.prepare_dependencies([task_a, task_b]):
    job = cluster.submit(my_workflow)(input_data)
```

### Recommended: Option 1 (Method Chaining)

```python
class SubmittableTask:
    def with_dependencies(self, tasks: List[Callable]) -> "SubmittableTask":
        """Pre-build containers for dependent tasks.

        Args:
            tasks: List of task functions that will be called by the workflow.
                   These will have their containers built before workflow submission.

        Returns:
            Self for method chaining.
        """
        self._dependencies = tasks
        return self

    def __call__(self, *args, **kwargs) -> Job:
        # Build dependency containers first
        if self._dependencies:
            self._build_dependency_containers()

        # Submit the workflow
        return self._submit(*args, **kwargs)

    def _build_dependency_containers(self):
        """Build containers for all dependent tasks."""
        for task in self._dependencies:
            if hasattr(task, '_slurm_task'):
                slurm_task = task._slurm_task

                # Apply container packaging from workflow
                if slurm_task.packaging != "inherit":
                    # Task has explicit packaging, build it
                    pass
                else:
                    # Task inherits, use workflow's container config
                    # Build with same dockerfile/registry
                    pass

                # Actually build and push container
                packaging_result = slurm_task.packaging_strategy.prepare(
                    slurm_task, self._cluster
                )

                # Store image reference for later use
                self._built_images[task.__name__] = packaging_result.get('image')
```

### Implementation Details

#### 1. Container Image Registry

Store built images in a registry that child tasks can reference:

```python
class DependencyContainerRegistry:
    """Registry of pre-built container images for workflow dependencies."""

    def __init__(self):
        self._images: Dict[str, str] = {}

    def register(self, task_name: str, image_ref: str):
        self._images[task_name] = image_ref

    def get_image(self, task_name: str) -> Optional[str]:
        return self._images.get(task_name)

    def serialize(self) -> str:
        """Serialize registry for passing to workflow environment."""
        return json.dumps(self._images)

    @classmethod
    def deserialize(cls, data: str) -> "DependencyContainerRegistry":
        registry = cls()
        registry._images = json.loads(data)
        return registry
```

#### 2. Environment Variable Passing

Pass the registry to the workflow via environment variable:

```python
# In job script generation
env_vars['SLURM_SDK_DEPENDENCY_IMAGES'] = registry.serialize()

# In runner.py, when loading cluster
dependency_images = os.environ.get('SLURM_SDK_DEPENDENCY_IMAGES')
if dependency_images:
    registry = DependencyContainerRegistry.deserialize(dependency_images)
    cluster.set_dependency_registry(registry)
```

#### 3. Task Resolution at Runtime

When tasks are called inside the workflow:

```python
# In cluster.submit() or task submission path
def _resolve_task_packaging(self, task):
    if task.packaging == "inherit":
        # Check if pre-built image exists
        registry = self._dependency_registry
        if registry:
            image = registry.get_image(task.name)
            if image:
                # Use pre-built image instead of building
                task.packaging_config['container_image'] = image
                task.packaging_config['skip_build'] = True
```

### Advantages of Pre-Build Approach

1. **No Docker-in-Docker**: Avoids complex container-in-container scenarios
2. **Faster Execution**: Containers are ready when workflow starts
3. **Explicit Dependencies**: Clear declaration of what the workflow needs
4. **Better Caching**: Images can be cached and reused across runs
5. **Simpler Debugging**: Build failures happen before workflow submission

### Disadvantages

1. **Upfront Cost**: All containers built even if not all tasks run
2. **Manual Declaration**: User must list all dependent tasks
3. **Dynamic Tasks**: Doesn't work for dynamically created tasks

## Recommended Approach

### Short-term (1-2 days): Fix Inherit Packaging

1. Fix the environment metadata to properly capture container image reference
2. Update inherit strategy to generate correct Pyxis commands
3. Pass container config through cluster loading

### Medium-term (3-5 days): Implement Pre-Build Dependencies

1. Implement `with_dependencies()` API
2. Add DependencyContainerRegistry
3. Update task resolution to use pre-built images
4. Add tests for the full workflow

### Implementation Priority

1. **Phase 1**: Fix metadata and inherit strategy (unblocks basic inheritance)
2. **Phase 2**: Implement pre-build API (production-ready solution)
3. **Phase 3**: Auto-detection of dependencies (future enhancement)

## Testing Strategy

### Unit Tests

```python
def test_dependency_container_build():
    """Test that dependencies are built before workflow submission."""

def test_dependency_registry_serialization():
    """Test registry can be passed through environment."""

def test_inherit_uses_prebuilt_image():
    """Test that inherit packaging uses pre-built image when available."""
```

### Integration Tests

```python
@pytest.mark.container_packaging
def test_workflow_with_prebuilt_dependencies(slurm_pyxis_cluster):
    """Test workflow with pre-built task containers."""

    @task(time="00:01:00", packaging="inherit")
    def add(a: int, b: int) -> int:
        return a + b

    @workflow(time="00:05:00", packaging="container", ...)
    def my_workflow(x: int, ctx: WorkflowContext):
        job = add(x, 10)
        return job.get_result()

    with slurm_pyxis_cluster:
        # Pre-build the add task container
        job = slurm_pyxis_cluster.submit(my_workflow).with_dependencies([
            add
        ])(5)

        assert job.wait(timeout=120)
        assert job.get_result() == 15
```

## Open Questions

1. **Image Naming**: How to name pre-built images? `{workflow}_{task}:{hash}`?
2. **Cache Invalidation**: When to rebuild? Hash of task code + Dockerfile?
3. **Registry Cleanup**: How long to keep pre-built images?
4. **GPU Support**: How to handle GPU-specific container builds?

## Related Files

- `src/slurm/runner.py`: Environment metadata writing, cluster loading
- `src/slurm/packaging/inherit.py`: Inherit packaging strategy
- `src/slurm/packaging/container.py`: Container build/push logic
- `src/slurm/cluster.py`: Cluster configuration, submit logic
- `src/slurm/rendering.py`: Job script generation
