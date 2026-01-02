# Slurm SDK Examples

This directory contains example scripts demonstrating various features of the Slurm SDK.

## Examples

### Basic Examples

#### `hello_world.py`
Basic task submission demonstrating the core SDK functionality.

```bash
uv run python -m slurm.examples.hello_world
```

#### `hello_container.py`
Container packaging example showing how to use Docker/Podman containers for task execution.

```bash
uv run python -m slurm.examples.hello_container
```

### Submitless Execution Examples

#### `dependent_jobs.py`
Demonstrates the submitless execution API with automatic job dependencies.

**Features:**
- Cluster context manager usage (`with Cluster.from_env()`)
- Automatic dependency detection (passing Job as argument)
- Explicit dependencies (`.after()`)
- Container packaging
- Result retrieval via `.get_result()`

**Pipeline:**
1. Generate dataset
2. Preprocess data (depends on step 1)
3. Train model (depends on step 2)
4. Evaluate model (depends on step 3)
5. Generate report (depends on steps 3 & 4)

```bash
uv run python -m slurm.examples.dependent_jobs --dataset-size 1000 --learning-rate 0.001
```

#### `ml_workflow.py`
Complete workflow demonstrating advanced orchestration features.

**Features:**
- `@workflow` decorator
- `WorkflowContext` parameter injection
- Array jobs via `.map()` for parallel execution
- Automatic dependency tracking through Job arguments
- Conditional logic based on intermediate results
- Shared directory usage (`ctx.shared_dir`)
- Multiple stages with mixed parallel/sequential execution

**Pipeline:**
1. Prepare shared dataset
2. Train multiple models in parallel (array job)
3. Evaluate all models in parallel (array job with dependencies)
4. Select best model based on results
5. Conditionally run final validation if threshold met
6. Generate comprehensive report

```bash
uv run python -m slurm.examples.ml_workflow --dataset mnist --num-configs 4
```

### Advanced Examples

#### `distributed_context.py`
Distributed computing example demonstrating multi-node execution.

#### `chain_jobs.py`
Job chaining and dependency management patterns.

#### `launch_with_preflight.py`
Advanced submission patterns with preflight checks.

## Running Examples

All examples support the following common arguments:

- `--slurmfile PATH`: Path to Slurmfile configuration (default: bundled example)
- `--env ENV`: Environment key from Slurmfile (default: `local`)

### Local Testing

To test examples locally without a SLURM cluster, use the bundled local backend:

```bash
uv run python -m slurm.examples.dependent_jobs
```

### Cluster Execution

To run on an actual SLURM cluster, create a Slurmfile with your cluster configuration:

```toml
[production.cluster]
backend = "ssh"
job_base_dir = "/home/user/slurm_jobs"

[production.cluster.backend_config]
hostname = "login.cluster.edu"
username = "your_username"

[production.packaging]
type = "container"
runtime = "podman"
dockerfile = "Dockerfile"
```

Then run with:

```bash
uv run python -m slurm.examples.dependent_jobs --env production
```

## Key Concepts

### Submitless Execution

Modern Slurm SDK examples use submitless execution where tasks return Jobs (Futures) when called:

```python
with Cluster.from_env() as cluster:
    # Calling task returns Job, not result
    job1 = generate_data(1000)
    
    # Pass Job as argument - automatic dependency!
    job2 = preprocess(job1)
    
    # Get result triggers submission and waits
    result = job2.get_result()
```

### Array Jobs with .map()

Use `.map()` for parallel execution over multiple inputs:

```python
# Train multiple models in parallel
configs = [{"lr": 0.001}, {"lr": 0.01}, {"lr": 0.1}]
train_jobs = train_model.map(configs)

# Get all results
results = train_jobs.get_results()
```

### Workflow Orchestration

Use `@workflow` for complex multi-stage pipelines:

```python
@workflow(time="02:00:00")
def my_pipeline(data: str, ctx: WorkflowContext):
    # ctx.shared_dir for workflow data
    # ctx.tasks_dir for task outputs
    
    job1 = task1(data)
    jobs = task2.map(items).after(job1)
    
    return final_task(jobs).get_result()
```

## Further Documentation

See the main documentation for more details:
- [Getting Started Guide](../../docs/guides/getting_started.md)
- [API Reference](../../docs/reference/api.md)
- [Submitless Execution Design](../../designs/submitless_execution_design.md)

