# Creating Tasks Dynamically

This guide shows how to create SLURM tasks dynamically at runtime using the `task()` function, rather than the `@task` decorator syntax.

## Why Create Tasks Dynamically?

Dynamic task creation is useful when:

- You want to wrap existing functions without modifying their source code
- You need to create tasks programmatically based on runtime conditions
- You're building a library where task configuration varies
- You want to apply different resource requirements to the same function

## Basic Usage

Instead of using `@task` as a decorator, you can call it as a function:

```python
from slurm import task

# Define a regular Python function
def process_data(input_file: str, threshold: float) -> dict:
    """Process data from a file."""
    # ... processing logic ...
    return {"status": "complete", "records": 1000}

# Wrap it as a SLURM task dynamically
process_task = task(process_data, time="00:30:00", mem="4G")

# Now it can be submitted to a cluster
job = cluster.submit(process_task)("data.csv", 0.5)
result = job.get_result()
```

## Complete Example

Here's a minimal working example:

```python
from slurm import task
from slurm.cluster import Cluster

# Regular Python function
def analyze(dataset: str, method: str) -> dict:
    print(f"Analyzing {dataset} using {method}")
    return {"dataset": dataset, "method": method, "score": 0.95}

# Create task dynamically
analyze_task = task(
    analyze,
    time="01:00:00",
    mem="8G",
    cpus_per_task=4
)

# Submit and run
cluster = Cluster.from_env("Slurmfile.toml")
job = cluster.submit(analyze_task)("experiments/data.csv", "random_forest")
job.wait()
print(job.get_result())
```

## Creating Multiple Task Variants

You can create different task configurations for the same function:

```python
def train_model(data_path: str, epochs: int) -> str:
    """Train a machine learning model."""
    # ... training logic ...
    return "model.pth"

# Quick test version
quick_train = task(train_model, time="00:10:00", mem="2G")

# Full training version
full_train = task(train_model, time="08:00:00", mem="32G", gpus=4)

# GPU version with container
gpu_train = task(
    train_model,
    time="24:00:00",
    mem="64G",
    gpus=8,
    container_file="train.Dockerfile"
)

# Use the appropriate version based on your needs
if quick_test:
    job = cluster.submit(quick_train)("data.csv", epochs=5)
else:
    job = cluster.submit(full_train)("data.csv", epochs=100)
```

## Wrapping Third-Party Functions

Dynamic task creation is especially useful for wrapping functions from libraries:

```python
from slurm import task
from slurm.cluster import Cluster
import pandas as pd

# Wrap a pandas operation as a SLURM task
process_csv = task(
    pd.read_csv,
    time="00:05:00",
    mem="4G",
    job_name="load_csv"
)

# Submit it
cluster = Cluster.from_env("Slurmfile.toml")
job = cluster.submit(process_csv)("large_dataset.csv")
df = job.get_result()
```

## Dynamic Task Factory Pattern

You can create factories that generate tasks based on parameters:

```python
from slurm import task

def create_analysis_task(complexity: str):
    """Create a task with resources based on complexity."""

    def analyze_data(input_file: str) -> dict:
        # ... analysis logic ...
        return {"status": "done"}

    if complexity == "low":
        return task(analyze_data, time="00:10:00", mem="2G")
    elif complexity == "medium":
        return task(analyze_data, time="01:00:00", mem="16G", cpus_per_task=8)
    else:  # high
        return task(analyze_data, time="08:00:00", mem="128G", cpus_per_task=32)

# Use the factory
medium_task = create_analysis_task("medium")
job = cluster.submit(medium_task)("data.csv")
```

## Best Practices

1. **Name your tasks explicitly** - Use the `job_name` parameter for clarity:
   ```python
   my_task = task(my_function, time="01:00:00", job_name="descriptive_name")
   ```

2. **Reuse task definitions** - Store commonly-used task configurations:
   ```python
   # Define once
   standard_cpu_task = lambda func: task(func, time="02:00:00", mem="8G", cpus_per_task=4)

   # Use multiple times
   task1 = standard_cpu_task(process_data)
   task2 = standard_cpu_task(analyze_results)
   ```

3. **Document dynamic tasks** - Since there's no decorator to see, add comments:
   ```python
   # Task: Heavy computation requiring 32GB RAM and 16 CPUs
   heavy_compute = task(compute_function, time="10:00:00", mem="32G", cpus_per_task=16)
   ```

## Comparison: Decorator vs Function Call

Both approaches create the same SlurmTask object:

```python
# Using decorator
@task(time="01:00:00", mem="4G")
def process_a(data: str) -> str:
    return data.upper()

# Using function call
def process_b(data: str) -> str:
    return data.upper()

process_b_task = task(process_b, time="01:00:00", mem="4G")

# Both work identically
job_a = cluster.submit(process_a)("hello")
job_b = cluster.submit(process_b_task)("hello")
```

Choose decorators for static task definitions and function calls for dynamic creation based on your use case.
