# Parallelization Patterns

Use this guide to explore fan-out/fan-in, pipeline stages, hyperparameter sweeps, and dynamic dependencies. The `slurm.examples.parallelization_patterns` example runs multiple patterns and prints progress for each phase.

## What it does
- Builds a container image from `src/slurm/examples/map_reduce.Dockerfile`.
- Runs several orchestration patterns in a single script.
- Demonstrates array jobs, dependency chaining, and dynamic task submission.

## Pattern Overview

### Fan-out / Fan-in

Process multiple items in parallel, then aggregate results:

```mermaid
graph LR
    subgraph Fan-out
        A[Input Data] --> B1[Task 1]
        A --> B2[Task 2]
        A --> B3[Task 3]
        A --> B4[Task N]
    end
    subgraph Fan-in
        B1 --> C[Aggregate]
        B2 --> C
        B3 --> C
        B4 --> C
    end
    C --> D[Final Result]
```

Use `.map()` for fan-out and `.after()` for fan-in aggregation.

### Pipeline

Sequential stages where each stage's output feeds into the next:

```mermaid
graph LR
    A[Raw Data] --> B[Stage 1: Preprocess]
    B --> C[Stage 2: Transform]
    C --> D[Stage 3: Validate]
    D --> E[Stage 4: Export]
```

Chain tasks using `.after()` or pass `Job` objects as arguments to automatically resolve dependencies.

### Hyperparameter Sweep

Run the same task with different configurations using array jobs:

```mermaid
graph TD
    A[Sweep Config] --> B[Array Job]
    B --> C1["Task[0]: lr=0.001"]
    B --> C2["Task[1]: lr=0.01"]
    B --> C3["Task[2]: lr=0.1"]
    B --> C4["Task[N]: lr=..."]
    C1 --> D[Collect Results]
    C2 --> D
    C3 --> D
    C4 --> D
    D --> E[Best Config]
```

Use `.map()` with a list of configuration dictionaries.

### Dynamic Dependencies

Submit tasks based on results from previous tasks:

```mermaid
graph TD
    A[Initial Task] --> B{Check Result}
    B -->|Condition A| C[Task Path A]
    B -->|Condition B| D[Task Path B]
    C --> E[Continue A1]
    C --> F[Continue A2]
    D --> G[Continue B1]
    E --> H[Final Merge]
    F --> H
    G --> H
```

Use `job.get_result()` to inspect results before deciding which tasks to submit next.

## Notes
- Use `--pattern` to run a single pattern (e.g. `fanout`, `pipeline`, `sweep`, `dynamic-deps`).
- The example prints per-pattern summaries so you can compare behaviors.

## Run this example
```bash
uv run python -m slurm.examples.parallelization_patterns \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --pattern all \
  --packaging container \
  --packaging-registry registry:5000/parallel-patterns \
  --packaging-platform linux/amd64 \
  --packaging-tls-verify false
```
