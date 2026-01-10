# System Overview

Slurm SDK is built around a small set of components that turn Python functions into containerized Slurm jobs.

## Core components
- **Cluster** (`slurm.cluster`): Holds backend connection details, packaging defaults, and submission behavior.
- **Backend** (`slurm.api.*`): Submits scripts and fetches logs (SSH or local).
- **Packaging** (`slurm.packaging.container`): Builds or references container images for tasks.
- **Renderer** (`slurm.rendering`): Produces the sbatch script and wires up the runner.
- **Runner** (`slurm.runner`): Executes inside the job and calls your function.
- **Callbacks** (`slurm.callbacks`): Lifecycle events for logging and instrumentation.

## Component architecture

```mermaid
graph TB
    subgraph Client["Your Code"]
        Task["@task function"]
        Workflow["@workflow function"]
    end

    subgraph SDK["Slurm SDK"]
        Cluster
        Packaging
        Renderer
        Callbacks
    end

    subgraph Remote["Remote Cluster"]
        Backend
        SLURM
        Runner
        JobDir["Job Directory"]
    end

    Task --> Cluster
    Workflow --> Cluster
    Cluster --> Packaging
    Cluster --> Renderer
    Cluster --> Callbacks
    Cluster --> Backend
    Backend --> SLURM
    SLURM --> Runner
    Runner --> JobDir
    Callbacks -.->|events| Client
```

## Execution flow (high level)
```mermaid
graph TD
  A[Python task/workflow] --> B[Cluster.submit]
  B --> C[Packaging strategy]
  C --> D[render_job_script]
  D --> E[Backend.submit_job]
  E --> F[Slurm job starts]
  F --> G[Runner executes function]
  G --> H[Results written to job dir]
```

## What gets persisted
- **Job directory**: Scripts, stdout/stderr, pickled args/kwargs, and result files.
- **Workflow directory**: A root job directory plus per-task subdirectories.
- **Environment metadata**: `.slurm_environment.json` for workflow inheritance.

## Packaging decision flow

When you submit a task, the SDK determines how to package and deploy your code:

```mermaid
flowchart TD
    A[Submit Task] --> B{packaging=?}
    B -->|auto| C{pyproject.toml exists?}
    C -->|yes| D[Build wheel]
    C -->|no| E[No packaging]
    B -->|wheel| D
    B -->|none| E
    B -->|container:image| F{Dockerfile specified?}
    F -->|yes| G[Build & push container]
    F -->|no| H[Use existing image]
    D --> I[Upload to cluster]
    E --> I
    G --> I
    H --> I
    I --> J[Submit sbatch]
```

## Why this structure
- **Separation of concerns**: Packaging, rendering, and execution are isolated.
- **Debuggability**: The job directory is the source of truth for execution artifacts.
- **Repeatability**: Container builds and image references are explicit and traceable.
