# Callbacks and Events

Callbacks let you observe packaging, submission, execution, and workflow events without changing task code.

## Lifecycle stages

- **Packaging begin/end**: Inspect container build and image resolution.
- **Submit begin/end**: Track job submission metadata and target paths.
- **Run begin/end**: Observe execution timing and outcomes.
- **Workflow begin/end**: Monitor workflow orchestration on the client side.
- **Workflow task submitted**: Capture dependency edges for graph visualization.

## Callback timeline

```mermaid
sequenceDiagram
    participant Client
    participant SLURM
    participant Runner

    rect rgb(230, 245, 255)
        Note over Client: Client-side callbacks
        Client->>Client: on_begin_packaging
        Client->>Client: on_end_packaging
        Client->>Client: on_begin_submit_job
        Client->>SLURM: sbatch
        SLURM-->>Client: job_id
        Client->>Client: on_end_submit_job
    end

    rect rgb(255, 245, 230)
        Note over Runner: Runner-side callbacks
        SLURM->>Runner: Start job
        Runner->>Runner: on_begin_run
        Runner->>Runner: Execute task function
        Runner->>Runner: on_end_run
    end

    rect rgb(230, 255, 230)
        Note over Client: Workflow callbacks (client-side)
        Client->>Client: on_begin_workflow
        loop For each child task
            Client->>Client: on_workflow_task_submitted
        end
        Client->>Client: on_end_workflow
    end
```

## Execution loci
Callbacks can be configured to run on the client or on the runner. The SDK checks `should_run_on_client` and `should_run_on_runner` to decide where each callback fires.

## Serialization rules
Callbacks that need to run on the runner are pickled and shipped alongside the job script. Lightweight logging callbacks typically run only on the client.

## Typical uses

- Structured logging and progress output.
- Dependency graph visualization.
- Custom metrics or telemetry hooks.
