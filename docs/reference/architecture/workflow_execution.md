# Workflow Execution

Workflows are Python functions decorated with `@workflow`. They orchestrate multiple tasks and can submit additional jobs while running on the cluster.

## Key pieces
- **WorkflowContext**: Injected automatically into workflow functions. It provides a `cluster` instance, the workflow job directory, and a shared directory for artifacts.
- **Nested submissions**: Tasks invoked inside a workflow submit new jobs using the same cluster configuration.
- **Packaging inheritance**: Child tasks reuse the parent container image when possible, so they do not rebuild images mid-workflow.

## What happens at runtime
1. The workflow job starts and the runner creates a `WorkflowContext`.
2. Environment metadata is written into the workflow job directory (`.slurm_environment.json`).
3. `SLURM_SDK_PACKAGING_CONFIG` is exported so child submissions inherit the container image.
4. Child tasks write their outputs under `workflow_job_dir/tasks/<task-name>/<timestamp>/`.

## Error handling
- If cluster initialization fails inside the workflow, task submission is blocked early with a clear error.
- The workflow job still captures stdout/stderr in the usual job directory.

## Tips
- Keep workflow orchestration lightweight; heavy computation should live in task functions.
- Use the shared directory to pass large artifacts between tasks.
- Attach callbacks to observe workflow-level events and dependency graphs.
