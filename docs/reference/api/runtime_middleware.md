# Runtime and Middleware

This page documents the generalized invocation runtime and middleware hook
contracts.

## Invocation runtime model

- A task call is delegated to the active `InvocationRuntime`.
- Runtime binding is context-driven:
  - `ClusterRuntime` in `with Cluster(...)` execution.
  - `WorkflowRuntime` inside workflow execution contexts.
- Extension runtimes (for example HA replay runtimes) can replace the active
  runtime through context binding.

## Middleware ordering

Hooks execute in declaration order for each middleware instance:

1. `before_invoke`
2. `transform_call`
3. `before_submit`
4. `after_submit`
5. `on_result`

## Runtime and hooks reference

::: slurm.core.protocols.InvocationRuntime

::: slurm.core.runtime.DefaultInvocationRuntime

::: slurm.core.runtime.ClusterRuntime

::: slurm.core.runtime.WorkflowRuntime

::: slurm.core.middleware.TaskMiddleware

::: slurm.core.middleware.InvokeContext

::: slurm.core.middleware.SubmitContext

::: slurm.core.middleware.ResultContext

::: slurm.core.middleware.NoopMiddleware

::: slurm.core.middleware.LoggingMiddleware
