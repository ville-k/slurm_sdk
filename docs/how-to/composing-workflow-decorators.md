# How to Compose Middleware and Workflow Decorators

## Problem

You want workflows to inherit shared orchestration behavior (resource policy,
submission hooks, or audit tags) without duplicating logic in every workflow.

## Prerequisites

- You already use `@workflow`.
- You want to apply reusable behavior across multiple workflows.

## Steps

1. Define middleware for shared behavior.

```python
class EnforceWorkflowTag:
    def before_submit(self, ctx) -> None:
        ctx.kwargs.setdefault("workflow_tag", "standard")
```

2. Build a reusable workflow decorator.

```python
from slurm import workflow_decorator

ops_workflow = workflow_decorator(
    default_options={"time": "00:20:00", "mem": "2G"},
    middleware=(EnforceWorkflowTag(),),
)
```

3. Apply it to raw workflows or existing workflow handles.

```python
@ops_workflow
def orchestration_flow(ctx, dataset: str) -> str:
    return dataset


priority_flow = ops_workflow(orchestration_flow.with_options(partition="priority"))
```

4. Submit and verify options are applied.

```python
job = cluster.submit(priority_flow)(dataset="s3://bucket/train")
```

## Verification

- The workflow has merged SBATCH defaults from your custom decorator.
- Middleware hooks run in deterministic declaration order.
- `with_options(...)` still works after decoration.

## Troubleshooting

- If middleware appears to do nothing, confirm hooks mutate `ctx.args`/`ctx.kwargs`
  before submit (`transform_call` or `before_submit`).
- If stacked decorators conflict, apply the strictest policy decorator last.

## See also

- [Tasks and Workflows reference](../reference/api/tasks_workflows.md)
- [Runtime and Middleware reference](../reference/api/runtime_middleware.md)
- [Tutorial: custom task decorators](../tutorials/custom-task-decorator.md)
