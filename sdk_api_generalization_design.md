# SDK API Generalization Design

## Status

Draft proposal (breaking changes allowed).

## Objective

Refactor the SDK API so users can compose and author custom task/workflow decorators
that encapsulate concerns (fault tolerance, retries, observability, policy, validation,
resource presets) without modifying SDK internals.

Primary goals:

- Minimize end-user cognitive load for common cases.
- Support gradual revelation of complexity for advanced users.
- Enable decorator composition without metadata loss.
- Avoid hard-coding special orchestration behavior into core `task`/`workflow`.
- Preserve a clean imperative authoring style for workflow business logic.

## Problem Summary (Current Architecture)

Current behavior is strong for basic submission but constrains extensibility:

- `Cluster.submit()` expects `SlurmTask` concretely (not a protocol), limiting wrappers.
- `SlurmTask.with_options()`/`with_dependencies()` reconstruct plain `SlurmTask`,
  dropping wrapper-specific metadata.
- `SlurmTask.__call__()` is fixed to “submit immediately, return `Job`”, making
  alternative call semantics hard (for example `ResultRef`-based supervision).
- Placeholder/dependency model is tightly coupled to `Job` IDs, which is not ideal
  for logical retries and replay across attempts.
- Workflow context injection and runner plumbing are mostly internal and not modeled
  as an extension surface.

These constraints make decorator composition fragile and push advanced behavior
into application-specific supervisors.

## Design Principles

1. Core API should be protocol-oriented, not class-identity-oriented.
2. Decorators should produce immutable, clonable task/workflow objects.
3. Concerns should compose via middleware/hooks, not subclass overrides alone.
4. Default ergonomics should remain simple (`@task`, `@workflow`, call like function).
5. Advanced runtimes (HA, tracing, policy) should plug into the same call path.
6. Returned handles should express logical dependencies, not scheduler IDs only.

## Proposed Architecture

### 1. New Core Protocols

Introduce stable protocols used across cluster/runtime/decorators:

```python
from typing import Protocol, Any, Mapping, Callable, Optional

class TaskLike(Protocol):
    name: str
    sbatch_options: Mapping[str, Any]
    packaging: Mapping[str, Any] | None
    is_workflow: bool

    def __call__(self, *args: Any, **kwargs: Any) -> "InvocationRef": ...
    def with_options(self, **options: Any) -> "TaskLike": ...
    def after(self, *deps: "DependencyRef") -> "TaskLike": ...
    def clone_with(self, **changes: Any) -> "TaskLike": ...
    @property
    def unwrapped(self) -> Callable[..., Any]: ...

class WorkflowLike(TaskLike, Protocol):
    is_workflow: bool  # always True

class DependencyRef(Protocol):
    def scheduler_dependencies(self) -> list[str]: ...

class InvocationRef(Protocol):
    def wait(self, timeout: float | None = None) -> bool: ...
    def get(self, timeout: float | None = None) -> Any: ...
```

Notes:

- `TaskLike` replaces hard `SlurmTask` checks in core submission paths.
- `InvocationRef` is the common return type. `Job` becomes one implementation.
- HA or other runtimes can return non-`Job` refs while preserving dependency semantics.

### 2. Unified Task/Workflow Definition Objects

Replace current ad-hoc copied attributes with explicit immutable specs:

```python
@dataclass(frozen=True)
class TaskSpec:
    func: Callable[..., Any]
    sbatch_options: dict[str, Any]
    packaging: dict[str, Any] | None
    flags: dict[str, Any]  # includes is_workflow, custom markers
    middleware: tuple["TaskMiddleware", ...]
```

`TaskHandle` holds a `TaskSpec` and implements `TaskLike`.
`WorkflowHandle` can be a thin specialization over `TaskHandle`.

Why:

- Every fluent modifier returns `clone_with(...)` preserving type and custom metadata.
- User-defined decorators can attach middleware/flags in a stable way.
- No silent metadata loss during `.with_options()` and related operations.

### 3. Invocation Runtime Abstraction

Introduce an invocation runtime bound to the active context:

```python
class InvocationRuntime(Protocol):
    def invoke(self, task: TaskLike, args: tuple[Any, ...], kwargs: dict[str, Any]) -> InvocationRef: ...
```

Default runtimes:

- `ClusterRuntime`: current behavior, returns `JobRef` (wrapper over `Job`).
- `WorkflowRuntime`: same default semantics but context-aware.

Optional runtimes:

- `HARuntime`: interprets calls as `ensure_work_item`, returns logical `ResultRef`.
- `LocalRuntime`: for testing/simulation.

`TaskHandle.__call__` becomes:

1. Resolve active runtime from context.
2. Delegate to `runtime.invoke(...)`.
3. Return `InvocationRef`.

This is the key decoupling that makes custom decorators first-class.

### 4. Middleware Pipeline (Extensibility Surface)

Add middleware hooks for submission/execution lifecycle:

```python
class TaskMiddleware(Protocol):
    def before_invoke(self, ctx: "InvokeContext") -> None: ...
    def transform_call(self, ctx: "InvokeContext") -> None: ...
    def before_submit(self, ctx: "SubmitContext") -> None: ...
    def after_submit(self, ctx: "SubmitContext", ref: InvocationRef) -> InvocationRef: ...
    def on_result(self, ctx: "ResultContext") -> None: ...
```

Middleware examples:

- Retry policy decoration.
- Fault classification decoration.
- Resource policy enforcement.
- Tracing and metrics correlation.
- Serialization policy and schema validation.

User decorators become pure middleware/spec transformers rather than “magic wrappers”.

### 5. Generalized Reference Model

Define two built-in reference implementations:

- `JobRef`: wraps scheduler job ID + existing methods.
- `ResultRef`: logical work identity + terminal artifact location(s), retry-aware.

Both satisfy `InvocationRef`. For dependencies:

- `JobRef.scheduler_dependencies()` returns concrete job IDs.
- `ResultRef.scheduler_dependencies()` returns current active attempt job IDs or empty
  (runtime decides best strategy).

This supports deterministic replay and logical retries without leaking attempt IDs.

### 6. Serialization/Placeholder Generalization

Replace `JobResultPlaceholder` with generic `RefPlaceholder`:

```python
@dataclass(frozen=True)
class RefPlaceholder:
    ref_type: str
    payload: dict[str, Any]
```

Resolver registry:

- `job` resolver (existing behavior).
- `ha_result` resolver (logical result lookup).
- User-registered resolver keys.

Benefits:

- Avoid `Job`-only coupling.
- Enable custom decorator ecosystems with custom ref types.
- Fix nested structure resolution consistently.

### 7. Decorator Composition API

Expose helper APIs for users to author decorators cleanly:

```python
def task_decorator(
    *,
    middleware: tuple[TaskMiddleware, ...] = (),
    default_options: dict[str, Any] | None = None,
    mutate_spec: Callable[[TaskSpec], TaskSpec] | None = None,
) -> Callable[[TaskLike], TaskLike]: ...

def workflow_decorator(...same shape...) -> Callable[[WorkflowLike], WorkflowLike]: ...
```

User-defined decorators should work in two modes:

- Decorate Python functions directly.
- Decorate existing `TaskLike`/`WorkflowLike` objects (post-`@task` composition).

## API Shape (Target)

### Base User API

```python
@task(time="00:10:00", mem="4G")
def preprocess(path: str) -> str:
    ...

@workflow(time="00:20:00")
def pipeline(ctx: WorkflowContext):
    out = preprocess("data.csv")
    return out.get()
```

### Custom Decorator Authoring

```python
def gpu_task(*, gpus: int = 1, **defaults):
    return task_decorator(
        default_options={"partition": "gpu", "gpus": gpus, **defaults},
        middleware=(GpuPolicyMiddleware(),),
    )

@gpu_task(time="01:00:00", mem="16G")
def train_model(cfg: dict) -> dict:
    ...
```

### HA as a Normal Extension

```python
@ha_task(retries=3, key_by=("run_id", "epoch", "chunk"))
def train_chunk(input: TrainChunkInput, io: AttemptIO) -> TrainChunkOutput:
    ...

@ha_workflow(run_dir="/runs/my_run")
def train_eval(ctx: WorkflowContext):
    ref = train_chunk(run_id="r1", epoch=0, chunk=0)
    return ref.get()
```

No SDK-internal special cases are required for HA if `HARuntime` + middleware exist.

## Breaking Changes (Intentional)

1. `@task`/`@workflow` return type changes:
   - From concrete `SlurmTask` assumptions to `TaskHandle` implementing `TaskLike`.
2. `Cluster.submit()` signature changes:
   - Accept `TaskLike` instead of requiring `SlurmTask` instance checks.
3. Call return type abstraction:
   - `__call__` returns `InvocationRef` (with `JobRef` default), not guaranteed concrete `Job`.
4. Placeholder model changes:
   - Replace `JobResultPlaceholder` pipeline with generic `RefPlaceholder`.
5. Fluent clone semantics:
   - `.with_options()`, `.after()`, and other modifiers must preserve concrete handle type/spec.
6. Context/runtime model:
   - Internal context stores active `InvocationRuntime`, not only cluster/workflow objects.

Given project stage, clean break is preferable to compatibility shims.

## Migration Strategy

Even with breaking changes, keep migration straightforward:

### Phase A: Introduce New Core Types

- Add `TaskLike`, `InvocationRef`, `DependencyRef`, `InvocationRuntime`.
- Implement `TaskHandle` and `JobRef`.
- Update `Cluster.submit()` to consume `TaskLike`.

### Phase B: Replace Legacy Plumbing

- Move submission internals from `SlurmTask` assumptions to `TaskSpec`.
- Replace placeholder resolver with registry-based refs.
- Convert dependency extraction to `DependencyRef` protocol.

### Phase C: Implement Decorator Authoring Surface

- Add `task_decorator` and `workflow_decorator` helper APIs.
- Add middleware base interfaces and invoke/submit hooks.
- Publish “custom decorator cookbook” examples.

### Phase D: Build HA Extension on New Surface

- Re-implement example HA using runtime + middleware, no bespoke supervisor coupling
  to train/eval types.
- Validate generic applicability (`dataprep`, `export`, `eval`, etc.).

### Phase E: Remove Legacy Types

- Remove strict `SlurmTask` checks and compatibility branches.
- Keep a thin deprecated alias only if needed during one release window.

## Testing Plan

Add/adjust tests around the new abstraction boundaries:

- `TaskLike` acceptance in `Cluster.submit()`.
- Modifier cloning preserves extension metadata/middleware.
- Custom decorator stacking order and determinism.
- Reference placeholder serialization/resolution for nested structures.
- Runtime swap tests (`ClusterRuntime` vs `HARuntime`) with identical task code.
- Existing integration coverage for standard submission path to prevent regressions.

## Documentation Plan

Documentation should explicitly separate:

- Tutorial: create first custom decorator.
- How-to: add middleware for retries/observability/resource policies.
- Reference: `TaskLike`, `InvocationRef`, middleware hook contracts.
- Explanation: runtime model and why logical refs reduce replay complexity.

## Risks and Mitigations

Risk: over-generalized API becomes difficult to understand.
Mitigation: keep default path minimal and hide extension APIs from quickstart docs.

Risk: middleware ordering ambiguity.
Mitigation: define strict deterministic order and conflict rules in reference docs.

Risk: type complexity for users.
Mitigation: provide narrow typed aliases for common patterns and pragmatic examples.

Risk: performance overhead in generalized invocation.
Mitigation: keep middleware no-op fast path and avoid repeated deep-copying of specs.

## Acceptance Criteria

This proposal is successful when:

1. A user can author a custom task decorator with no SDK patches.
2. Decorator-composed tasks preserve behavior through `.with_options()`, `.after()`, and `.map()`.
3. `Cluster.submit()` accepts custom task handles implementing protocol contracts.
4. Logical refs and placeholder resolution support non-`Job` runtimes.
5. HA behavior can be implemented as extension runtime/middleware, not task-specific supervisor classes.

## Out of Scope (This Proposal)

- Full implementation details for one specific HA policy.
- UI/CLI redesign.
- Backward-compatibility shims beyond minimal transitional support.

