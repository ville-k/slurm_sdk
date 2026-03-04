# Generalization Implementation Plan

## Purpose

Execution plan for implementing the API generalization described in
`sdk_api_generalization_design.md`, with milestone checklists that agents can
mark as work completes.

This plan assumes breaking changes are acceptable.

## Status Legend

- `[ ]` not started
- `[-]` in progress
- `[x]` done
- `[!]` blocked

## Global Constraints

- Keep `@task`/`@workflow` default UX simple.
- Prioritize protocol-based extensibility over concrete type checks.
- Prefer clean breaks over compatibility shims unless a shim reduces migration risk materially.
- Keep each milestone mergeable and testable on its own.

## Milestone Board

- [x] M0: Design lock + scaffolding
- [x] M1: Core protocols and handles
- [x] M2: Invocation runtime + context refactor
- [x] M3: `Cluster.submit` generalization
- [x] M4: Fluent clone semantics + dependency API cleanup
- [x] M5: Reference/placeholder generalization
- [x] M6: Middleware + custom decorator authoring API
- [x] M7: HA extension proof via new API
- [x] M8: Docs, examples, cleanup, release notes

## M0: Design Lock + Scaffolding

### Goal

Freeze naming and module layout before refactor churn.

### Checklist

- [x] Confirm core type names (`TaskLike`, `WorkflowLike`, `InvocationRef`, `DependencyRef`, `InvocationRuntime`).
- [x] Confirm handle names (`TaskHandle`, `WorkflowHandle`, `JobRef`).
- [x] Decide exact module locations (recommended: `src/slurm/core/` subtree).
- [x] Create architecture note with final module map and import boundaries.
- [x] Define temporary migration policy for old APIs during implementation window.

### Exit Criteria

- [x] All contributors agree on names and file layout.
- [x] No open architecture decisions blocking M1.

## M1: Core Protocols and Handles

### Goal

Introduce protocol-first domain model and immutable specs.

### Checklist

- [x] Add protocol definitions (`TaskLike`, `WorkflowLike`, `InvocationRef`, `DependencyRef`, `InvocationRuntime`).
- [x] Add immutable `TaskSpec` dataclass.
- [x] Implement `TaskHandle` with `clone_with`.
- [x] Implement `WorkflowHandle` specialization.
- [x] Keep `.unwrapped` access on handles.
- [x] Export new public types from `src/slurm/__init__.py`.
- [x] Add unit tests for cloning/immutability and protocol conformance.

### Suggested Test Scope

- `tests/test_task_decorator.py`
- new: `tests/test_task_handle_clone.py`
- new: `tests/test_protocol_conformance.py`

### Exit Criteria

- [x] Handles can represent tasks/workflows independently of legacy `SlurmTask`.
- [x] Protocol tests pass.

## M2: Invocation Runtime + Context Refactor

### Goal

Make task invocation runtime-driven instead of hard-coded in task `__call__`.

### Checklist

- [x] Add runtime registry/context state in `src/slurm/context.py`.
- [x] Implement default `ClusterRuntime` and `WorkflowRuntime`.
- [x] Refactor task call path to `runtime.invoke(task, args, kwargs)`.
- [x] Preserve context-managed behavior (`with Cluster(...)`) using runtime binding.
- [x] Add tests for runtime switching and context isolation.

### Suggested Test Scope

- `tests/test_context.py`
- `tests/test_context_execution.py`
- new: `tests/test_invocation_runtime.py`

### Exit Criteria

- [x] `TaskHandle.__call__` no longer performs direct submission logic.
- [x] Runtime selection is deterministic in cluster/workflow contexts.

## M3: `Cluster.submit` Generalization

### Goal

Accept protocol-compatible task objects instead of concrete `SlurmTask`.

### Checklist

- [x] Change `Cluster.submit()` to accept `TaskLike`.
- [x] Remove hard `isinstance(..., SlurmTask)` gate.
- [x] Ensure submit path reads task metadata from protocol fields/spec.
- [x] Update workflow detection (`is_workflow`) to protocol/flag-driven logic.
- [x] Ensure container dependency prebuild path works with generalized task handles.
- [x] Update related callback payload construction to use generalized task metadata.
- [x] Add tests for custom `TaskLike` wrappers with submission.

### Suggested Test Scope

- `tests/test_with_dependencies.py`
- `tests/test_workflow_callbacks.py`
- `tests/test_workflow_slurmfile_generation.py`
- new: `tests/test_cluster_submit_tasklike.py`

### Exit Criteria

- [x] `Cluster.submit()` works with generalized handles.
- [x] Existing submit/callback behavior remains functionally equivalent for default decorators.

## M4: Fluent Clone Semantics + Dependency API Cleanup

### Goal

Ensure fluent operations preserve concrete handle type and extension metadata.

### Checklist

- [x] Rebuild `.with_options()` on top of `clone_with`.
- [x] Rebuild `.after()` without metadata loss.
- [x] Rework dependency wrapper to avoid concrete-type downcast.
- [x] Ensure `.map()` composition still works after `.after()` and `.with_options()`.
- [x] Ensure workflow/container dependency APIs preserve custom wrapper metadata.
- [x] Add regression tests for chained fluent composition.

### Suggested Test Scope

- `tests/test_after_dependencies.py`
- `tests/test_array_jobs.py`
- `tests/test_dependencies.py`
- `tests/test_type_safety.py`

### Exit Criteria

- [x] No wrapper metadata loss across fluent chains.
- [x] Composition tests pass for default and custom decorators.

## M5: Reference/Placeholder Generalization

### Goal

Support non-`Job` references and resolver extensibility.

### Checklist

- [x] Add `RefPlaceholder` structure.
- [x] Add resolver registry (built-in `job`, extension hooks for custom refs).
- [x] Implement `JobRef` as `InvocationRef` + `DependencyRef`.
- [x] Update argument serialization to encode generalized refs.
- [x] Update runner resolution to decode generalized placeholders.
- [x] Ensure nested lists/dicts/tuples of refs are resolved.
- [x] Add tests for nested dependency resolution and custom resolver registration.

### Suggested Test Scope

- `tests/test_dependencies.py`
- `tests/test_union_type_signatures.py`
- new: `tests/test_ref_placeholder_registry.py`
- new: `tests/test_nested_ref_resolution.py`

### Exit Criteria

- [x] Placeholder resolution is not hard-coded to `Job` only.
- [x] Nested ref resolution works reliably.

## M6: Middleware + Custom Decorator Authoring API

### Goal

Make custom decorator composition a first-class public feature.

### Checklist

- [x] Add middleware protocol/interfaces and invocation hook pipeline.
- [x] Add `task_decorator(...)` helper.
- [x] Add `workflow_decorator(...)` helper.
- [x] Support decorating both raw functions and existing task/workflow handles.
- [x] Provide conflict/ordering rules for stacked decorators.
- [x] Add built-in sample middleware (for example logging or resource policy).
- [x] Add tests for stacking custom decorators and middleware ordering determinism.

### Suggested Test Scope

- new: `tests/test_custom_task_decorators.py`
- new: `tests/test_custom_workflow_decorators.py`
- new: `tests/test_middleware_ordering.py`

### Exit Criteria

- [x] Users can author and compose decorators without SDK internal patches.
- [x] Middleware ordering is deterministic and documented.

## M7: HA Extension Proof via New API

### Goal

Prove that HA orchestration can be expressed as extension runtime/decorators.

### Checklist

- [x] Implement `HARuntime` (or equivalent extension runtime) using new interfaces.
- [x] Add `ResultRef` implementation for logical work identity.
- [x] Port current HA example behavior to extension API (still example-scoped).
- [x] Remove need for train/eval-specific supervisor coupling in the API surface.
- [x] Verify replay semantics and retry behavior under simulated preemption/failure.
- [x] Add focused tests for `ResultRef` behavior and replay idempotency.

### Suggested Test Scope

- `tests/test_ha_task_attempt.py`
- `tests/test_high_availability_training_example.py`
- new: `tests/test_ha_runtime_replay.py`
- new: `tests/test_result_ref_dependencies.py`

### Exit Criteria

- [x] HA behavior is implemented as extension composition, not SDK hard-coding.
- [x] Existing HA example remains green with new architecture.

## M8: Docs, Examples, Cleanup, Release Notes

### Goal

Finalize public surface and provide migration guidance.

### Checklist

- [x] Update API docs for new protocols and handles.
- [x] Add tutorial: writing a custom task decorator.
- [x] Add how-to: composing middleware and custom workflow decorators.
- [x] Add reference docs for middleware hook contracts and runtime model.
- [x] Update examples to use new extension APIs.
- [x] Add migration guide for breaking changes.
- [x] Update `docs/CHANGELOG.md` under `## [Unreleased]`.
- [x] Remove deprecated compatibility code scheduled for deletion.

### Suggested Validation Commands

- [x] `uv format`
- [x] `uv run ruff check --fix`
- [x] `uv run pytest`
- [x] `uv run mkdocs build`

### Exit Criteria

- [x] Documentation and examples reflect final APIs.
- [x] Full test suite and docs build pass.

## Cross-Cutting Technical Checklist

- [x] Type hints remain strict and coherent across public API.
- [x] No duplicate context-injection logic remains in runner modules.
- [x] Callback behavior preserved or intentionally versioned.
- [x] Job directory/result persistence semantics are unchanged unless explicitly redesigned.
- [x] New abstractions do not regress packaging/container workflows.

## Agent Execution Checklist Template

Use this for each implementation PR/task batch:

- [ ] Milestone and task IDs referenced in PR title/description.
- [ ] Scope limited to one milestone slice (or explicitly justified cross-slice change).
- [ ] Unit tests added/updated for changed behavior.
- [ ] Integration tests run for affected flows.
- [ ] Docs updated for any public API change in the slice.
- [ ] Changelog updated (user-facing deltas only).
- [ ] Remaining unchecked boxes in affected milestone are listed in PR notes.

## Suggested Milestone Ownership Splits (Parallel Work)

- Agent A: M1 + M4 core handle semantics
- Agent B: M2 + M3 runtime and submit path
- Agent C: M5 resolver/ref pipeline
- Agent D: M6 decorator authoring + middleware
- Agent E: M7 HA extension and replay validation
- Agent F: M8 docs/migration/examples

## Final Definition of Done

- [x] All milestone board items checked.
- [x] No legacy concrete-type assumptions left in critical path.
- [x] Custom decorator authoring demonstrated by at least two non-trivial examples.
- [x] HA extension implemented without SDK-specific task-type coupling.
- [x] Full quality gates pass and release notes are ready.
