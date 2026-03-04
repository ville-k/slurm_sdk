# Fluent High-Availability Workflow Design

## Objective

Design a generic high-availability workflow model that:

- Hides fault-tolerance mechanics behind fluent decorators
- Keeps task/workflow code imperative and business-logic-first
- Works across domains (`train`, `eval`, `dataprep`, `export`, etc.)
- Preserves resumability and deterministic recovery after supervisor preemption

This design replaces workflow-specific supervision APIs with composable wrappers.

## Core Design

### 1. `ha_task`: fluent wrapper around `@task`

`ha_task` returns an HA-aware callable that behaves like `SlurmTask` but carries
retry/reconciliation metadata.

Example:

```python
@ha_task(time="00:10:00", mem="8G")
def train_chunk(input: TrainChunkInput, io: AttemptIO) -> TrainChunkOutput:
    ...

train = (
    train_chunk
    .keyed_by("run_id", "epoch", "chunk")
    .with_retry(max_attempts=3, backoff_s=30)
    .with_resiliency(enabled=True, implementation="mock", max_restarts=1)
)
```

Call style:

```python
result_ref = train(
    run_id="run-123",
    epoch=0,
    chunk=2,
    checkpoint_in="...",
)
```

Inside `@ha_workflow`, this call means:

- derive deterministic work key
- reconcile any prior attempts
- submit if needed
- return a stable result reference

Outside `@ha_workflow`, behavior can default to direct submission (optional).

### 2. `ha_workflow`: generic supervisory wrapper around `@workflow`

`ha_workflow` provides:

- stable `run_dir` state
- single-writer lease lock
- reconcile loop based on `result.json` commit records
- retry/backoff policy execution
- continuation-safe replay after preemption

Example:

```python
@ha_workflow(time="00:20:00", mem="1G", cpus_per_task=1)
def production_flow(cfg: RunConfig, ctx: WorkflowContext):
    for shard in cfg.shards:
        prep_ref = dataprep.with_retry(2)(run_id=cfg.run_id, shard=shard)
        model_ref = train.with_retry(3)(run_id=cfg.run_id, shard=shard, prep=prep_ref)
        export.with_retry(2)(run_id=cfg.run_id, shard=shard, model=model_ref)
```

The imperative body is replayed on restart; each call is idempotent because of
stable keying + persisted attempt state.

## Why This Is Generic

Generic HA does not need train/eval knowledge. It only needs a **work-item
contract**:

- key derivation
- input serialization
- attempt directory layout
- result commit format
- status classification (`SUCCESS`, `RETRYABLE_ERROR`, etc.)

Everything else (retry logic, lock, reconciliation, replay) is shared.

## Protocols and Contracts

### Status

Use a common status enum:

- `SUCCESS`
- `RETRYABLE_ERROR`
- `NONRETRYABLE_ERROR`
- `PREEMPTED`
- `TIMEOUT`
- `CANCELLED`

### Input/Output protocol

Use a lightweight protocol for task implementors:

```python
from typing import Protocol, Mapping, Any

class HAInput(Protocol):
    def to_json(self) -> Mapping[str, Any]: ...

class HAOutput(Protocol):
    def to_json(self) -> Mapping[str, Any]: ...
```

Dataclasses are a practical default:

```python
@dataclass
class TrainChunkInput:
    run_id: str
    epoch: int
    chunk: int
    checkpoint_in: str | None = None
```

### Attempt runtime protocol

```python
class AttemptIO(Protocol):
    output_dir: Path
    progress_path: Path
    checkpoint_out_path: Path
    metrics_path: Path
    def stop_requested(self) -> bool: ...
    def write_progress(self, payload: Mapping[str, Any]) -> None: ...
    def write_checkpoint(self, payload: Mapping[str, Any]) -> None: ...
    def append_metric(self, payload: Mapping[str, Any]) -> None: ...
```

### Task unit protocol

```python
class HATaskUnit(Protocol):
    def key(self, raw_args: Mapping[str, Any]) -> str: ...
    def decode_input(self, raw_args: Mapping[str, Any]) -> HAInput: ...
    def run(self, input: HAInput, io: AttemptIO) -> HAOutput: ...
    def summarize(self, output: HAOutput) -> str: ...
    def classify_exception(self, exc: BaseException) -> str: ...
```

`ha_task` can provide defaults so users only implement `run` in common cases.

## Fluent API Surface

The wrapper should be immutable and chainable:

- `.keyed_by(*fields)`
- `.with_retry(max_attempts, backoff_s=..., jitter_s=...)`
- `.with_resiliency(enabled, implementation="none", max_restarts=0, ...)`
- `.with_timeouts(signal="B:USR1@60", preempt_grace_s=60)`
- `.with_artifacts(layout=...)`
- `.with_classifier(classifier_fn)`

All methods return a new wrapped task with updated policy.

## Replay and Preemption Semantics

### Replay model

When supervisor restarts, workflow body is replayed from the top. Each
`ha_task(...)` call is interpreted as `ensure_work_item(desired_spec)`.

Per call, engine behavior:

1. Compute work key from declared key fields or custom key function.
2. Load work state from `state.json` + attempt directories.
3. If a committed terminal result exists, return existing `ResultRef`.
4. If active attempt exists, keep waiting/reconcile (no duplicate submission).
5. If retryable failure and budget remains, create next attempt.
6. If non-retryable or attempts exhausted, mark run failed.

This gives deterministic recovery without explicit planner calls.

### Dynamic control flow

Dynamic branches based on unavailable upstream values need one of:

- value placeholder semantics (`ResultRef.await_value()`)
- deferred evaluation pattern (`if ref.is_ready(): ...`)
- replay-safe pending sentinel handled by the supervisor loop

For first implementation, prefer static/deterministic branching and task-level
encapsulation of dynamic decisions.

## Artifacts and State

Recommended layout under `run_dir`:

```text
run_dir/
  state/state.json
  state/events.jsonl
  state/supervisor.lock/
  <task_name>/
    key_<hash>/
      attempt_001/
        input.json
        progress.json
        result.json
        checkpoint_out.json
        metrics.jsonl
```

Key invariants:

- `result.json` is commit record and written last
- attempt directories are immutable once committed
- supervisor is single writer of canonical state
- all snapshots use atomic replace

## Dependency Semantics

`ResultRef` should represent logical dependency (work key + terminal artifact),
not a single job id. This is required because retries generate new job ids.

Passing `ResultRef` as task input is preferred over using raw `.after(job)` in
HA workflows.

## Implementation Strategy

### Phase 1: infrastructure

- Add `ha_task` wrapper type with fluent policy methods
- Add `ha_workflow` wrapper with lock/state/reconcile loop
- Reuse existing attempt/result/lock primitives from the HA example package

### Phase 2: ergonomic integration

- Add `ResultRef` contract
- Add stable keying defaults (`keyed_by`)
- Add serializer defaults for dataclass inputs/outputs

### Phase 3: advanced behavior

- add continuation/requeue strategy options
- add richer dependency composition
- support controlled dynamic branches

## Pros and Cons

### Pros

- Uniform HA behavior across workflow types
- Business logic remains imperative and concise
- Replay safety and resumability are built in
- Fluent API aligns with existing `SlurmTask` ergonomics

### Cons

- Hidden orchestration can make debugging less obvious
- Requires strict, stable keying discipline
- Dynamic branching is harder than static DAG-style flows
- Clear docs are mandatory to avoid semantic confusion around `__call__`

## Minimal User Experience Target

This should be a valid, readable end state:

```python
@ha_task(time="00:30:00", mem="16G")
def dataprep(input: DataPrepInput, io: AttemptIO) -> DataPrepOutput:
    ...

@ha_task(time="02:00:00", mem="64G")
def train(input: TrainInput, io: AttemptIO) -> TrainOutput:
    ...

@ha_workflow(time="00:20:00", mem="1G")
def pipeline(cfg: RunConfig, ctx: WorkflowContext):
    prep = dataprep.keyed_by("run_id", "shard").with_retry(2)
    trainer = train.keyed_by("run_id", "shard").with_retry(3)

    for shard in cfg.shards:
        prep_ref = prep(run_id=cfg.run_id, shard=shard)
        trainer(run_id=cfg.run_id, shard=shard, prep=prep_ref)
```

No explicit planner object is required in user code, while the runtime still
has the data needed for deterministic supervision.
