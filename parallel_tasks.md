# Parallel Tasks Within a Single Job: API Design

> **Status:** Design draft. Target release 0.6.0 (core) / 0.7.0 (advanced topology).
> **Primitive:** Slurm job steps (`srun` inside an `sbatch` script).

## 1. Motivation

Users want to co-schedule **multiple distinct functions inside one Slurm allocation**. Two recurring shapes:

1. **Leader + helpers.** An ML training job with a metrics collector, tensorboard server, or data prefetcher alongside it. The helpers exist only to support the primary task and die with it.
2. **Equal peers.** Coupled simulation (ocean / atmosphere / coupler), heterogeneous MPMD workloads, or ensembles that must share topology — every peer is load-bearing and the job fails if any fails.

Today each `@task` is its own `sbatch` submission. Running N cooperating processes means either:

- N independent jobs (wasteful, coordination is manual), or
- Stuffing everything into one `@task` with hand-rolled `subprocess` management (defeats the decorator).

Slurm already solves this — a single allocation can run multiple `srun` steps concurrently, each with its own resources, container image, and stdout stream. We just need the SDK to expose it.

## 2. Design principles

These follow from the rest of the SDK:

1. **Function-centric.** Users write `@task`-decorated Python functions. A "sidecar" is a deployment role, not a property of the function — there is no `@sidecar` decorator. The same function can be a primary today and a helper tomorrow.
2. **Fluent & composable.** Parallel steps compose with the existing `.after(*jobs)`, `.with_options(**opts)`, `.map(items)` chain. Nothing new gets a bespoke builder.
3. **Context injection, not string sentinels.** Runtime metadata (job dir, step id, rank, shutdown signal) reaches the function through `JobContext` — the same mechanism `@task` already uses. No `"auto"` / `"inherit"` magic strings.
4. **Immutable task objects.** Every modifier returns a new `SlurmTask` or `BoundTask`. Users never mutate in place.
5. **Conceptually close to Slurm.** The abstraction is "steps in a job," which is what Slurm calls them. The rendered script is obvious `srun … &` + `wait`. No hidden orchestration layer.

## 3. The two APIs

One primitive at a time. Both produce a single `sbatch` submission with multiple `srun` steps.

### 3.1 `task.with_sidecars(...)` — leader + helpers (asymmetric)

```python
from slurm import task, Cluster, JobContext

@task(time="04:00:00", gpus_per_node=4, mem="32G")
def train(config: dict, ctx: JobContext) -> dict:
    return run_training(config, ctx.output_dir)

@task(cpus_per_task=1, mem="2G")
def metrics(ctx: JobContext) -> None:
    while not ctx.shutdown_requested:
        log_gpu_stats(ctx.output_dir)
        time.sleep(30)

@task(cpus_per_task=2, mem="4G")
def tensorboard_server(ctx: JobContext) -> None:
    subprocess.run(["tensorboard", "--logdir", ctx.output_dir, "--port", "6006"])

with Cluster(backend_type="ssh", hostname="hpc") as cluster:
    job = train.with_sidecars(metrics, tensorboard_server)(config={"lr": 0.001})
    result = job.get_result()          # primary's return value
```

**Semantics.**

- The receiver of `.with_sidecars(...)` is the **leader**; its args bind on the outer `(...)` call. Its return value is the job result, its exit code is the job exit code.
- Each sidecar is a `SlurmTask` or `BoundTask` (see §3.3). Sidecars that need no caller-supplied args — the common case — are passed bare.
- When the leader exits (success or failure), sidecars receive a graceful shutdown (see §6). The overall job's outcome is the leader's.
- Sidecars are started _before_ the leader by the rendered script, but the SDK does not guarantee they are "ready" (listening on sockets, etc.) when the leader starts. Sidecars that need this should write a ready-file under `ctx.output_dir` and the leader should poll for it.

**Passing args to sidecars.** Use `.partial(...)` (§3.3):

```python
job = train.with_sidecars(
    metrics,                                # JobContext is enough
    tensorboard_server.partial(port=6006),  # extra kwargs captured now
)(config={"lr": 0.001})
```

### 3.2 `parallel(...)` — peers (symmetric)

```python
from slurm import task, parallel, Cluster

@task(cpus_per_task=32, mem="64G")
def ocean_model(params: dict) -> dict: ...

@task(cpus_per_task=32, mem="64G")
def atmosphere_model(params: dict) -> dict: ...

@task(cpus_per_task=8, mem="16G")
def coupler(params: dict) -> dict: ...

with Cluster(backend_type="ssh", hostname="hpc") as cluster:
    job = parallel(
        ocean_model.partial(params=ocean_cfg),
        atmosphere_model.partial(params=atmo_cfg),
        coupler.partial(params=coupler_cfg),
    )
    [ocean_r, atmo_r, coupler_r] = job.get_results()
```

**Named peers** — useful for clarity and dict-style result access:

```python
job = parallel(
    ocean=ocean_model.partial(params=ocean_cfg),
    atmo=atmosphere_model.partial(params=atmo_cfg),
)
results = job.get_results()     # {"ocean": ..., "atmo": ...}
results["ocean"]
```

**Semantics.**

- All peers start concurrently. The job succeeds iff **every** peer succeeds.
- **Fail-fast** is the default: if any peer fails, remaining peers are signalled to shut down (§6).
- Each peer's return value is retrievable individually.
- Positional-only form returns a list; keyword-only form returns a dict. Mixing positional and keyword is an error (avoids the surprising `list-with-a-side-dict` result type).

### 3.3 `SlurmTask.partial(*args, **kwargs)` — deferred binding

Today, calling `train(config=cfg)` inside a cluster context *submits* the job. That makes it hard to "bind args but not submit" — which both APIs above need.

`.partial()` returns a `BoundTask`:

```python
handle = train.partial(config={"lr": 0.001})    # captured, not submitted
```

`BoundTask` is what `with_sidecars` / `parallel` / future `parallel.map` accept. It is also the natural layer for a user to pre-configure a task once and pass it around. Outside of multi-step APIs, calling `handle(...)` is an error — use the plain task if you want a single submission.

`.partial()` composes with the rest of the fluent API:

```python
train.with_options(partition="gpu").after(prep).partial(config=cfg)
```

> **Why a new primitive?** We need something that captures args without submitting. Reusing `functools.partial` was tempting, but it would obscure the `SlurmTask` type and break `.with_options()` chaining. A first-class `BoundTask` keeps the type story clean.

## 4. Composition with the existing fluent API

Everything chains as expected; submission semantics match intuition.

| Composition                                           | Meaning                                                     |
| ----------------------------------------------------- | ----------------------------------------------------------- |
| `train.with_sidecars(m).after(prep)(cfg)`             | The whole allocation waits for `prep`.                      |
| `train.after(prep).with_sidecars(m)(cfg)`             | Same — order of `.after()` / `.with_sidecars()` is free.    |
| `train.with_sidecars(m).with_options(partition="gpu")(cfg)` | Outer `#SBATCH` picks up the partition.                     |
| `parallel(a, b).after(prep)`                          | Outer allocation waits for `prep`.                          |
| `parallel(a, b).get_results()` → `analyze.after(parallel_job)(...)` | Downstream task depends on all peers.              |
| `train.with_sidecars(m).map(configs)`                 | **Not supported in 0.6.** Each array element would need its own sidecar instance; see §11. |
| `parallel(task.map(items), other())`                  | **Not supported.** A peer may not itself be an array.       |

Inside `@workflow`, both APIs work identically — `parallel()` / `with_sidecars()` submit a single multi-step allocation that the workflow driver waits on like any other job.

## 5. Resource model

### 5.1 Outer allocation = union of steps

The SDK computes the `#SBATCH` directives from the step-level specs. "Union" is per-field:

| Resource                      | Union rule                                               |
| ----------------------------- | -------------------------------------------------------- |
| `cpus_per_task`, `mem`, `gpus`, `ntasks` | **Sum** across steps (steps run concurrently).  |
| `gpus_per_node`               | Sum; errors if the total exceeds `nodes * gpus_per_node`. |
| `nodes`                       | **Max**; must be given explicitly on the parent when > 1. |
| `time`                        | **Max** of step times (same clock for everyone).         |
| `partition`, `account`, `qos` | Must match across steps; mismatch is a validation error. |
| Any other SBATCH directive    | Must match across steps.                                 |

The outer union is overridable via `.with_options(...)` on the parent — useful when the sum overshoots what you want to request (e.g. you know steps won't actually use their nominal memory peak at the same time).

### 5.2 Per-step `srun` flags

Each step gets an `srun` invocation with `--ntasks`, `--cpus-per-task`, `--mem`, `--gpus[-per-task|-per-node]`, and `--exact` (to prevent oversubscription). Step-level resource specs come from the corresponding `@task(...)` decorator.

### 5.3 GPU placement

**Basic case (single node).** Slurm assigns non-overlapping GPU devices to steps. The user code only sees its own GPUs via `CUDA_VISIBLE_DEVICES`. No manual indexing needed.

**Multi-node topology** — see Appendix A.

### 5.4 Validation at submission time

Before we hand the script to `sbatch`, the SDK validates:

- Mismatched `partition` / `account` / `qos` → `SubmissionError`.
- Step resources exceed outer allocation → `SubmissionError` with the offending step name.
- Sum of `gpus-per-node` exceeds hardware (if known) → `SubmissionError`.
- An array-producing task used inside `with_sidecars` / `parallel` → `SubmissionError`.

## 6. Runtime / lifecycle model

### 6.1 Rendered script (leader + sidecars)

```bash
#!/bin/bash
#SBATCH --time=04:00:00
#SBATCH --gpus-per-node=4
#SBATCH --mem=38G
#SBATCH --cpus-per-task=11

# ... packaging setup ...
trap 'scancel --signal=TERM --batch "$SLURM_JOB_ID"' EXIT

# Sidecars first so they're at least launched before the leader
srun --exact --ntasks=1 --cpus-per-task=1 --mem=2G \
    --job-name="metrics" \
    "$PY_EXEC_RESOLVED" -m slurm.runner --step sidecar:0:metrics ... &
SIDE_0_STEP_ID=$!

srun --exact --ntasks=1 --cpus-per-task=2 --mem=4G \
    --job-name="tb_server" \
    "$PY_EXEC_RESOLVED" -m slurm.runner --step sidecar:1:tensorboard_server ... &
SIDE_1_STEP_ID=$!

# Leader (blocking)
srun --exact --ntasks=1 --cpus-per-task=8 --gpus=4 --mem=32G \
    --job-name="train" \
    "$PY_EXEC_RESOLVED" -m slurm.runner --step leader:train ...
LEADER_EXIT=$?

# Tell sidecars to shut down gracefully, then hard-cancel any stragglers
scancel --signal=TERM "$SLURM_JOB_ID.0" "$SLURM_JOB_ID.1" 2>/dev/null
sleep "${SLURM_SDK_SIDECAR_GRACE:-10}"
scancel "$SLURM_JOB_ID.0" "$SLURM_JOB_ID.1" 2>/dev/null
wait
exit $LEADER_EXIT
```

Key details:

- We use `scancel --signal=TERM <jobid>.<stepid>` instead of shell `kill`. A bare `kill` targets the local `srun` wrapper, not the remote step process — `scancel` is the Slurm-native way to propagate signals through the step.
- A grace period (`SLURM_SDK_SIDECAR_GRACE`, default 10s) between SIGTERM and the force cancel lets sidecars flush logs.
- `trap … EXIT` ensures an external `scancel` of the job cleans up stragglers instead of orphaning steps.

### 6.2 Rendered script (peers, fail-fast)

```bash
# ... all peers launched with srun --exact … & ...
FAILED=0
FAILED_STEP=""
while (( ${#STEP_PIDS[@]} )); do
    if wait -n -p DONE_PID "${STEP_PIDS[@]}"; then
        STEP_PIDS=( ${STEP_PIDS[@]/$DONE_PID} )
    else
        FAILED=$?
        FAILED_STEP=$DONE_PID
        break
    fi
done

if (( FAILED != 0 )); then
    scancel --signal=TERM "$SLURM_JOB_ID" 2>/dev/null
    sleep "${SLURM_SDK_PEER_GRACE:-10}"
    scancel "$SLURM_JOB_ID" 2>/dev/null
fi
wait
exit $FAILED
```

### 6.3 How sidecars know to stop: `JobContext.shutdown_requested`

The runner installs a SIGTERM handler that flips a thread-safe flag. Sidecar code polls it:

```python
@task(cpus_per_task=1)
def metrics(ctx: JobContext) -> None:
    while not ctx.shutdown_requested:
        log_gpu_stats(ctx.output_dir)
        time.sleep(30)
```

For sidecars that block in `subprocess.run(...)` (e.g. `tensorboard`), the SIGTERM propagates to the child automatically — no polling needed. `shutdown_requested` exists for pure-Python loops.

### 6.4 Sidecar failure policy

Configurable, with a safe default:

```python
train.with_sidecars(metrics, on_sidecar_failure="ignore")   # default
train.with_sidecars(metrics, on_sidecar_failure="kill")     # abort the leader too
train.with_sidecars(metrics, on_sidecar_failure="callback", callback=alert)
```

- `"ignore"` (default): a crashed sidecar does not affect the leader. Non-fatal helpers (metrics, logging) suit this.
- `"kill"`: a sidecar failure aborts the whole allocation; treat as if the leader itself had failed. Suitable when the sidecar is load-bearing (data server, parameter server).
- `"callback"`: a user-supplied function is invoked with the sidecar's `JobSnapshot`; return value decides whether to continue.

### 6.5 `JobContext` additions

```python
@dataclass(frozen=True)
class JobContext:
    # … existing fields …
    step_id: Optional[str]              # already exists (SLURM_STEP_ID)
    step_role: Literal["leader", "sidecar", "peer", "solo"] = "solo"
    step_name: Optional[str] = None     # SDK-supplied, e.g. "metrics"
    shared_dir: Optional[Path] = None   # common scratch across steps (same for all steps in one job)

    @property
    def shutdown_requested(self) -> bool: ...
```

`shared_dir` is the convention for inter-step data — a subdirectory of the job directory created by the runner before any step starts.

## 7. Packaging model (per-step container images)

Per-step `packaging=` on `@task` determines each step's `srun --container-image=...`. Common cases:

```python
@task(packaging="container:nvcr.io/nvidia/pytorch:24.01-py3", gpus_per_node=4)
def train(config: dict) -> dict: ...

@task(packaging="container:prom/node-exporter:latest")
def host_metrics() -> None: ...

@task()                                 # no packaging → wheel → bare node
def light_sidecar(ctx: JobContext) -> None: ...
```

- If a sidecar has no `packaging` set, it **inherits** the leader's image. This is the common case — sidecars are just different entrypoints in the same environment.
- For per-step heterogeneous containers, `packaging.prepare()` is invoked per unique image: N images produced, each tagged and pushed.
- `packaging="none"` (new) runs a step on the bare node even when the leader is containerized — useful for host-level telemetry.

Implementation impact: the packaging pipeline becomes a list-of-configs resolution instead of single-config. No conceptual change.

## 8. Local mode

For `Cluster(backend_type="local")`, parallel steps run as peer subprocesses inside a single "allocation" (which is already just a subprocess for us):

- Each step launches as its own `python -m slurm.runner …` child.
- Shutdown uses SIGTERM + grace timer, same semantics as Slurm.
- No containerization in local mode; `packaging="container:…"` falls back to venv/wheel as today.
- This keeps the smoke test (`uv run python -m slurm.examples.hello_world`) sensibly exercisable.

## 9. Return types

```python
class SidecarJob(Job):
    """Returned by task.with_sidecars(...)(args). Behaves like Job; result is the leader's."""
    def sidecar_snapshots(self) -> Dict[str, JobSnapshot]: ...

class ParallelJob:
    """Returned by parallel(...). Heterogeneous peer steps."""
    def get_results(self) -> Union[List[Any], Dict[str, Any]]: ...
    def __getitem__(self, key: Union[int, str]) -> Job: ...  # individual step
    def wait(self, timeout: Optional[float] = None) -> bool: ...
    def snapshot(self) -> Dict[str, JobSnapshot]: ...
    def after(self, *jobs) -> "ParallelJob": ...             # pre-submission only
```

`SidecarJob` inherits from `Job` so existing `.get_result()`, `.get_stdout()`, `.wait()`, callbacks all work unchanged. `ParallelJob` is a new type that deliberately does **not** inherit from `Job` — it has no single result and plural semantics should be surfaced at the type level (parallel to how `ArrayJob` is its own type).

## 10. Submission timing

- `task.with_sidecars(...)(args)` — **eager**, on the call. Consistent with `task(args)`.
- `parallel(...)` — **eager**, on construction. Consistent with `task.map(items)` (`ArrayJob`).

Deferred submission is achieved the same way as everywhere else in the SDK: add `.after(dep)` before the call / construction, since `.after()` returns a new object but does not trigger submission.

## 11. Out of scope for 0.6

These deserve their own design passes:

- **`task.with_sidecars(...).map(items)`** — sidecars per array element. Interesting but requires resolving per-element step identity; defer until we see real user demand.
- **Peers that are themselves array jobs** — `parallel(task.map(items), other())`. Composition would combinatorially explode the step table.
- **Dynamic sidecar lifetime** — sidecars that the leader can start or stop at runtime. Current model is launch-at-start, shutdown-at-end.
- **Cross-step IPC abstractions** — we do not model MPI, named pipes, or shared memory. Steps that need IPC configure it themselves; Slurm shares the node network stack and the job directory, which covers most cases.

## 12. Non-goals

- **Replacing `ntasks`/`ntasks_per_node`.** Data-parallel (same function, many ranks, MPI) is already handled by the decorator's SBATCH options. `parallel()` is for *distinct* functions.
- **Step-level dependency DAG.** All steps start together. Sequential steps within one allocation are a separate feature (and usually better modeled as separate jobs with `.after()`).
- **Heuristic resource inference.** Users declare per-step resources with `@task(...)`. The SDK sums; it does not guess.

## 13. Implementation phasing

| Phase      | Scope                                                                                                                                    | Notes                                                 |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| **0.6.0**  | `BoundTask`, `.partial()`. `task.with_sidecars(*sidecars, on_sidecar_failure=...)`. `parallel(*peers, **named_peers)`. Multi-step script rendering. Runner `--step` mode. `JobContext.step_role`, `step_name`, `shared_dir`, `shutdown_requested`. Per-step container images (including `"none"` and inherit). Local mode parity. | Single-node only. Peer and leader-sidecar lifecycles. |
| **0.7.0**  | Advanced multi-node topology (§Appendix A). `--gpu-bind` passthrough. `srun_args=[...]` per-step escape hatch.                           | Gated on user demand for DGX-class workloads.        |
| **Later**  | `with_sidecars().map()`, array-of-parallel, dynamic sidecar lifetime.                                                                    | Keep notes in this doc as they come up.              |

**Shared implementation substrate** (both APIs use):

- `rendering.py` — new multi-step script renderer.
- `runner/__main__.py` — `--step <role>:<index>:<name>` flag selecting which bound function + args to execute.
- `_serialization.py` — per-step `step_<N>_args.pkl` / `step_<N>_result.pkl` files.
- `_submission.py` — resource union computation and per-step validation.
- `packaging/` — multi-image `prepare()` loop.

## 14. Alternatives considered

- **`@sidecar` decorator.** Rejected: a sidecar is a deployment role, not a function property. Same function should work as a leader in one script and a sidecar in another.
- **`"auto"` sentinel for sidecar args** (`monitor(job_dir="auto")`). Rejected: duplicates what `JobContext` already does in a grep-hostile way.
- **Tuple form for sidecar args** (`(metrics, {"port": 6006})`). Rejected: ad-hoc; `.partial()` is the existing-shape answer.
- **Unified `parallel.leader(primary=..., sidecars=[...])` vs `parallel(...)`.** Rejected: one verb for two lifecycles reads like a config dict. Two verbs express intent.
- **`StepGroup(primary=..., sidecars=[...])` builder.** Rejected: extra concept with no earning power over `.with_sidecars()`.
- **`functools.partial` instead of `BoundTask`.** Rejected: loses the `SlurmTask` type, breaks `.with_options()` chaining, and the type checker can't see through it.
- **Shell `kill` for step shutdown.** Rejected: Slurm-native `scancel --signal` propagates reliably through `srun`; `kill` targets the wrapper.

---

## Appendix A: multi-node topology

On DGX-class nodes with 8 GPUs each, steps sometimes want different GPU slices across multiple nodes. Example: 2-node job (16 GPUs total) where:

- 4 data generators each get 1 GPU (2 per node)
- 1 training step gets 12 GPUs (6 per node) for DDP

Slurm supports this via per-step `--gpus`, `--gpus-per-node`, `--gpus-per-task`, and `--nodes`:

```python
@task(ntasks=4, ntasks_per_node=2, gpus_per_task=1)
def generate(shard_id: int) -> dict: ...

@task(ntasks=2, ntasks_per_node=1, gpus_per_task=6)
def train(config: dict) -> dict: ...

job = parallel(
    generate.partial(shard_id=0),
    train.partial(config=cfg),
    nodes=2,                   # outer allocation parameters
    gpus_per_node=8,
)
```

The `nodes=` / `gpus_per_node=` kwargs on `parallel()` set the outer `#SBATCH` allocation — required when the union computation can't infer multi-node placement from step specs alone.

### Useful per-step flags

| Flag                         | Meaning                            |
| ---------------------------- | ---------------------------------- |
| `--gpus=N`                   | Total GPUs for this step           |
| `--gpus-per-task=N`          | GPUs per process within the step   |
| `--gpus-per-node=N`          | GPUs per node for this step        |
| `--ntasks=N`                 | Processes in this step             |
| `--ntasks-per-node=N`        | Processes per node                 |
| `--gpu-bind=map_gpu:0,1`     | Pin specific GPU indices           |
| `--exact`                    | Enforce strict resource accounting |

### Escape hatch

For advanced binding (NVLink-aware placement, NUMA pinning) the SDK does not attempt to model:

```python
train.with_options(srun_args=["--gpu-bind=closest", "--cpu-bind=ldoms"])
```

`srun_args` is appended verbatim to the step's `srun` line — last-resort knob for topology we don't yet have first-class support for.
