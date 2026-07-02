# Parallel Task Execution — Unified Design

> **Scope:** a single `parallel(...)` primitive that submits one Slurm allocation running 1…N peers — from "leader + helpers" to heterogeneous multi-pool deployments — with first-class peer discovery, named nodes, and failure policies.
> **Status:** design spec.

---

## 1. Motivation

Three shapes of workload, all of which should share one allocation:

1. **Leader + helpers** — a training task with a metrics collector, tensorboard server, or data prefetcher. Helpers exist only to support the primary. _Asymmetric._
2. **Equal peers** — coupled simulation (ocean / atmosphere / coupler), heterogeneous MPMD, or ensembles. Each peer is load-bearing. _Symmetric, fail-fast._
3. **Heterogeneous services** — RL training (e.g. [cosmos-rl](https://github.com/nvidia-cosmos/cosmos-rl)): one learner on an H100 node, an inference fleet on A100 nodes, CPU-only simulators, a replay buffer colocated with the learner. _Heterogeneous, service-discovery-heavy, fine-grained placement._

Slurm has the machinery for all three (job steps + heterogeneous jobs + feature constraints + GRES). The SDK job is to expose it as one coherent primitive whose simple form stays simple and whose advanced form is powerful and readable.

---

## 2. Design principles

1. **One primitive.** `parallel(...)` covers every shape. `with_sidecars(...)` exists only as sugar (§5.7).
2. **Function-centric.** Peers are `@task`-decorated functions. There is no `@sidecar`, `@peer`, `@leader`, or `@service` decorator — behaviour is a property of the `Peer(...)` wrapper, not the function.
3. **Fluent & immutable.** Every modifier (`.partial()`, `.after()`, `.with_options()`) returns a new object. Nothing mutates.
4. **Type-driven runtime injection.** `JobContext` is injected by parameter annotation. No magic strings (`"auto"`, `"inherit"`) in argument lists.
5. **Pool-first placement.** Co-location and isolation are expressed through pool membership. No `colocate_with` soup needed for the common case (but supported for precise control — §6.4).
6. **Uniform runtime surface.** Peer discovery (`ctx.peers`), node discovery (`ctx.nodes`), shutdown (`ctx.shutdown_requested`), shared directory (`ctx.shared_dir`) are present in every topology — including the trivial 1-peer case.
7. **Validate at submission.** Pool/peer/resource/placement errors surface before `sbatch`. A 24-hour RL job never fails after 30 seconds because a pool was mis-sized.
8. **Escape hatches are ugly on purpose.** Raw `srun_args=`, `extra_sbatch=`, `prolog=` / `epilog=` are available but grep-hostile so you know you left the paved path.

---

## 3. Conceptual model

Five things, no more:

| Concept       | What it is                                                                                                  | Compiles to                                                   |
| ------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| **`Peer`**    | One service — one task with placement + lifecycle directives. A replica set is a peer with `count > 1`.    | One `srun` step.                                              |
| **`Pool`**    | One homogeneous reservation: N nodes of a given shape, partition, constraint. Optionally with node labels. | One hetjob component (`#SBATCH ... hetjob`).                  |
| **`Topology`**| The set of named pools for one `parallel(...)` call.                                                       | The hetjob header structure.                                  |
| **Registry**  | Runtime directory of who landed where. Populated at bootstrap, updated by `ctx.announce()`.                | `$JOB_DIR/registry.json`.                                     |
| **Supervisor**| The Python process that owns lifecycle: launches steps, applies failure policies, handles restarts.        | `python -m slurm.topology_supervisor` inside the batch script. |

The entire design is these five concepts plus one entry point — `parallel(...)`.

---

## 4. Walkthrough

Each example introduces the minimum new vocabulary. Everything prior keeps working.

### 4.1 Two peers

```python
from slurm import task, parallel, Cluster

@task(cpus_per_task=32, mem="64G")
def ocean_model(cfg: dict) -> dict: ...

@task(cpus_per_task=32, mem="64G")
def atmosphere_model(cfg: dict) -> dict: ...

with Cluster(backend_type="ssh", hostname="hpc") as cluster:
    job = parallel(
        ocean_model.partial(cfg=ocean_cfg),
        atmosphere_model.partial(cfg=atmo_cfg),
        time="04:00:00",
    )
    results = job.get_results()       # {"ocean_model": ..., "atmosphere_model": ...}
```

- `.partial(...)` captures args without submitting — it returns a `BoundTask` (§5.5).
- A bare `BoundTask` or `SlurmTask` is auto-wrapped in `Peer(...)` with defaults.
- Default lifecycle: all peers must succeed. Any failure aborts the group. (Exact semantics: each peer defaults to `on_failure="kill"`; §9.)
- Default placement: one implicit `Pool` sized by the resource union of the peers' specs. One hetjob component.
- Result access is by peer name (derived from `__name__`).

### 4.2 Leader + helpers

```python
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

with cluster:
    job = parallel(
        Peer(train.partial(config={"lr": 0.001}), leader=True),
        Peer(metrics,            on_failure="continue"),
        Peer(tensorboard_server, on_failure="continue"),
    )
    result = job.leader_result       # train's return value
```

- `Peer(...)` wraps a task when you need to attach directives (`leader=True`, `on_failure=`, etc.).
- `leader=True` means "when this peer exits (success or failure), signal the rest to shut down gracefully." Multiple leaders are allowed; the first exit triggers shutdown.
- `on_failure="continue"` means "my failure is non-fatal; the group keeps running."
- `ctx.shutdown_requested` flips to `True` in helpers when the leader exits.
- `job.leader_result` is shorthand for the (unique) leader peer's result; raises if the job has zero or multiple leaders.

### 4.3 Sugar: `task.with_sidecars(...)`

The §4.2 pattern is common enough to deserve a one-liner:

```python
job = train.with_sidecars(metrics, tensorboard_server)(config={"lr": 0.001})
```

Desugars exactly to the §4.2 call. Sidecars default to `on_failure="continue"`; the receiver becomes `leader=True`. Nothing new to learn.

### 4.4 Symmetric peers sharing a pool

When peers have specific resource needs that should come from one explicit reservation:

```python
TOPOLOGY = Topology(
    pools={
        "coupled": Pool(
            nodes=1, cpus_per_node=128, mem_per_node="256G",
            constraint="high-mem", partition="compute",
        ),
    },
)

with cluster:
    job = parallel(
        ocean_model.partial(cfg=ocean_cfg),
        atmosphere_model.partial(cfg=atmo_cfg),
        coupler.partial(cfg=coupler_cfg),
        topology=TOPOLOGY,
    )
```

- Every peer without an explicit `pool=` lands in `topology.default_pool` (auto-set if there's only one pool).
- All three peers run as steps inside the single hetjob component; Slurm places them on the reserved node.

### 4.5 Heterogeneous pools

Different peers, different node shapes:

```python
TOPOLOGY = Topology(
    pools={
        "gpu": Pool(nodes=1, gpus_per_node=8, cpus_per_node=128, mem_per_node="1T",
                    gpu_type="h100", partition="gpu-fat"),
        "cpu": Pool(nodes=4, cpus_per_node=96, mem_per_node="384G",
                    partition="cpu"),
    },
)

job = parallel(
    Peer(learner.partial(cfg=cfg), pool="gpu"),
    Peer.replicas(simulator, count=64, pool="cpu",
                  args=lambda i: {"env_id": i}),
    topology=TOPOLOGY,
    time="12:00:00",
)
```

- Two hetjob components — different partitions, different node shapes.
- `Peer.replicas(...)` is a peer with `count > 1` and per-replica argument generation (§5.2).

### 4.6 Named nodes and colocation (RL-style)

```python
TOPOLOGY = Topology(
    pools={
        "learn": Pool(
            nodes=2, gpus_per_node=8, cpus_per_node=128, mem_per_node="1.5T",
            gpu_type="h100", partition="gpu-fat",
            node_labels=["head", "ops"],     # ← first allocated node → "head", second → "ops"
        ),
        "serve": Pool(nodes=2, gpus_per_node=8, cpus_per_node=64,  mem_per_node="512G",
                      gpu_type="a100", partition="gpu"),
        "sim":   Pool(nodes=4, cpus_per_node=96, mem_per_node="384G",
                      partition="cpu"),
    },
)

job = parallel(
    # Learner pinned to the "head" node
    Peer(learner.partial(cfg=cfg), pool="learn", on_node="head", leader=True),

    # Replay buffer forced onto the same node as the learner
    Peer(replay_buffer, pool="learn", colocate_with="learner"),

    # Telemetry agent on the other learn-pool node
    Peer(node_telemetry, pool="learn", on_node="ops", on_failure="continue"),

    # Inference fleet — 8 replicas across 2 A100 nodes (4 per node)
    Peer.replicas(inference, count=8, pool="serve",
                  args=lambda i: {"worker_id": i},
                  on_failure="restart", max_restarts=2),

    # Renderers share the serve pool with inference (--overlap)
    Peer.replicas(renderer, count=16, pool="serve",
                  args=lambda i: {"shard_id": i},
                  on_failure="continue"),

    # CPU simulators
    Peer.replicas(simulator, count=64, pool="sim",
                  args=lambda i: {"env_id": i},
                  on_failure="continue"),

    topology=TOPOLOGY,
    time="24:00:00",
)

final = job.leader_result
```

This is the full cosmos-rl shape in ~25 lines. The three new things:

- **`Pool(node_labels=[...])`** gives logical names to the pool's nodes in allocation order.
- **`Peer(on_node="head")`** pins a peer to a specific labeled node (or ordinal index).
- **`Peer(colocate_with="learner")`** pins a peer to the same node(s) as another peer. Resolved at bootstrap.

Runtime, inside any peer:

```python
@task(...)
def inference(worker_id: int, ctx: JobContext) -> None:
    # Peer discovery
    learner = ctx.peers["learner"].first
    weight_sync_url = f"grpc://{learner.hostname}:{learner.ports['weights']}"

    # Node discovery
    print(f"I'm on node {ctx.node.label}")        # e.g. "serve-01" (unlabeled pool)
    ops_host = ctx.nodes["ops"].hostname

    # Sibling iteration
    for sim in ctx.peers["simulator"].ready_only():
        connect(sim.hostname, sim.ports["obs"])
```

---

## 5. API reference

### 5.1 `parallel(...)`

```python
def parallel(
    *peers: SlurmTask | BoundTask | Peer,
    topology: Topology | None = None,
    time: str | None = None,
    account: str | None = None,
    qos: str | None = None,
    reservation: str | None = None,
    network: str | None = None,
    grace_period_seconds: int = 10,
    **named_peers: SlurmTask | BoundTask | Peer,
) -> ParallelJob
```

- **`*peers` / `**named_peers`** — mixed positional and keyword forms are allowed. Keyword name becomes the peer's `name` if not set on the `Peer` itself. Positional peers get their `name` from the task's `__name__` (or `Peer.name=` if provided).
- **`topology`** — when `None`, an implicit `Topology` with one `Pool` sized by the resource union of all peers is created. Named as `"default"`.
- **`time`, `account`, `qos`, `reservation`, `network`** — defaults applied to every pool that does not override them.
- **`grace_period_seconds`** — SIGTERM-to-SIGKILL window for shutdown (§9.4).

Submission is **eager**. The call returns a `ParallelJob` whose hetjob is already submitted.

### 5.2 `Peer(...)` and `Peer.replicas(...)`

```python
Peer(
    task_or_bound: SlurmTask | BoundTask,
    *,
    pool: str | None = None,          # default: topology.default_pool
    name: str | None = None,          # default: task.__name__
    leader: bool = False,              # exit triggers group shutdown
    on_failure: Literal["kill", "continue", "restart", "callback"] = "kill",
    max_restarts: int = 0,
    callback: Callable[[JobSnapshot], Literal["kill", "continue"]] | None = None,
    exclusive: bool = False,           # per-step --exclusive
    on_node: str | int | None = None,  # node label or ordinal within the pool
    colocate_with: str | None = None,  # other peer's name (same pool required)
    srun_args: list[str] = [],         # verbatim srun flags
    announce: dict[str, Any] | None = None,  # static registry metadata at launch
)

Peer.replicas(
    task_or_bound,
    *,
    count: int,                                                  # replica count
    args: list | Callable[[int], dict | tuple] | range | None = None,
    tasks_per_node: int | None = None,                           # pack density
    on_nodes: list[str | int] | None = None,                     # pin replica i to node[i]
    # ... plus all the keyword args from Peer(...) above
)
```

- **`task_or_bound`** — a `SlurmTask` (no args needed) or a `BoundTask` returned by `.partial(...)`.
- **`pool`** — the destination `Pool`. Must be a key of `topology.pools`. Defaults to the topology's default pool.
- **`name`** — used as the key in `ctx.peers[...]` and `job[...]`. Must be unique across peers.
- **`leader`** — when any leader peer exits (success or failure), the supervisor signals graceful shutdown to every other peer. Multiple leaders are allowed; the first exit triggers shutdown.
- **`on_failure`** — policy when this peer's exit code is non-zero:
  - `"kill"` (default) — abort the group.
  - `"continue"` — the group keeps running without this peer.
  - `"restart"` — re-launch the step up to `max_restarts` times; on exhaustion the policy falls through to `"kill"`.
  - `"callback"` — `callback(snapshot)` decides per-failure, returning `"kill"` or `"continue"`.
- **`on_node`** — string label from `Pool.node_labels` or integer ordinal (0-based) within the pool.
- **`colocate_with`** — name of another peer in the same pool; this peer inherits the target's node assignment.
- **`exclusive`** — emit `--exclusive` on the step; no other step on the same node.
- **`srun_args`** — appended verbatim to the step's `srun` command line. Advanced placement (`--gpu-bind=closest`, `--distribution=...`, `--cpu-bind=...`, `--nodelist=`, ...).
- **`announce`** — static metadata written to the registry at step launch, before the user function runs.

**Replica-specific:**

- **`count`** — compiled as `srun --ntasks=count`.
- **`args`** — per-replica argument generator; same semantics as `SlurmTask.map(items)`. Upstream `.partial(...)` provides common args; `args` provides per-replica variance.
- **`tasks_per_node`** — forces `--ntasks-per-node=N`. Default: computed from pool shape + per-task resource claims to pack densely without oversubscription.
- **`on_nodes`** — list of N labels/ordinals; replica `i` is pinned to `on_nodes[i]`. Errors if `len(on_nodes) != count`.

### 5.3 `Pool(...)`

```python
Pool(
    nodes: int,                                 # hard reservation size

    # Per-node shape
    cpus_per_node: int | None = None,
    mem_per_node: str | None = None,            # "64G" | "1T" | "1536G"
    gpus_per_node: int | None = None,

    # Selection
    partition: str | None = None,
    qos: str | None = None,
    account: str | None = None,
    constraint: str | None = None,              # e.g. "h100&nvlink4"
    gpu_type: str | None = None,                # sugar: --gres=gpu:<type>:<gpus_per_node>
    features: list[str] = [],                   # AND-joined into constraint
    reservation: str | None = None,
    exclude_nodes: list[str] | None = None,

    # Placement
    node_labels: list[str] | None = None,       # must be len == nodes, unique, no dots/brackets

    # Scheduling
    time: str | None = None,                    # override parallel(time=)
    exclusive: bool = False,                    # component-level --exclusive
    gres: dict[str, str] = {},                  # raw GRES passthrough
    extra_sbatch: dict[str, Any] = {},          # raw #SBATCH passthrough
)
```

- **`nodes`** is the exact reservation size. Requesting an empty pool is an error (add nodes or remove the pool).
- **`gpu_type`** and a manual `gres["gpu"]` are mutually exclusive.
- **`features`** are AND-joined with `constraint` (if both set).
- **`node_labels`** is optional; when omitted, nodes are addressable only by ordinal (`on_node=0`, `ctx.nodes[0]`).
- **`extra_sbatch`** and **`gres`** are escape hatches for flags the SDK doesn't model yet.

### 5.4 `Topology(...)`

```python
Topology(
    pools: dict[str, Pool],
    default_pool: str | None = None,            # key of pools; auto if exactly one pool
    network: str | None = None,                 # applied to every pool
    prolog: str | None = None,                  # shell, runs once before any peer
    epilog: str | None = None,                  # shell, runs once after every peer
)
```

- **`default_pool`** — optional when `len(pools) == 1`; required when there are multiple and some peers omit `pool=`.
- **`network`** — attached to every pool's component as `#SBATCH --network=`.
- **`prolog` / `epilog`** — shell snippets spliced into the batch script before the first peer launches and after the supervisor exits. Cluster-specific setup (module loads, license servers, EFA bring-up).

### 5.5 `BoundTask` and `.partial(...)`

```python
class SlurmTask:
    def partial(self, *args, **kwargs) -> "BoundTask": ...

class BoundTask:
    def partial(self, *args, **kwargs) -> "BoundTask":  # merges further args
        ...
    # BoundTask is NOT directly callable — it is a handle for multi-peer APIs.
    # Use the plain SlurmTask to submit a single job.
```

Calling a `BoundTask` directly raises — nudging users toward the multi-peer primitives.

`.partial()` composes with `.with_options()` and `.after()` and is the canonical way to pre-bind args for `Peer(...)`:

```python
Peer(train.with_options(partition="gpu").after(prep).partial(cfg=cfg), leader=True)
```

### 5.6 `ParallelJob`

```python
class ParallelJob:
    # Per-peer
    def __getitem__(self, key: str | tuple[str, int]) -> Job: ...
    # Aggregate
    def get_results(self) -> dict[str, Any | list[Any]]: ...
    def wait(self, timeout: float | None = None) -> bool: ...
    def snapshot(self) -> dict[str, JobSnapshot | list[JobSnapshot]]: ...
    def peer_outcomes(self) -> dict[str, PeerOutcome]: ...
    # Shortcut
    @property
    def leader_result(self) -> Any: ...   # raises if 0 or >1 leaders

    # Pre-submission composition
    def after(self, *jobs) -> "ParallelJob": ...
```

- `job["inference"]` — the replica set as a single handle (supports `.get_results()`, `.snapshot()`, indexing).
- `job["inference", 3]` — replica 3 as an individual `Job`.
- `job.get_results()` returns `{peer_name: result}` for single peers, `{peer_name: [r0, r1, ...]}` for replica sets. Raises `CompositeJobError` if any peer with `on_failure="kill"` died.
- `job.peer_outcomes()` reports per-peer status: `success`, `continue_on_failure`, `restarted`, `fatal`, `not_started`. Use it when peers have `on_failure="continue"` and you want to distinguish intentional partial success from fatal failure.
- `job.leader_result` is the idiomatic access for the leader+helpers pattern.

### 5.7 Sugar: `SlurmTask.with_sidecars(...)`

```python
def with_sidecars(
    self,
    *sidecars: SlurmTask | BoundTask,
    grace_period_seconds: int = 10,
) -> BoundLeaderBundle: ...
```

`train.with_sidecars(m, tb)(cfg=cfg)` is exactly:

```python
parallel(
    Peer(train.partial(cfg=cfg), leader=True),
    Peer(m,  on_failure="continue"),
    Peer(tb, on_failure="continue"),
    grace_period_seconds=10,
)
```

Sugar only — no new concept. Supports all the usual chaining (`.with_options()`, `.after()`). Not usable with `topology=` — if you need explicit pools, write the `parallel(...)` call.

---

## 6. Placement model

### 6.1 Pools map to hetjob components

One pool → one `#SBATCH ... hetjob` component with that pool's partition, constraint, node count, and per-node shape. Components are ordered by pool-key insertion order (Python dict preserves it). Slurm gives each component its own `$SLURM_JOB_NODELIST_HET_GROUP_<n>` variable, which the bootstrap uses to resolve hostnames.

### 6.2 Pool-shared = co-located, pool-separate = isolated

**Two peers in the same pool share that pool's nodes.** Slurm lays them down as sibling steps; `--overlap` is emitted by the SDK when ≥2 steps target the same component so that Slurm does not serialize them.

**Two peers in different pools land on different nodes** — potentially different partitions, node types, and constraints.

This is the primary placement mechanism. For most deployments, choosing the right pools eliminates the need for explicit pinning.

### 6.3 Named nodes and `on_node=`

When finer control is needed:

```python
Pool("main", nodes=4, gpus_per_node=8, node_labels=["head", "aux", "spare1", "spare2"])

Peer(coordinator, pool="main", on_node="head")
Peer(backup,      pool="main", on_node="aux")
Peer.replicas(worker, count=2, pool="main", on_nodes=["spare1", "spare2"])
```

- **Resolution.** At bootstrap, the Python helper reads `$SLURM_JOB_NODELIST_HET_GROUP_<n>` for each pool, expands it with `scontrol show hostnames` (or the equivalent parse), and pairs `node_labels[i]` with `hostnames[i]` in allocation order.
- **Ordinals always work** (`on_node=0`) even without labels. Labels are sugar over ordinals.
- **The SDK emits `srun --nodelist=<resolved_hostname>`** for pinned peers.
- **Replica sets** with `on_nodes=[...]` get `--distribution=arbitrary --nodelist=<comma-separated hostnames>`.

### 6.4 `colocate_with=` semantics

```python
Peer(learner, pool="gpu", on_node="head")
Peer(replay,  pool="gpu", colocate_with="learner")
```

- **Same-pool only.** Colocation across pools is an error — different pools may be on entirely different node types.
- **Target must be resolvable.** If the target has `on_node` or `on_nodes`, the colocator inherits those hostnames. If the target has no pin, the SDK assigns the target to the pool's first unused node (stable, deterministic) and the colocator inherits.
- **Replica set as target** — `Peer(sidecar, colocate_with="inference")` pins `sidecar` to all of inference's nodes (Slurm handles per-node scheduling within the reservation). This is how "one metrics agent per inference node" is expressed naturally.
- **Chains are allowed** — `A colocate_with B`, `B on_node="head"` → `A` on "head". Cycles are detected and reported.

### 6.5 Deterministic default placement

When no peer in a pool is pinned and no `colocate_with` graph forces a choice, the supervisor leaves the step unpinned — Slurm chooses. This is the intended 90% path: you declare the pool shape, Slurm places freely, and your program reads hostnames from `ctx.peers` / `ctx.nodes` at runtime.

### 6.6 Placement conflicts

Validated before submission:

- Pinning two peers to the same node with incompatible resource claims (total GPUs, mem, cpus exceed per-node capacity) → error.
- `on_node="unknown_label"` → error listing available labels.
- `colocate_with` across pools → error.
- `on_nodes=[...]` length mismatch with `count` → error.
- Cycle in `colocate_with` graph → error with the cycle spelled out.

---

## 7. Resource model

### 7.1 Pool sizing

`Pool(nodes=N, cpus_per_node=C, mem_per_node=M, gpus_per_node=G)` is the Slurm request. These four numbers define the hetjob component's `#SBATCH` header.

### 7.2 Peer claims

Each peer's step-level `srun` flags come from its own `@task(...)` decorator: `cpus_per_task`, `mem`, `gpus_per_task`, `ntasks`, `ntasks_per_node`. The SDK validates that the per-step claims fit inside the pool's per-node budget when all steps targeting that pool run concurrently.

Validation algorithm per pool:

```
for each node n in pool:
    for each peer p assigned to n (via pinning, colocate_with, or unpinned default):
        sum(p.cpus_per_task × p.replicas_on_n) ≤ pool.cpus_per_node
        sum(p.gpus_per_task × p.replicas_on_n) ≤ pool.gpus_per_node
        parse_mem(p.mem) × p.replicas_on_n ≤ parse_mem(pool.mem_per_node)
```

For unpinned peers, the validator assumes worst-case (they could share a node with any other unpinned peer) and sums conservatively.

### 7.3 Implicit pool inference

When `parallel(...)` is called without `topology=`, the SDK builds a single pool:

- `nodes = max(peer.nodes for peer in peers)` — usually 1.
- `cpus_per_node = sum(peer.cpus_per_task × peer.count for peer in peers)`.
- `mem_per_node = sum(peer.mem × peer.count for peer in peers)`.
- `gpus_per_node = sum(peer.gpus_per_task × peer.count for peer in peers)`.
- Other directives come from `parallel(time=, account=, qos=, ...)`.

This is the uniform model: even the simplest 2-peer call uses the same machinery as a 50-peer heterogeneous deployment.

### 7.4 Resource overrides

`parallel(..., nodes=2)` and other outer kwargs override the inferred pool's fields when the inference is too coarse:

```python
parallel(
    train.partial(cfg=cfg),
    monitor,
    nodes=1,                            # override: explicitly one node
    mem="32G",                           # override the union
)
```

---

## 8. Runtime services

### 8.1 `JobContext` additions

```python
@dataclass(frozen=True)
class JobContext:
    # Existing fields (job_id, step_id, rank, world_size, hostnames, output_dir, master_addr, ...)

    # Topology identity
    peer_name: str | None = None
    peer_pool: str | None = None
    replica_index: int | None = None
    replica_count: int | None = None

    # Discovery
    peers: Mapping[str, "PeerGroup"] = field(default_factory=dict)
    nodes: "NodeGroup" = field(default_factory=_empty_node_group)
    node: "NodeInfo" = field(default_factory=_empty_node_info)   # the node running me
    my_ports: Mapping[str, int] = field(default_factory=dict)

    # Coordination
    shared_dir: Path | None = None
    shutdown_requested: bool = False                              # property, actually

    def announce(self, **fields: Any) -> None: ...
    def reserve_port(self, name: str) -> int: ...
```

All fields are populated for **every** peer in every topology, including the degenerate case of a 1-peer job (where `ctx.peers` has a single entry referring to the peer itself).

### 8.2 Peer registry

Written to `$JOB_DIR/registry.json` during bootstrap, updated at runtime by `ctx.announce()`:

```json
{
  "peers": {
    "learner": [{
      "name": "learner",
      "replica_index": 0,
      "replica_count": 1,
      "pool": "learn",
      "hostname": "gpu-fat-01",
      "hostnames": ["gpu-fat-01"],
      "node_label": "head",
      "step_id": "12345.0",
      "ports": {"weights": 42011},
      "metadata": {"model_version": "r3"},
      "state": "ready",
      "restart_count": 0
    }],
    "inference": [
      {"name": "inference", "replica_index": 0, ...},
      {"name": "inference", "replica_index": 1, ...}
    ]
  },
  "nodes": {
    "head":  {"hostname": "gpu-fat-01", "pool": "learn", "peers": ["learner", "replay"]},
    "ops":   {"hostname": "gpu-fat-02", "pool": "learn", "peers": ["node_telemetry"]},
    "serve-01": {"hostname": "a100-01", "pool": "serve", "peers": ["inference", "renderer"]},
    "serve-02": {"hostname": "a100-02", "pool": "serve", "peers": ["inference", "renderer"]}
  }
}
```

Writes are atomic (`write_tmp + rename`). Reads are lock-free and cache the deserialized snapshot; `ctx.peers[...].refresh()` re-reads.

### 8.3 `ctx.peers`

`PeerGroup` (the value of `ctx.peers[name]`) supports:

| Method                       | Returns                                                                             |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `group[i]`                   | `PeerInfo` for replica `i`.                                                          |
| `group.first`                | First replica; raises if empty.                                                      |
| `group.hostnames`            | `tuple[str, ...]` of all replica hostnames.                                          |
| `group.ready_only()`         | Iterator over replicas whose `state == "ready"`.                                     |
| `group.wait_all(keys=..., timeout=...)` | Block until every replica has announced the requested keys (or all are ready if keys is None). |
| `group.refresh()`            | Re-read the registry from disk.                                                      |
| `len(group)`                 | Replica count.                                                                       |
| `iter(group)`                | All `PeerInfo`s.                                                                     |

`PeerInfo`:

```python
@dataclass(frozen=True)
class PeerInfo:
    name: str
    replica_index: int
    replica_count: int
    pool: str
    hostname: str                         # first of hostnames
    hostnames: tuple[str, ...]            # all hosts this step runs on
    node_label: str | None
    step_id: str
    ports: Mapping[str, int]
    metadata: Mapping[str, Any]
    state: Literal["pending", "ready", "failed"]
    restart_count: int
```

### 8.4 `ctx.nodes` and `ctx.node`

`NodeGroup` indexes nodes by label and by ordinal:

| Access                           | Returns                                                  |
| -------------------------------- | -------------------------------------------------------- |
| `ctx.nodes["head"]`              | `NodeInfo` for the labeled node.                         |
| `ctx.nodes[0]`                   | `NodeInfo` for the first node of the current pool.       |
| `ctx.nodes.by_hostname("x-01")`  | `NodeInfo` or `None`.                                    |
| `ctx.nodes.in_pool("serve")`     | Iterator of `NodeInfo` in the given pool.                |
| `len(ctx.nodes)`                 | Total nodes in the allocation (all pools).               |

`NodeInfo`:

```python
@dataclass(frozen=True)
class NodeInfo:
    hostname: str
    pool: str
    label: str | None
    ordinal: int                          # 0-based within pool
    peers: tuple[str, ...]                # peer names running on this node
```

`ctx.node` — the `NodeInfo` for the hostname where the current process is running. For multi-node steps (replicas on different hosts), `ctx.node` still refers to this process's host — use `ctx.peers[ctx.peer_name][ctx.replica_index].hostnames` for the step's full node set.

### 8.5 `ctx.announce()`

```python
@task(...)
def learner(cfg: dict, ctx: JobContext) -> dict:
    model = build_model(cfg)
    ctx.announce(ready=True, model_version="r3", checkpoint_interval=cfg["ckpt_every"])
    ...
```

Atomic write to the registry. Visible to everyone via `ctx.peers["learner"].first.metadata["model_version"]`. Reserved key names (`name`, `replica_index`, `hostname`, `step_id`, `ports`, `state`, `node_label`, etc.) are rejected.

### 8.6 Port reservation

**Decorator form** — declared once, bound before user code runs:

```python
@task(gpus_per_task=2, ports={"rpc": "auto", "health": "auto"})
def inference(worker_id: int, ctx: JobContext) -> None:
    grpc_port = ctx.my_ports["rpc"]
    health_port = ctx.my_ports["health"]
    serve_rpc(grpc_port)
```

Behaviour: the runner binds an ephemeral socket (`SO_REUSEADDR`), records the number in the registry, closes the socket just before calling user code. Race window is narrow; production apps can pass fixed ports (`ports={"rpc": 50051}`) and rely on `SO_REUSEADDR` instead.

**Runtime form** — reserve mid-function:

```python
port = ctx.reserve_port("secondary_rpc")
```

### 8.7 `ctx.shared_dir`

Points at `$JOB_DIR/shared/` — one directory shared across all peers in one allocation. Checkpoint handoff, bootstrap configs, and first-boot parameter broadcasts live here.

### 8.8 `ctx.shutdown_requested`

A property; reads from a thread-safe flag the runner flips on SIGTERM. Pure-Python sidecar loops poll it:

```python
@task(cpus_per_task=1)
def metrics(ctx: JobContext) -> None:
    while not ctx.shutdown_requested:
        log_gpu_stats(ctx.output_dir)
        time.sleep(30)
```

Sidecars that block in subprocesses (`tensorboard`, `node-exporter`) inherit SIGTERM via process group — no polling needed.

---

## 9. Failure model

### 9.1 Per-peer `on_failure`

Policy when a peer exits with a non-zero code:

- **`"kill"`** (default) — supervisor signals shutdown to every other peer; the overall job fails.
- **`"continue"`** — the peer's failure is reported in `peer_outcomes()` but the group keeps running.
- **`"restart"`** — supervisor re-launches the step (new `srun`, same `--het-group`, same placement) up to `max_restarts` times. The peer's `restart_count` in the registry increments. On exhaustion, falls through to `"kill"`.
- **`"callback"`** — `callback(snapshot)` runs and returns `"kill"` or `"continue"`. Useful for "ignore if exit code == 137 (OOM) else kill."

### 9.2 Leader exit

When any `leader=True` peer exits (success OR failure), the supervisor signals all other peers with SIGTERM, waits `grace_period_seconds`, then hard-cancels stragglers.

- A leader's success is an orderly shutdown. `job.leader_result` yields its return value. Helper peers' outcomes in `peer_outcomes()` report `shutdown_by_leader` (not `failure`).
- A leader's failure combines with its `on_failure`:
  - `"kill"` — immediate abort (same as non-leader).
  - `"restart"` — restart before triggering group shutdown; sidecars keep running until the restart succeeds or exhausts.
  - `"continue"` with a leader is a validation error. (A leader that's allowed to fail silently makes the group's termination condition undefined.)

### 9.3 Group termination rules

The supervisor terminates the allocation when **any** of:

1. A leader exits (any outcome).
2. A peer with `on_failure="kill"` fails (or `restart` exhausts).
3. All non-leader peers have completed successfully (no leaders defined).
4. `time` expires — Slurm terminates, supervisor reports it as timeout.
5. External `scancel` — trap handler cleans up.

### 9.4 Cascading shutdown

On shutdown trigger:

1. Supervisor writes `state="shutting_down"` to every peer's registry entry.
2. `scancel --signal=TERM` is issued to every step in every hetjob component (`$SLURM_JOB_ID`, `$SLURM_JOB_ID+1`, ...). `scancel` propagates the signal through `srun` to the remote process — a bare shell `kill` would only hit the local wrapper.
3. Grace window (`parallel(grace_period_seconds=10)` default) — peers handle SIGTERM, flush state, exit.
4. Hard `scancel` for stragglers.
5. `wait`; supervisor exits with the final status code.

### 9.5 Partial success

When peers have `on_failure="continue"`, `job.get_results()` raises `CompositeJobError` if at least one `on_failure="kill"` peer failed, otherwise returns the full result map with `None` for peers that didn't produce a value.

`job.peer_outcomes()` is the precise status view:

```python
for name, outcome in job.peer_outcomes().items():
    match outcome.status:
        case "success":             print(name, "✓")
        case "continue_on_failure": print(name, "died (tolerated)")
        case "restarted":           print(name, f"succeeded after {outcome.restart_count} restarts")
        case "fatal":               print(name, "killed the job")
        case "shutdown_by_leader":  print(name, "terminated by leader exit")
        case "not_started":         print(name, "never ran (upstream failure)")
```

---

## 10. Packaging

Per-peer packaging via the existing `@task(packaging=...)` field — heterogeneous images are natural:

```python
@task(packaging="container:nvcr.io/nvidia/pytorch:24.01-py3", gpus_per_task=8)
def learner(cfg: dict) -> dict: ...

@task(packaging="container:vllm/vllm-openai:0.6", gpus_per_task=2)
def inference(worker_id: int) -> None: ...

@task(packaging="container:prom/node-exporter:latest")
def host_metrics() -> None: ...

@task()                                              # wheel → bare node
def light_sidecar(ctx: JobContext) -> None: ...
```

- Each peer's step gets its own `--container-image=` on the `srun` line.
- If a peer has no `packaging=`, it **inherits** the leader's image (or the job's first peer if no leader).
- `packaging="none"` runs the step on the bare node even when the leader is containerized.
- The packaging pipeline's `prepare()` becomes a list-of-configs loop; one registry push per unique image.

---

## 11. Rendering

### 11.1 Batch script skeleton

```bash
#!/bin/bash
#SBATCH --job-name=cosmos-rl
#SBATCH --time=24:00:00

# ── Component 0: pool "learn"
#SBATCH --partition=gpu-fat
#SBATCH --nodes=2
#SBATCH --gpus-per-node=8
#SBATCH --gres=gpu:h100:8
#SBATCH --cpus-per-task=16
#SBATCH --mem=1.5T

#SBATCH hetjob
# ── Component 1: pool "serve"
#SBATCH --partition=gpu
#SBATCH --nodes=2
#SBATCH --gpus-per-node=8
#SBATCH --gres=gpu:a100:8
#SBATCH --cpus-per-task=8
#SBATCH --mem=512G

#SBATCH hetjob
# ── Component 2: pool "sim"
#SBATCH --partition=cpu
#SBATCH --nodes=4
#SBATCH --cpus-per-task=96
#SBATCH --mem=384G

set -eo pipefail
export JOB_DIR="$(pwd)/slurm_${SLURM_JOB_ID}"
mkdir -p "$JOB_DIR/shared"

# Bootstrap: resolve node labels, seed the registry, prepare per-peer arg files
python -m slurm.topology_bootstrap --job-dir "$JOB_DIR"

trap 'scancel "$SLURM_JOB_ID" "$SLURM_JOB_ID+1" "$SLURM_JOB_ID+2" 2>/dev/null' EXIT

# Everything below is the supervisor's job
exec python -m slurm.topology_supervisor \
    --job-dir "$JOB_DIR" \
    --plan "$JOB_DIR/plan.json" \
    --grace 10
```

### 11.2 Bootstrap phase (`slurm.topology_bootstrap`)

Runs once, on the batch allocation's head node. Responsibilities:

1. Parse `SLURM_JOB_NODELIST_HET_GROUP_<n>` for each component; expand to hostnames (via `scontrol show hostnames`).
2. For each pool, pair `node_labels[i]` with `hostnames[i]` in allocation order.
3. Resolve `on_node` / `on_nodes` directives to concrete hostnames.
4. Resolve the `colocate_with` dependency graph (topological sort + cycle detection).
5. Assign unpinned peers their target nodes where placement matters (to satisfy colocation anchors).
6. Write `$JOB_DIR/registry.json` skeleton (peers with `state="pending"`, nodes with their peer lists).
7. Serialize per-peer arg files (`peer_<name>_<replica>_args.pkl`).
8. Write `$JOB_DIR/plan.json` with every step's `srun` command, `--het-group`, `--nodelist`, per-step container image, failure policy, and restart config.

### 11.3 Supervisor phase (`slurm.topology_supervisor`)

Python process that owns lifecycle:

1. Reads `plan.json`. For each peer, launches its `srun ...` as a subprocess with `Popen`.
2. Watches subprocess exits via an epoll/select loop.
3. On peer exit, applies policy:
   - Success + leader → trigger shutdown.
   - Success + non-leader → mark `success`, continue.
   - Non-zero + `on_failure="continue"` → mark `continue_on_failure`, continue.
   - Non-zero + `on_failure="restart"` with budget → re-launch with new step id, increment `restart_count`.
   - Non-zero + `on_failure="kill"` (or restart exhausted) → trigger shutdown.
   - Non-zero + `on_failure="callback"` → invoke callback, apply its decision.
4. On shutdown trigger: `scancel --signal=TERM` for all components, wait `grace`, `scancel` hard.
5. Waits for all subprocesses, writes final outcomes into the registry, exits with consolidated status.

This replaces the 0.6-ish bash wait/kill approach. Bash is not expressive enough for restart + callbacks + partial-success tracking — doing it in Python from the start avoids a throwaway intermediate.

### 11.4 Runner `--step` mode

Each `srun` invocation runs:

```
python -m slurm.runner \
    --step peer:<name>[:by-taskid] \
    --registry "$JOB_DIR/registry.json" \
    --args "$JOB_DIR/peer_<name>_<replica>_args.pkl"
```

- `--step peer:<name>` for single-replica peers.
- `--step peer:<name>:by-taskid` for replica sets — the runner reads `SLURM_PROCID` (the task index within the step) and loads the matching args file.
- `--registry` path is cached; `ctx.peers` is a live view.

---

## 12. Composition

### 12.1 With `@workflow`

`parallel(...)` inside a `@workflow`-decorated function submits a nested allocation that the workflow driver waits on like any other job:

```python
@workflow(time="30:00:00")
def rl_pipeline(cfg: dict, ctx: WorkflowContext):
    prep = preprocess(cfg)
    train_job = parallel(
        Peer(learner.partial(cfg=cfg), pool="learn", leader=True),
        Peer.replicas(inference, count=8, pool="serve", args=lambda i: {"worker_id": i}),
        topology=TOPOLOGY,
    ).after(prep)
    evaluate.after(train_job)(checkpoint=train_job.leader_result)
```

### 12.2 With `.after(...)`

`parallel(...).after(prep_job)` sets `--dependency=afterok:<prep_id>` on every hetjob component. The allocation won't start until `prep_job` succeeds.

### 12.3 With `.with_options(...)`

`Peer(train.with_options(partition="gpu-debug").partial(cfg=cfg), ...)` — per-peer SBATCH overrides propagate to the step's `srun` flags. Does not change the pool's component directives (those come from `Pool(...)`).

### 12.4 With `.map(...)`

Re-submit the topology across a parameter sweep at the caller level:

```python
for hyperparams in sweep:
    job = parallel(
        Peer(learner.partial(cfg=hyperparams), ...),
        ...,
        topology=TOPOLOGY,
    )
    results.append(job.leader_result)
```

Built-in fan-out (`parallel.map(configs, topology=TOPOLOGY)`) is **not** included. Write the loop; it's clearer.

### 12.5 Not supported

| Combination                                                       | Reason                                                      |
| ----------------------------------------------------------------- | ----------------------------------------------------------- |
| `parallel(task.map(items), ...)`                                  | Array jobs and peer replica sets are distinct allocations. Use `Peer.replicas(...)` instead. |
| `Peer(task.with_sidecars(...), ...)`                              | A peer's body is a single task. Flatten into more peers.   |
| Nested `parallel(...)` inside a peer                              | One allocation per call. For multiple, use `@workflow`.    |
| Mixing `topology=` with `parallel(...).with_sidecars(...)`        | `with_sidecars` is sugar only; use `parallel(Peer(...), ...)` for explicit topology. |
| Dynamic add/remove of peers after submission                      | Slurm hetjobs are fixed at allocation time.                |
| `colocate_with` crossing pool boundaries                          | Pools are different node types; "same node" is undefined.  |

---

## 13. Local mode

`Cluster(backend_type="local")` runs every peer as a subprocess on the workstation:

- Pool shape is validated against local CPU/RAM/GPU capacity.
- Constraints (`partition`, `gpu_type`, `constraint=`, `reservation`) are parsed for syntax and ignored for matching.
- Node labels still work — with one local node per pool "component" (or all peers on the same host if pools have `nodes=1`).
- Supervisor is the same process; it uses `subprocess.Popen` instead of `srun`.
- `scancel` is replaced by sending SIGTERM to the subprocess group.

This keeps the cosmos-rl example runnable on a developer workstation with scaled-down `count=` values — essential for integration testing without cluster access.

---

## 14. Validation

Submission-time validator aggregates every error before raising:

```
TopologyError: 4 problems with parallel() submission:

  • Peer 'renderer' (pool='sim') requests gpus_per_task=1 but pool 'sim' has
    gpus_per_node=0 (cpu-only). Pool 'serve' has gpus. Suggestion: pool='serve'.

  • Pool 'learn' overflow: total GPU demand 16 (learner:8 × replicas:1 + replay:0 + telemetry:0 = 8 per node × 2 nodes pinned = 16 required) exceeds capacity
    16 = 8 per-node × 2 nodes. Exactly at limit; marginal — consider gpus_per_node=8 on 1 node or adding headroom.

  • Peer 'replay' colocate_with 'learner' but peers are in different pools
    ('learn_alt' vs 'learn'). Move both peers to the same pool.

  • Cyclic colocate_with: A → B → C → A. Break the cycle.
```

Every error names the offending peer/pool and offers a suggestion. Topology errors never make it to `sbatch`.

---

## 15. Escape hatches

Last-resort knobs for things the first-class API does not yet model:

### 15.1 Per-peer `srun_args`

```python
Peer(learner.partial(cfg=cfg), pool="learn", leader=True,
     srun_args=["--gpu-bind=closest", "--cpu-bind=ldoms"])

Peer.replicas(inference, count=16, pool="serve",
              srun_args=["--distribution=block:block"])
```

### 15.2 Per-pool `extra_sbatch` / `gres`

```python
Pool(
    nodes=4, gpus_per_node=8, partition="gpu",
    gres={"gpu": "h100:8", "bb": "capacity=1TB"},
    extra_sbatch={"switches": "2@10:00", "network": "nopm"},
)
```

### 15.3 `Topology(prolog=..., epilog=...)`

```python
Topology(
    pools={...},
    prolog="""
    module load cuda/12.1
    module load nccl/2.19.3
    export FI_PROVIDER=efa
    """,
    epilog="""
    /opt/metrics/flush.sh "$JOB_DIR/metrics"
    """,
)
```

Runs once per allocation on the head node — not per step.

### 15.4 `Cluster.submit_raw(script, ...)`

Existing primitive for hand-written batch scripts. Use when even the escape hatches above aren't enough.

---

## 16. Non-goals

- **Elastic peer counts.** Scale-up/down during a live allocation is unsupported. Re-submit with `@workflow` + adjusted `count=` if you need it.
- **Cross-allocation discovery.** `ctx.peers` only sees peers in the same `parallel(...)` call. Talk to another allocation through shared storage or external service discovery.
- **Automatic pool sizing.** Declare pool shape explicitly. The SDK will not infer "you probably want 3 nodes" from peer specs — predictability trumps cleverness for 24-hour jobs.
- **Cross-peer RPC frameworks.** The contract is hostnames + ports + `shared_dir`. Ray, gRPC, NCCL, etc. live in user code.
- **Full Slurm flag coverage.** We model the common 90%. The long tail lives in `srun_args`, `extra_sbatch`, and `submit_raw`.
- **Transparent re-scheduling across failures beyond restart.** If a whole node dies, the restart policy kicks in; if a whole allocation dies, Slurm requeue semantics apply (unmodified). We do not synthesize allocation-level redundancy.

---

## 17. Alternatives considered

- **Separate `with_sidecars()` API as its own primitive.** Rejected: sugar over `parallel(Peer(x, leader=True), Peer(y, on_failure="continue"))`. Keeping it as a syntactic shorthand earns the ergonomic win without duplicating behaviour.
- **`@sidecar` / `@service` / `@peer` decorators.** Rejected: a peer is a deployment role, not a function property. The same function can be a leader one day and a sidecar the next.
- **Implicit ordinal-only placement (no `node_labels`).** Rejected: RL topologies routinely want named per-node roles, and naming beats integer indices for reading the code back.
- **`colocate_with` as the only placement primitive.** Rejected on its own: falls apart for ≥3 co-located peers (N-ary chain), and for per-node roles (which are naturally label-addressed). Kept alongside `on_node` — they solve adjacent problems.
- **String-sentinel runtime discovery (`arg="auto"`).** Rejected: duplicates `JobContext` injection in a grep-hostile way.
- **Tuple-form sidecar arguments (`(metrics, {"port": 6006})`).** Rejected: `BoundTask.partial()` is the existing-shape answer.
- **Bash-based supervisor with Python restart helper.** Rejected: bifurcates the lifecycle logic and adds a hand-off boundary for no payoff. One Python supervisor handles everything.
- **Eager resolution of `colocate_with` at `Peer()` construction.** Rejected: pool allocation happens at submission, so hostnames are only known at bootstrap. Resolving earlier would require a second submission phase.
- **Requiring every peer to declare a pool.** Rejected: `topology=None` + auto-inferred single pool is the universal degenerate case and keeps 2-peer calls idiomatic.
- **`ParallelJob` inheriting from `Job`.** Rejected: a parallel job has no single result; the plural nature should be at the type level, parallel to `ArrayJob`.
- **Shell `kill` for shutdown.** Rejected: targets the `srun` wrapper, not the remote step. `scancel --signal=TERM <jobid>.<stepid>` propagates correctly.

---

## Appendix A — `JobContext` topology fields

```python
@dataclass(frozen=True)
class JobContext:
    # Existing fields unchanged: job_id, step_id, rank, local_rank, world_size,
    # num_nodes, hostnames, master_addr, master_port, environment, output_dir, created_at.

    # Topology identity
    peer_name: str | None = None
    peer_pool: str | None = None
    replica_index: int | None = None
    replica_count: int | None = None

    # Discovery
    peers: Mapping[str, "PeerGroup"] = field(default_factory=dict)
    nodes: "NodeGroup" = field(default_factory=_empty_node_group)
    node: "NodeInfo" = field(default_factory=_empty_node_info)
    my_ports: Mapping[str, int] = field(default_factory=dict)

    # Coordination
    shared_dir: Path | None = None

    @property
    def shutdown_requested(self) -> bool: ...

    def announce(self, **fields: Any) -> None: ...
    def reserve_port(self, name: str) -> int: ...
```

## Appendix B — Hetjob operational notes

- `scancel $SLURM_JOB_ID` signals only component 0. The supervisor always includes `$SLURM_JOB_ID+1`, `+2`, ... in cancellations.
- `SLURM_JOB_NODELIST` inside the batch script is the submission's head nodelist; per-component nodelists are in `SLURM_JOB_NODELIST_HET_GROUP_<n>`.
- `--overlap` is required whenever two sibling steps target the same component; without it the second step blocks waiting for resources Slurm has already given the first.
- `sacct -j <jobid>.batch --format=JobID,State,ExitCode` yields one row per hetjob component. The supervisor consolidates them into the SDK's status surface.
- `--exact` is emitted on every step by default to prevent resource oversubscription across sibling steps.

## Appendix C — Worked example, pool to script

```python
parallel(
    Peer(train.partial(cfg=cfg), leader=True),
    Peer(metrics, on_failure="continue"),
    topology=None,
    time="04:00:00",
)
```

Inferred pool:

```
Pool(
    nodes=1,
    cpus_per_node=train.cpus_per_task + metrics.cpus_per_task,
    mem_per_node=train.mem + metrics.mem,
    gpus_per_node=train.gpus_per_task + metrics.gpus_per_task,
)
```

Rendered:

```bash
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=9
#SBATCH --gpus-per-node=4
#SBATCH --mem=34G
# (no hetjob divider — single pool = single component)
# ... bootstrap + supervisor ...
```

Supervisor plan entry:

```json
{
  "peers": [
    {"name": "train", "leader": true, "on_failure": "kill",
     "srun": ["srun", "--exact", "--ntasks=1", "--cpus-per-task=8", "--gpus=4", "--mem=32G",
              "python", "-m", "slurm.runner", "--step", "peer:train", ...]},
    {"name": "metrics", "leader": false, "on_failure": "continue",
     "srun": ["srun", "--exact", "--overlap", "--ntasks=1", "--cpus-per-task=1", "--mem=2G",
              "python", "-m", "slurm.runner", "--step", "peer:metrics", ...]}
  ],
  "grace_seconds": 10
}
```

Every `parallel(...)` reduces to this shape — one or more `Pool` directives, a plan of `Peer` steps, and the supervisor running the whole thing.
