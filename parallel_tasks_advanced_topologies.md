# Advanced Multi-Node Topologies

> **Status:** Design draft. Target release 0.7.0.
> **Builds on:** the single-pool `parallel(...)` / `with_sidecars(...)` APIs from [`parallel_tasks.md`](./parallel_tasks.md).
> **Slurm primitives:** heterogeneous jobs (`hetjob`), job steps (`srun`), feature constraints, GRES, and step-level placement flags.

## 1. Motivation

Modern RL training (e.g. [cosmos-rl](https://github.com/nvidia-cosmos/cosmos-rl)) is a heterogeneous pipeline:

```
┌──────────────────┐       ┌───────────────────┐       ┌────────────────────┐
│  Simulators (N)  │──obs─>│  Inference (M)    │──act─>│    Learner (1)     │
│  CPU-heavy       │<─act──│  Small-GPU serve  │<─grad─│  Fat-GPU DDP/FSDP  │
│  many instances  │       │  many instances   │       │  single instance   │
└──────────────────┘       └───────────────────┘       └────────────────────┘
        rendering, replay, weight-broadcast … wired across the same allocation
```

Each service has a different resource appetite:

| Service       | Shape                         | Scheduling constraint                |
| ------------- | ----------------------------- | ------------------------------------ |
| Learner       | 8× fat GPU (H100), NVLink     | One node, exclusive, fastest fabric  |
| Inference     | 2× GPU per replica, 8 replicas| Spread across inference nodes        |
| Simulators    | 4× CPU per replica, 64 replicas| CPU-only partition, packed densely  |
| Renderer      | 1× GPU per replica, 16 replicas| Any GPU (A100 OK), flexible        |
| Replay buffer | 1× host with 1 TB RAM         | Same node as learner (low-latency)   |

All of them must **share one allocation** so they can communicate over the fast fabric and come up/down atomically. They must land on **different node types** (fat-GPU vs thin-GPU vs CPU-only). Replicas of a service must be **packed** onto the right pool without colliding.

Slurm's [heterogeneous job](https://slurm.schedmd.com/heterogeneous_jobs.html) model handles this natively — but the raw `#SBATCH hetjob` + per-step `--het-group=N --ntasks-per-node=X --gpus-per-task=Y --constraint=Z` incantation is miserable to hand-write. The SDK should expose the expressive power without the syntax.

### 1.1 What "beautiful" means here

The goal is an API where **what you write reads like a deployment intent diagram, and what runs matches it exactly.**

- Declare the **hardware shapes** you need once (`Pool`).
- Declare the **services** as peers assigned to pools (`Peer`, `Peer.replicas`).
- The SDK compiles that to a `hetjob` submission with per-component steps, sized and placed according to the pools.
- At runtime each peer can discover every other peer by name and replica index.

The simple `parallel(a, b)` from 0.6 stays — it becomes the degenerate case of a single-pool topology. This document is the power-user layer sitting on top.

## 2. The allocation model

The 0.6 `parallel(...)` maps to **one step per peer inside one allocation**:

```
sbatch ─┬─ srun step_0 (peer A)
        ├─ srun step_1 (peer B)
        └─ srun step_2 (peer C)
```

That's fine when all peers fit on the same node shape. Once peers need different partitions / node types / resource ratios, one allocation can't cover all of them.

Slurm's answer is `hetjob`:

```
sbatch ─┬─ component 0 (partition=gpu-fat, 1 node,  8 H100)
        │     └─ srun --het-group=0 step_0   (peer: learner)
        ├─ component 1 (partition=gpu,     2 nodes, 4 A100 each)
        │     ├─ srun --het-group=1 step_0   (peer: inference)
        │     └─ srun --het-group=1 step_1   (peer: renderer)
        └─ component 2 (partition=cpu,     4 nodes, 96 CPU each)
              └─ srun --het-group=2 step_0   (peer: simulator × 64)
```

One submission, one job id family (`$SLURM_JOB_ID`, `$SLURM_JOB_ID+1`, `$SLURM_JOB_ID+2`), co-scheduled. Peers in different components can talk over the cluster fabric; peers in the same component are co-placed on the same nodes.

**The SDK maps pools → hetjob components, peers → steps inside a component, replica sets → multi-task steps.** That's it. Everything below is vocabulary and ergonomics for describing the mapping.

## 3. Vocabulary

- **Pool** — a homogeneous hardware rectangle you want to reserve: N nodes of a given shape, satisfying a given constraint, on a given partition. Compiles to one hetjob component.
- **Peer** — one service running inside a pool. Compiles to one `srun` step. A peer can be a single instance or a replica set.
- **Replica set** — a peer with `replicas=N`; each replica is one task inside the step (`srun --ntasks=N`).
- **Topology** — a collection of named pools. One per `parallel(...)` call; also reusable via constants.
- **Peer registry** — runtime directory of where every peer landed (hostnames, ports, step ids). Populated by the SDK, consumed by peers via `JobContext.peers`.

## 4. API walkthrough

From the simplest case up. Every `parallel(...)` call you see below is the same entry point, differentiated by whether `topology=` is supplied.

### 4.1 One pool implied (0.6 form — still works)

```python
job = parallel(
    ocean.partial(cfg=ocean_cfg),
    atmo.partial(cfg=atmo_cfg),
    coupler.partial(cfg=coupler_cfg),
)
```

No `topology=` → single implicit pool, resource-union semantics, one hetjob component. Identical to the 0.6 behaviour documented in `parallel_tasks.md`.

### 4.2 One pool, explicit

Useful when you want to name the pool, set a constraint, or attach per-peer placement directives:

```python
job = parallel(
    Peer(train.partial(cfg=cfg),  name="train"),
    Peer(monitor,                 name="monitor"),
    topology=Topology(
        pools={"main": Pool(nodes=1, gpus_per_node=8, constraint="h100")},
        default_pool="main",
    ),
)
```

A bare `BoundTask` / `SlurmTask` inside a topology'd `parallel()` is sugar for `Peer(bt, pool=<default_pool>)`.

### 4.3 Two pools, heterogeneous hardware

```python
TOPOLOGY = Topology(
    pools={
        "gpu":  Pool(nodes=1, gpus_per_node=8, cpus_per_node=128, mem_per_node="1T",
                     constraint="h100", partition="gpu-fat"),
        "cpu":  Pool(nodes=4, cpus_per_node=96, mem_per_node="384G",
                     partition="cpu"),
    },
)

job = parallel(
    Peer(learner.partial(cfg=cfg),     pool="gpu",  name="learner"),
    Peer.replicas(simulator, count=64, pool="cpu",  name="simulator",
                  args=lambda i: {"env_id": i}),
    topology=TOPOLOGY,
    time="12:00:00",
)
```

This compiles to a 2-component hetjob: one `gpu-fat` node for the learner, four `cpu` nodes for 64 simulator replicas (16 per node).

### 4.4 Full RL topology (cosmos-rl-shaped)

```python
from slurm import task, parallel, Peer, Pool, Topology, Cluster, JobContext

@task(gpus_per_task=8, cpus_per_task=16, mem="512G")
def learner(cfg: dict, ctx: JobContext) -> dict: ...

@task(gpus_per_task=2, cpus_per_task=8, mem="64G", ports={"rpc": "auto"})
def inference(worker_id: int, ctx: JobContext) -> None: ...

@task(gpus_per_task=1, cpus_per_task=8, mem="32G")
def renderer(shard_id: int, ctx: JobContext) -> None: ...

@task(cpus_per_task=4, mem="16G")
def simulator(env_id: int, ctx: JobContext) -> None: ...

@task(cpus_per_task=32, mem="1T")
def replay_buffer(ctx: JobContext) -> None: ...


TOPOLOGY = Topology(
    pools={
        "learn": Pool(nodes=1, gpus_per_node=8, cpus_per_node=128, mem_per_node="1.5T",
                      gpu_type="h100", partition="gpu-fat"),
        "serve": Pool(nodes=2, gpus_per_node=8, cpus_per_node=64,  mem_per_node="512G",
                      gpu_type="a100", partition="gpu"),
        "sim":   Pool(nodes=4, cpus_per_node=96, mem_per_node="384G",
                      partition="cpu"),
    },
)


with Cluster(backend_type="ssh", hostname="hpc") as cluster:
    job = parallel(
        # Co-located on the learner node
        Peer(learner.partial(cfg=cfg),         pool="learn", name="learner"),
        Peer(replay_buffer,                    pool="learn", name="replay"),

        # Inference fleet on the a100 nodes
        Peer.replicas(inference, count=8,      pool="serve", name="inference",
                      args=lambda i: {"worker_id": i},
                      on_failure="restart", max_restarts=2),

        # Renderers share the same a100 nodes as inference (Slurm packs them)
        Peer.replicas(renderer,  count=16,     pool="serve", name="renderer",
                      args=lambda i: {"shard_id": i},
                      on_failure="ignore"),

        # CPU simulators
        Peer.replicas(simulator, count=64,     pool="sim",   name="simulator",
                      args=lambda i: {"env_id": i},
                      on_failure="ignore"),

        topology=TOPOLOGY,
        time="24:00:00",
        on_peer_failure="kill",   # default; overridden per-peer above
    )

    # The learner's return value is the canonical job result
    final = job["learner"].get_result()
```

What the SDK does from that declaration:

1. **Compile three hetjob components**: one per pool.
2. **Allocate**: `--partition=gpu-fat --nodes=1 --gpus=8 --constraint=h100 … hetjob … --partition=gpu --nodes=2 --gpus=16 --constraint=a100 … hetjob … --partition=cpu --nodes=4 --cpus-per-task=96 …`
3. **Launch steps**: one `srun --het-group=N --ntasks=<replicas>` per peer, backgrounded.
4. **Populate the peer registry** so `ctx.peers["inference"][3].hostname` resolves.
5. **Wait and apply failure policy** per peer, then overall.

## 5. The `Peer` primitive

```python
Peer(
    task_or_bound: SlurmTask | BoundTask,
    *,
    pool: str = "<default>",
    name: str | None = None,
    on_failure: Literal["kill", "ignore", "restart", "callback"] = "kill",
    max_restarts: int = 0,
    callback: Callable[[JobSnapshot], Literal["kill", "ignore"]] | None = None,
    exclusive: bool = False,
    srun_args: list[str] = [],
    announce: dict[str, Any] | None = None,
)
```

- `task_or_bound` — a `SlurmTask` (no args) or a `BoundTask` (from `.partial(...)`).
- `pool` — which `Pool` hosts this peer. Defaults to `Topology.default_pool`.
- `name` — used for registry lookup (`ctx.peers["<name>"]`) and result access (`job["<name>"]`). Defaults to the task's `__name__`. Must be unique across peers.
- `on_failure`:
  - `"kill"` (default) — the whole allocation aborts.
  - `"ignore"` — this peer's failure is tolerated. Suitable for best-effort sidecars and for loss-tolerant replica sets (e.g. 63/64 simulators is still useful).
  - `"restart"` — the peer is re-launched inside the same allocation, up to `max_restarts`. See §5.3.
  - `"callback"` — a user function decides per-failure.
- `exclusive` — add `--exclusive` to the step so it does not share CPUs/GPUs with other steps on its node.
- `srun_args` — appended verbatim to the step's `srun` line. Escape hatch for `--gpu-bind=closest`, `--cpu-bind=…`, `--distribution=…`, etc.
- `announce` — static key/value pairs written to the peer registry at launch, before the function body runs. Useful for constant metadata (`{"protocol": "grpc+mTLS"}`).

### 5.1 `Peer.replicas(...)` — replica sets

```python
Peer.replicas(
    task_or_bound: SlurmTask | BoundTask,
    *,
    count: int,
    args: list | Callable[[int], dict | tuple] | range | None = None,
    pool: str = "<default>",
    name: str | None = None,
    on_failure: ... = "kill",
    max_restarts: int = 0,
    tasks_per_node: int | None = None,
    srun_args: list[str] = [],
    announce: ... = None,
)
```

- `count` — number of replicas (compiles to `srun --ntasks=count`).
- `args` — per-replica argument generator. Same semantics as `SlurmTask.map(items)`:
  - `list` → one entry per replica; if an entry is a dict it becomes kwargs, if a tuple it becomes positional, otherwise it is passed as the first positional arg.
  - `Callable[[int], ...]` → called with the replica index to produce args.
  - `range(N)` / `range(a, b)` → values passed as the first positional arg.
  - `None` → no per-replica args; every replica gets identical args (from the upstream `.partial()`).
- `tasks_per_node` — forces `--ntasks-per-node=N` on the step. Default: computed from the pool shape and peer's `gpus_per_task`/`cpus_per_task` so replicas pack densely without oversubscription.

All the other `Peer(...)` directives apply identically to replica sets.

### 5.2 Placement intent

Users do not pin to specific hostnames. The placement contract is:

- **Peers in the same pool share that pool's nodes.** Slurm decides exact placement; replicas pack densely by `tasks_per_node`.
- **Peers in different pools land on their own pool's nodes** — different hetjob components can use different partitions, node types, and constraints.
- **Co-location intent** is expressed by putting peers in the **same pool**. The `learner` + `replay_buffer` example above uses `pool="learn"` with `nodes=1`, so both are on the learner node by construction.
- **Isolation intent** is expressed by putting peers in **different pools**.

This is sufficient and far clearer than per-peer `colocate_with=` / `disjoint_from=` directives would be. Pools *are* the placement groups.

### 5.3 Restart policy

When `on_failure="restart"`, the driver script re-launches that step (only that step) up to `max_restarts` times. Restart resets the replica index and assumes the peer is idempotent — it is the user's responsibility to make the peer re-entrant (idempotent init, recovery from checkpoints, stable service discovery key). The peer registry records restart count under `peer.restart_count`.

Restart does **not** grow the allocation. It reuses the nodes already reserved for the pool.

If `max_restarts` is exceeded, the policy falls through to `on_peer_failure` (the parent default). So `restart` + fallback `kill` is common for critical-but-flaky peers.

## 6. The `Pool` primitive

```python
Pool(
    nodes: int,
    *,
    # Per-node shape
    cpus_per_node: int | None = None,
    mem_per_node: str | None = None,
    gpus_per_node: int | None = None,

    # Selection
    partition: str | None = None,
    qos: str | None = None,
    account: str | None = None,
    constraint: str | None = None,         # e.g. "h100&nvlink4"
    gpu_type: str | None = None,           # sugar over --gres=gpu:<type>:N
    reservation: str | None = None,
    features: list[str] = [],              # syntactic sugar; AND-joined into constraint
    exclude_nodes: list[str] | None = None,

    # Scheduling
    time: str | None = None,               # per-pool override; defaults to parallel(time=)
    exclusive: bool = False,                # outer --exclusive for this component
    gres: dict[str, str] = {},             # raw GRES passthrough
    extra_sbatch: dict[str, Any] = {},     # raw SBATCH passthrough
)
```

- `nodes` is the **hard** reservation size for this pool — the hetjob component requests exactly this many nodes.
- `gpu_type` is converted to `--gres=gpu:<gpu_type>:<gpus_per_node>` and kept in sync with `gpus_per_node`. Mutually exclusive with manual `gres["gpu"]`.
- `constraint` is Slurm's boolean feature expression (`--constraint=`). If `features=["h100", "nvlink4"]` is given, they're AND-joined into the constraint. Both can be set; they are AND-combined.
- `extra_sbatch` / `gres` are raw passthroughs for knobs the SDK doesn't model yet (license acquisition, switches, custom plugins).

### 6.1 Required consistency

Within one `Topology`:

- Every pool may have a different partition/constraint/node shape (that's the point).
- `time`, `qos`, `account` default from the parent `parallel(...)` and may be overridden per pool.
- `Topology(network=...)` applies to every pool (e.g. `network="efa"`).

### 6.2 Pool sanity

The SDK validates on submission:

- Σ(`peer.gpus_per_task × peer.replicas`) ≤ `pool.nodes × pool.gpus_per_node` for every pool.
- Same for CPU and memory, using the pool's per-node numbers.
- `peer.gpus_per_task > pool.gpus_per_node` → error with a suggestion (wrong pool).
- A peer referencing an unknown pool name → error listing available pools.
- Overlap of static `peer.announce` keys with reserved keys (`hostname`, `step_id`, `replica_index`, …) → error.

Error messages name the offending peer, pool, and resource.

## 7. Runtime service discovery

### 7.1 `ctx.peers`

```python
@task(gpus_per_task=2)
def inference(worker_id: int, ctx: JobContext) -> None:
    # Find the learner
    learner = ctx.peers["learner"].first
    grad_rpc = f"grpc://{learner.hostname}:{learner.ports['rpc']}"

    # Find my sibling inference workers (includes myself)
    siblings = ctx.peers["inference"]
    my_peer = siblings[ctx.replica_index]
    print(f"I am inference/{ctx.replica_index} on {my_peer.hostname}")

    # Iterate simulators (loss-tolerant — some may be missing)
    for sim in ctx.peers["simulator"].ready_only():
        connect(sim.hostname, sim.ports["obs"])
```

The runner builds `ctx.peers` before the task function runs, by reading `$JOB_DIR/peers.json`:

```json
{
  "learner":    [{"hostname": "gpu-node01", "replica_index": 0,
                  "step_id": "123.0", "pool": "learn",
                  "ports": {"rpc": 42011}, "metadata": {"model_version": "r3"},
                  "state": "ready"}],
  "inference":  [{...}, {...}, ...],
  "simulator":  [{...}, ...]
}
```

`PeerGroup` (`ctx.peers["name"]`) supports:

| Method              | Returns                                        |
| ------------------- | ---------------------------------------------- |
| `group[i]`          | `PeerInfo`                                     |
| `group.first`       | first replica (errors if zero)                 |
| `group.ready_only()`| iterator over peers that announced `ready`     |
| `group.wait_all(timeout=)` | block until all peers announce `ready`; time-bounded |
| `group.hostnames`   | tuple of hostnames (for MPI-style `WORLD_SIZE` manipulations) |
| `len(group)`        | replica count                                  |

### 7.2 `ctx.announce`

Peers update the registry at runtime to publish state and metadata:

```python
def learner(cfg, ctx: JobContext):
    model = build_model(cfg)
    ctx.announce(
        ready=True,
        model_version="r3",
        checkpoint_interval=cfg["ckpt_every"],
    )
    train(model)
```

Writes are atomic (write-to-tmp + `rename`). Readers either poll or block on `wait_all(keys=["ready"])`.

### 7.3 `ctx.reserve_port(name)` and `@task(ports={...})`

Decorator form reserves N named ports **before** the function runs, writes them into `peer.ports`, and exposes `ctx.my_ports`:

```python
@task(gpus_per_task=2, ports={"rpc": "auto", "health": "auto"})
def inference(worker_id: int, ctx: JobContext) -> None:
    grpc_port = ctx.my_ports["rpc"]      # a known-free port on this host
    health_port = ctx.my_ports["health"]
    serve_rpc(grpc_port)

# Fetched from anywhere:
peer = ctx.peers["inference"][0]
peer.ports["rpc"]
```

Under the hood the runner binds ephemeral sockets (`SO_REUSEADDR`), closes them just before handing control to user code, and records the numbers. This is best-effort — there's a race if something else binds the port in the gap. For production workloads you can instead pass `ports={"rpc": 50051}` (fixed) and let service discovery resolve hostnames only; the fixed-port case is what most apps use with `SO_REUSEADDR`.

For inline reservation mid-function:

```python
port = ctx.reserve_port("secondary_rpc")
```

It does the same thing imperatively and updates the registry atomically.

### 7.4 `ctx.shared_dir`

Every peer sees `ctx.shared_dir` pointing at `$JOB_DIR/shared/`. Already specified in `parallel_tasks.md`; called out here because multi-service topologies use it heavily (checkpoint handoff, parameter broadcasts that bypass RPC for first boot, etc.).

## 8. `parallel(...)` with `topology=`

```python
parallel(
    *peers: SlurmTask | BoundTask | Peer,
    **named_peers: SlurmTask | BoundTask | Peer,
    topology: Topology | None = None,
    on_peer_failure: Literal["kill", "ignore"] = "kill",
    time: str | None = None,
    account: str | None = None,
    qos: str | None = None,
    network: str | None = None,
    grace_period_seconds: int = 10,
)
```

When `topology=` is supplied:

- Every positional / keyword arg is wrapped in a `Peer(..., pool=topology.default_pool)` if it isn't already a `Peer`.
- Keyword args set the peer's `name` to the keyword (`parallel(ocean=ocean_peer, ...)` → peer named `"ocean"`).
- `on_peer_failure` is the fallback when a peer's own `on_failure` is `"kill"` or exceeds `max_restarts`.
- `time`, `account`, `qos`, `network` are defaults for every pool that doesn't specify its own.

When `topology=` is absent, the entire call is the 0.6 single-pool `parallel()` — fully backward compatible.

### 8.1 Result access

```python
job = parallel(..., topology=...)

# Per peer
job["learner"]                      # Job for a single peer
job["learner"].get_result()

# Per replica
job["inference", 3]                 # Job for replica 3 of the inference replica set
job["inference", 3].get_result()

# Aggregate
job["inference"].get_results()      # list of 8 results (len == count)

# Everything
job.get_results()                   # {"learner": ..., "inference": [...], ...}
```

Keyed access + integer tuple keys avoid the "ambiguous dict-or-list" problem the simple `parallel()` form has.

### 8.2 Composition

All the composition rules from `parallel_tasks.md` apply:

```python
# Depend on an earlier job
parallel(..., topology=TOPOLOGY).after(prep_job)

# Feed topology results into another task
rl_job = parallel(..., topology=TOPOLOGY)
evaluation = evaluate.after(rl_job)(checkpoint=rl_job["learner"])
```

`BoundTask.partial(...)` continues to be the way to pre-bind args.

## 9. End-to-end: what gets rendered

Taking the §4.4 cosmos-rl example, the SDK emits a single batch script of roughly:

```bash
#!/bin/bash
#SBATCH --job-name=rl
#SBATCH --time=24:00:00

# ───── component 0: pool "learn"
#SBATCH --partition=gpu-fat
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --gres=gpu:h100:8
#SBATCH --cpus-per-task=16
#SBATCH --mem=1.5T

#SBATCH hetjob
# ───── component 1: pool "serve"
#SBATCH --partition=gpu
#SBATCH --nodes=2
#SBATCH --gpus-per-node=8
#SBATCH --gres=gpu:a100:8
#SBATCH --cpus-per-task=8
#SBATCH --mem=512G

#SBATCH hetjob
# ───── component 2: pool "sim"
#SBATCH --partition=cpu
#SBATCH --nodes=4
#SBATCH --cpus-per-task=96
#SBATCH --mem=384G

set -eo pipefail
export JOB_DIR="$(pwd)/slurm_${SLURM_JOB_ID}"
mkdir -p "$JOB_DIR/shared"
python -m slurm.topology_bootstrap --job-dir "$JOB_DIR" --registry "$JOB_DIR/peers.json"

trap 'scancel "$SLURM_JOB_ID" "$SLURM_JOB_ID+1" "$SLURM_JOB_ID+2" 2>/dev/null' EXIT

# Peer: learner  (pool "learn")
srun --het-group=0 --exact --ntasks=1 \
     --cpus-per-task=16 --gpus-per-task=8 --mem=1.5T \
     --job-name=learner \
     "$PY" -m slurm.runner --step peer:learner --peer-registry "$JOB_DIR/peers.json" &
PID_LEARNER=$!

# Peer: replay  (pool "learn", shares node with learner)
srun --het-group=0 --overlap --exact --ntasks=1 \
     --cpus-per-task=32 --mem=1T \
     --job-name=replay \
     "$PY" -m slurm.runner --step peer:replay --peer-registry "$JOB_DIR/peers.json" &
PID_REPLAY=$!

# Peer: inference (pool "serve", replica set × 8)
srun --het-group=1 --exact --ntasks=8 --ntasks-per-node=4 \
     --cpus-per-task=8 --gpus-per-task=2 --mem=64G \
     --job-name=inference \
     "$PY" -m slurm.runner --step peer:inference:by-taskid --peer-registry "$JOB_DIR/peers.json" &
PID_INFERENCE=$!

# Peer: renderer (pool "serve", replica set × 16)
srun --het-group=1 --overlap --exact --ntasks=16 --ntasks-per-node=8 \
     --cpus-per-task=8 --gpus-per-task=1 --mem=32G \
     --job-name=renderer \
     "$PY" -m slurm.runner --step peer:renderer:by-taskid --peer-registry "$JOB_DIR/peers.json" &
PID_RENDERER=$!

# Peer: simulator (pool "sim", replica set × 64)
srun --het-group=2 --exact --ntasks=64 --ntasks-per-node=16 \
     --cpus-per-task=4 --mem=16G \
     --job-name=simulator \
     "$PY" -m slurm.runner --step peer:simulator:by-taskid --peer-registry "$JOB_DIR/peers.json" &
PID_SIMULATOR=$!

python -m slurm.topology_supervisor \
    --job-dir "$JOB_DIR" \
    --pids learner=$PID_LEARNER replay=$PID_REPLAY inference=$PID_INFERENCE \
           renderer=$PID_RENDERER simulator=$PID_SIMULATOR \
    --policy '{"learner":"kill","replay":"kill","inference":"restart:2","renderer":"ignore","simulator":"ignore"}' \
    --fallback kill \
    --grace 10

EXIT=$?
wait
exit $EXIT
```

Key points:

- **`--overlap`** on the replay + renderer peers lets them share the pool's nodes with learner/inference respectively. Without `--overlap` the second peer on the same component would fail to schedule.
- **`by-taskid`** mode on the runner tells it to read `SLURM_PROCID` and pick `peer_<name>_<i>_args.pkl`. This reuses the multi-step serialization story from `parallel_tasks.md`.
- **`slurm.topology_bootstrap`** writes the peer registry skeleton with resolved hostnames (from `SLURM_JOB_NODELIST_HET_GROUP_N`).
- **`slurm.topology_supervisor`** is a Python process that owns the lifecycle: waits on step PIDs, consults per-peer failure policies, re-launches steps for `restart`, and signals the rest on terminal failure. Doing this in Python (instead of bash) is necessary for the restart policy and for user callbacks.

## 10. Failure model

### 10.1 Policy resolution order

For each peer, on failure:

1. Apply the peer's own `on_failure`.
2. For `"restart"`, attempt up to `max_restarts`, each with a fresh `step_id` and `restart_count` incremented in the registry.
3. For `"callback"`, invoke the callback with the peer's `JobSnapshot`. The callback's return value (`"kill"` / `"ignore"`) replaces the policy for this failure.
4. If the peer's policy resolves to `"kill"` (either directly or as a restart fallback), consult the parent `parallel(..., on_peer_failure=...)`:
   - `"kill"` (default) — signal every other peer, drain the grace window, abort.
   - `"ignore"` — the allocation continues without this peer; its `Job` reports failure but `parallel_job.get_results()` returns partial.

### 10.2 Cascading shutdown

When the supervisor decides to abort, it:

1. Sends `scancel --signal=TERM` to every step in every hetjob component.
2. Waits `grace_period_seconds` (default 10) so peers can flush state.
3. Sends an unconditional `scancel` on the whole hetjob family.

This matches the leader+sidecar model in `parallel_tasks.md` and reuses `JobContext.shutdown_requested`.

### 10.3 Partial success

`job.get_results()` returns a dict with every peer's status. Use `job.peer_outcomes` to distinguish success / ignored-failure / fatal:

```python
results = job.get_results()
for peer, outcome in job.peer_outcomes.items():
    if outcome.status == "ignored_failure":
        logger.warning("peer %s died but on_failure=ignore", peer)
```

## 11. Validation

Before submission the SDK runs a battery of pool/peer checks and emits one aggregated error if any fail. Each check produces a concrete message with enough data to fix:

```
TopologyError: invalid topology for parallel():
  • Peer 'renderer' requests gpus_per_task=1 but pool 'sim' (→ cpu) has no GPUs.
    Suggestion: pool='serve' (a100, gpus_per_node=8).
  • Pool 'learn' capacity exceeded:
      requested  cpus = 16 (learner) + 32 (replay) = 48 per-task slots
      available  cpus = 128 per-node × 1 node = 128
      OK on CPU.
      requested  gpus = 8 (learner) + 0 (replay) = 8 per-task slots
      available  gpus = 8 per-node × 1 node = 8
      OK on GPU.
      requested  mem  = 1.5T (learner) + 1T (replay) = 2.5T
      available  mem  = 1.5T per-node × 1 node = 1.5T
      Over by 1.0T. Raise pool.mem_per_node or drop replay into its own pool.
  • Peer 'inference' name collides with reserved registry key 'ports'.
```

The goal is that topology errors don't make it past `sbatch` — failing a 24-hour RL job after 30 seconds because a pool was mis-sized is a terrible user experience.

## 12. Advanced placement (escape hatches)

Hatches are explicit and ugly on purpose — reach for them when the first-class API isn't enough, but not before.

### 12.1 `srun_args=[...]` per peer

Appended verbatim to that peer's step. Use cases:

```python
# NVLink-aware binding for a fat DDP step
Peer(learner.partial(cfg=cfg), pool="learn",
     srun_args=["--gpu-bind=closest", "--cpu-bind=ldoms"])

# Block distribution across 2 nodes
Peer.replicas(inference, count=16, pool="serve",
              srun_args=["--distribution=block:block"])

# Pin to specific nodes within the pool (rarely needed)
Peer(debug.partial(), pool="serve",
     srun_args=["--nodelist=gpu-node02"])
```

### 12.2 `Pool(extra_sbatch={...})`

Raw SBATCH passthrough for component-level flags we don't model:

```python
Pool(
    nodes=4, gpus_per_node=8, partition="gpu",
    extra_sbatch={
        "switches": "2@10:00",           # --switches=2@10:00
        "bb": "capacity=1TB",            # --bb=capacity=1TB (burst buffer)
        "network": "nopm",
    },
)
```

### 12.3 `Topology(prolog=..., epilog=...)`

Shell snippets inserted before the first peer and after the last. For cluster-specific initialization (module loads, EFA setup, license servers). They run **once** per allocation, on the allocation's login node — not per step.

### 12.4 Fully-hand-written hetjob

If a user has an existing `#SBATCH`-laden script they need to preserve bit-for-bit, `Cluster.submit_raw(script, ...)` remains the escape. That's pre-existing, not something this design adds.

## 13. Composition with existing SDK

| Combination                                              | Supported?                                                   |
| -------------------------------------------------------- | ------------------------------------------------------------ |
| `parallel(..., topology=T).after(prep_job)`              | Yes — dependency applies to the whole hetjob.               |
| `analysis.after(parallel_job)(...)`                      | Yes — downstream waits on the entire allocation.            |
| `@workflow` that contains a `parallel(..., topology=T)`  | Yes — it's just another submission from the driver.         |
| `parallel(task.map(items), ..., topology=T)`             | **No.** Array peers not supported; use `Peer.replicas(...)`. |
| `Peer(task.with_sidecars(...), ...)`                     | **No** in 0.7 — a peer's body is a single task.             |
| `parallel(..., topology=T).map(configs)`                 | **No.** Re-submit the topology N times at the caller level. |
| Nested `parallel(..., topology=T)` inside a peer         | **No** in 0.7 — no nested allocations. Workaround: submit child allocations from a workflow. |

## 14. Local mode

`backend_type="local"` still works for topology — with obvious limitations:

- Pools are honoured for **CPU/RAM budgeting** against the local machine. `gpus_per_node` asserts that the local GPU count suffices; otherwise errors.
- Constraints (`partition`, `gpu_type`, `constraint=`, `reservation`) are validated for syntax and ignored for matching (no real Slurm to consult).
- All peers run as subprocess steps, coordinated by `topology_supervisor`.
- This keeps the cosmos-rl example runnable on a workstation with N GPUs for integration testing, at smaller `count=` values.

## 15. Non-goals

- **Elastic / dynamic replica counts.** Scale-up and scale-down during a single allocation is not supported. Use `@workflow` + re-submission for that.
- **Cross-allocation discovery.** Peers in a topology can only discover other peers from the same `parallel(...)` call.
- **Full Slurm flag coverage.** We model the common 90%. The long tail lives in `srun_args=` / `extra_sbatch=` / `submit_raw`.
- **Automatic pool sizing.** The user declares pool shape. The SDK does not try to infer "you probably need 3 nodes" from peer specs. Predictable > magic for 24-hour jobs.
- **Cross-peer RPC abstractions.** Hostnames + ports + `shared_dir` is the contract; frameworks (NCCL, Ray, gRPC) live in user code.

## 16. Phasing

| Phase       | Scope                                                                                                                                                         |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **0.7.0**   | `Pool`, `Topology`, `Peer`, `Peer.replicas`, `parallel(topology=)`, hetjob rendering, `topology_bootstrap` + `topology_supervisor`, `ctx.peers`, `ctx.announce`, failure policies (`kill`/`ignore`/`callback`), per-peer `srun_args`, local-mode parity. |
| **0.7.1**   | `on_failure="restart"`, `@task(ports={...})` + `ctx.reserve_port`, `Topology(prolog/epilog)`, richer validation messages, MkDocs cookbook page with cosmos-rl example. |
| **Later**   | Topology testing utilities (dry-run that prints the rendered script without submitting), `parallel.map(topology, param_sets)` for ensemble sweeps over topologies, observability integrations (per-peer Grafana panel generation). |

---

## Appendix A: `Pool` field reference

```python
Pool(
    nodes,                  # int. Exact node count for this hetjob component.
    cpus_per_node=None,     # int | None. --cpus-per-task × tasks-per-node budget.
    mem_per_node=None,      # "64G" | "512G" | "1T". Parsed with slurm.validation.parse_mem.
    gpus_per_node=None,     # int | None. GPU count per node.
    partition=None,         # str | None. --partition.
    qos=None,               # str | None. --qos.
    account=None,           # str | None. --account.
    constraint=None,        # str | None. Boolean expr, e.g. "h100&nvlink4".
    gpu_type=None,          # str | None. GRES sugar: --gres=gpu:<type>:<gpus_per_node>.
    features=[],            # list[str]. AND-joined with constraint.
    reservation=None,       # str | None. --reservation.
    exclude_nodes=None,     # list[str] | None. --exclude=n01,n02.
    time=None,              # "HH:MM:SS" | None. Override parallel(time=).
    exclusive=False,        # bool. --exclusive on this component.
    gres={},                # dict[str, str]. Raw GRES.
    extra_sbatch={},        # dict[str, Any]. Raw passthrough.
)
```

## Appendix B: `Peer` field reference

```python
Peer(
    task_or_bound,              # SlurmTask | BoundTask.
    pool="<default>",           # str. Must match a key in Topology.pools.
    name=None,                  # str | None. Defaults to task __name__.
    on_failure="kill",          # "kill" | "ignore" | "restart" | "callback".
    max_restarts=0,             # int. Only meaningful with on_failure="restart".
    callback=None,              # Callable[[JobSnapshot], Literal["kill","ignore"]] | None.
    exclusive=False,            # bool. Per-step --exclusive.
    tasks_per_node=None,        # int | None. Replica set only; packing density.
    srun_args=[],               # list[str]. Verbatim srun flags.
    announce=None,              # dict | None. Static registry metadata.
)

Peer.replicas(
    task_or_bound,
    count,                      # int. Replica count (srun --ntasks=).
    args=None,                  # list | Callable[[int], ...] | range | None.
    pool="<default>",
    name=None,
    on_failure="kill",
    max_restarts=0,
    callback=None,
    tasks_per_node=None,
    srun_args=[],
    announce=None,
)
```

## Appendix C: `JobContext` additions for topology

```python
@dataclass(frozen=True)
class JobContext:
    # … existing fields …

    # Set only inside a topology-parallel allocation:
    peer_name: str | None = None          # e.g. "inference"
    peer_pool: str | None = None          # e.g. "serve"
    replica_index: int | None = None       # 0..count-1 for replica sets
    replica_count: int | None = None       # count
    peers: Mapping[str, "PeerGroup"] = field(default_factory=dict)
    my_ports: Mapping[str, int] = field(default_factory=dict)

    def announce(self, **fields: Any) -> None: ...
    def reserve_port(self, name: str) -> int: ...

@dataclass(frozen=True)
class PeerInfo:
    name: str
    replica_index: int
    hostname: str
    hostnames: tuple[str, ...]
    step_id: str
    pool: str
    ports: Mapping[str, int]
    metadata: Mapping[str, Any]
    state: Literal["pending", "ready", "failed"]
    restart_count: int = 0
```

## Appendix D: hetjob signal handling notes

- `scancel $SLURM_JOB_ID` signals only component 0 by default. Always include `$SLURM_JOB_ID+1`, `+2`, … for the remaining components.
- `SLURM_JOB_NODELIST` inside the batch script is the login node of the submission; per-component nodelists are in `SLURM_JOB_NODELIST_HET_GROUP_<n>`. The bootstrap reads those to seed the peer registry.
- `--overlap` is required when two steps in the same component share nodes. Without it the second step blocks forever waiting for resources Slurm has already given to the first step.
- `sacct -j <jobid>.batch --format=JobID,State,ExitCode` returns one row per component. The supervisor consolidates them for the SDK's status surface.
