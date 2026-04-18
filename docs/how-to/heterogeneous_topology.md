# Deploy a heterogeneous topology across node types

## Problem

Your workload needs different node shapes for different jobs: a handful of
H100 nodes for training, A100 nodes for inference, and CPU-only boxes for
simulation. They must run in one atomic allocation so every piece starts
together, but the SDK has to reserve each shape through a separate Slurm
partition with its own `--gres`, `--constraint`, and `--mem` settings.

You want to express this declaratively, keep service discovery working
across node groups, and have the job fail fast if any pool is mis-sized.

## Solution

Declare one `Pool` per node type in a `Topology`, then place peers into
their target pool with `Peer(..., pool="<name>")`. Pools compile to Slurm
heterogeneous-job components; each `Peer` becomes one step inside its
pool's component.

This is the cosmos-rl-style RL training shape — a learner, an inference
fleet, and CPU simulators, with a replay buffer colocated on the learner's
node and per-node telemetry on the spare learn-pool node.

```python
from slurm import Cluster, JobContext, Peer, Pool, Topology, parallel, task


TOPOLOGY = Topology(
    pools={
        "learn": Pool(
            nodes=2,
            gpus_per_node=8,
            cpus_per_node=128,
            mem_per_node="1.5T",
            gpu_type="h100",
            partition="gpu-fat",
            node_labels=["head", "ops"],     # first node → "head", second → "ops"
        ),
        "serve": Pool(
            nodes=2,
            gpus_per_node=8,
            cpus_per_node=64,
            mem_per_node="512G",
            gpu_type="a100",
            partition="gpu",
        ),
        "sim": Pool(
            nodes=4,
            cpus_per_node=96,
            mem_per_node="384G",
            partition="cpu",
        ),
    },
)


@task(gpus_per_task=8, cpus_per_task=32, mem="512G")
def learner(cfg: dict, ctx: JobContext) -> dict:
    ctx.announce(ready=True)
    # ... RL training loop reading rollouts from ctx.peers["simulator"] ...
    return {"final_reward": 93.2}


@task(cpus_per_task=8, mem="128G")
def replay_buffer(ctx: JobContext) -> None:
    # Hosts rollouts in RAM, colocated with the learner.
    ...


@task(cpus_per_task=4, mem="16G")
def node_telemetry(ctx: JobContext) -> None:
    import time
    while not ctx.shutdown_requested:
        emit_metrics(ctx.node.hostname)
        time.sleep(10)


@task(gpus_per_task=1, cpus_per_task=8, mem="64G")
def inference(worker_id: int, ctx: JobContext) -> None:
    # Looks up the learner for weight sync at runtime.
    learner = ctx.peers["learner"].first
    serve_weights_from(learner.hostname)


@task(cpus_per_task=4, mem="8G")
def renderer(shard_id: int, ctx: JobContext) -> None:
    ...


@task(cpus_per_task=2, mem="4G")
def simulator(env_id: int, ctx: JobContext) -> None:
    ...


with Cluster.from_env("prod") as cluster:
    job = parallel(
        # Learner pinned to the "head" node of the learn pool.
        Peer(learner.partial(cfg=cfg), pool="learn", on_node="head", leader=True),

        # Replay buffer forced onto the same node as the learner.
        Peer(replay_buffer, pool="learn", colocate_with="learner"),

        # Telemetry on the other learn-pool node.
        Peer(node_telemetry, pool="learn", on_node="ops", on_failure="continue"),

        # Inference fleet — 8 replicas across 2 A100 nodes (4 per node).
        Peer.replicas(
            inference,
            count=8,
            pool="serve",
            args=lambda i: {"worker_id": i},
            on_failure="restart",
            max_restarts=2,
        ),

        # Renderers share the serve pool with inference (Slurm overlaps them).
        Peer.replicas(
            renderer,
            count=16,
            pool="serve",
            args=lambda i: {"shard_id": i},
            on_failure="continue",
        ),

        # CPU simulators.
        Peer.replicas(
            simulator,
            count=64,
            pool="sim",
            args=lambda i: {"env_id": i},
            on_failure="continue",
        ),

        topology=TOPOLOGY,
        time="24:00:00",
    )

    final = job.leader_result
```

## How each piece maps to Slurm

| Declaration                               | What the SDK emits                                                            |
| ----------------------------------------- | ----------------------------------------------------------------------------- |
| `Pool(nodes=2, partition="gpu-fat", ...)` | `#SBATCH --nodes=2 --partition=gpu-fat ...` for that hetjob component.        |
| `Peer(..., pool="learn")`                 | `srun --het-group=0 ...` (component index 0).                                 |
| `Peer(..., on_node="head")`               | `srun --nodelist=<resolved-hostname>` — pinned at bootstrap.                  |
| `Peer(..., colocate_with="learner")`      | Inherits the target peer's resolved hostname. Same-pool only.                 |
| `Peer.replicas(count=8, ...)`             | One `srun --ntasks=8` step; each replica sees its own `SLURM_PROCID`.         |
| `on_failure="restart", max_restarts=2`    | Supervisor re-launches the step up to twice before falling through to `kill`. |
| `on_failure="continue"`                   | Supervisor logs the failure but keeps the group alive.                        |

Every pool key becomes one `#SBATCH hetjob` component in the generated
batch script. Slurm allocates every component atomically — the learner will
not start on an H100 node while the simulators are still queued.

## Name your pool nodes

`Pool(node_labels=[...])` gives you a stable name for each node in the
pool, resolved at bootstrap:

- `Peer(..., on_node="head")` pins a peer to the labeled node. You can also
  use integer ordinals (`on_node=0`) when labels are overkill.
- `ctx.nodes["head"]` looks the node up inside peer code — `NodeInfo` with
  `hostname`, `pool`, `peers` (who else landed there), and `ordinal`.
- Labels must be unique per pool and must not contain `.`, `[`, or `]`.

Use labels whenever a node has a logical role ("head", "ops", "coord"). Use
raw ordinals when nodes are interchangeable.

## Colocate peers with `colocate_with=`

`Peer(replay_buffer, pool="learn", colocate_with="learner")` forces the
replay buffer onto the learner's node — without you knowing the hostname
up front. The bootstrap resolves the dependency graph:

- **Same pool only.** Colocation across pools would land peers on
  incompatible hardware.
- **Chains are allowed.** `A colocate_with B`, `B on_node="head"` → `A` on
  "head". Cycles are detected and reported before submission.
- **Replica-set targets.** `Peer(agent, colocate_with="inference")` places
  `agent` on *every* node of the inference replica set — one node per
  inference node, handled by Slurm's per-node scheduling inside the
  reservation.

## Service discovery across pools

Peers on different node types still see each other through the shared
`registry.json`:

```python
@task(gpus_per_task=1)
def inference(worker_id: int, ctx: JobContext) -> None:
    # Block until the learner has announced itself.
    ctx.peers["learner"].wait_all(keys=["ready"], timeout=300)
    learner = ctx.peers["learner"].first
    sync_weights_from(learner.hostname, port=learner.ports.get("weights", 50051))

    # Iterate ready sibling simulators — cross-pool, no hardcoded hostnames.
    for sim in ctx.peers["simulator"].ready_only():
        connect(sim.hostname)
```

`ctx.peers[name]` returns a `PeerGroup` whose entries span every replica.
`wait_all(...)` polls the registry until the condition is met — use it
instead of `time.sleep()` for coordination.

## Validate before you submit

The validator aggregates every placement, colocation, and resource error
into one `TopologyError` before `sbatch` is called. Typical catches:

- Sum of GPU demand across peers pinned to the same node exceeds
  `gpus_per_node`.
- `Peer(on_node="unknown_label")` — the error lists available labels.
- `colocate_with=` crosses pool boundaries — the error names both pools.
- Cycle in the colocation graph — the error spells out the cycle.

A 24-hour RL job should never fail after 30 seconds because a pool was
mis-sized.

## Verification

- Run against local mode first with scaled-down `count=` and `nodes=1`
  pools — the supervisor, registry, and service discovery all work without
  Slurm, so you can debug the topology before it costs a real allocation.
- `squeue -j <id>` prints one line per hetjob component after submission;
  each component has its own sub-id (`<id>+0`, `<id>+1`, ...).
- `job.peer_outcomes()` reports per-peer terminal status: `success`,
  `continue_on_failure`, `restarted`, `fatal`, `shutdown_by_leader`,
  `not_started`.

## See also

- [Parallel reference](../reference/parallel.md) — full `Topology` /
  `Pool` / `Peer` docstrings.
- [Run N copies of a task in parallel](replica_sets.md) — patterns for
  `Peer.replicas`.
- [Discover other peers at runtime](service_discovery.md) — the
  `ctx.peers` / `ctx.nodes` / `ctx.announce` surface in detail.
- [How parallel allocations work](../explanation/slurm_allocations.md) —
  the hetjob + supervisor mechanics behind this example.
