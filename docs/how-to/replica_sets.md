# Run N copies of a task in parallel

## Problem

You have one task that needs to run with many different inputs — data
shards, environment seeds, worker ids — and you want all copies running
concurrently inside a single allocation with one result handle.

Array jobs (`task.map(items)`) submit a separate allocation per batch and
are the right tool for embarrassingly parallel batch work. Replica sets are
the right tool when every copy must run **inside one `parallel(...)`
allocation** — because it shares node pinning with other peers, because it
participates in cross-peer service discovery, or because pool shape matters.

## Solution

`Peer.replicas(task, count=N, args=...)` declares N copies of `task` as one
peer. The SDK compiles it to a single `srun --ntasks=N` step (or N local
subprocesses in local mode). Every replica sees its own `SLURM_PROCID` and
the runner feeds it the matching args.

### Feed args with a list

Use a list when you have explicit per-replica inputs:

```python
from slurm import Peer, parallel, task


@task(cpus_per_task=2, mem="2G")
def process(shard: str, chunk: int) -> dict:
    return {"shard": shard, "chunk": chunk}


job = parallel(
    workers=Peer.replicas(
        process,
        count=3,
        args=[
            {"shard": "train", "chunk": 0},
            {"shard": "val",   "chunk": 1},
            {"shard": "test",  "chunk": 2},
        ],
    ),
)
```

List entries are kwarg dicts, positional tuples, or scalars (which become
the first positional arg). Length must equal `count` — a mismatch is a
validation error before submission.

### Feed args with a callable

Use a callable when the args derive from the replica index:

```python
job = parallel(
    workers=Peer.replicas(
        process,
        count=64,
        args=lambda i: {"shard": "train", "chunk": i},
    ),
)
```

The callable runs on the submitting machine, once per replica, with the
0-based replica index. It is the most compact form for large fan-outs.

### Feed args with a range

Use a `range` when the index *is* the argument:

```python
job = parallel(
    workers=Peer.replicas(process, count=8, args=range(8)),
)
```

Each replica receives its index as the first positional argument — handy
when the task takes a single integer.

## Read the replica index inside the task

You can always pick the replica index up from the `JobContext`:

```python
from slurm import JobContext


@task(cpus_per_task=1, mem="1G")
def worker(ctx: JobContext) -> int:
    assert ctx.replica_index is not None
    return ctx.replica_index
```

`ctx.replica_index` is `None` for singleton peers and `0..count-1` for
replicas. `ctx.replica_count` is the total, mirroring the Slurm task count
of the step. These are populated for both singleton and replica peers, so
branching on `ctx.replica_count > 1` is a portable way to vary behaviour.

## Collect replica results as a list

`get_results()` returns a list for replica peers, keyed by peer name:

```python
with cluster:
    job = parallel(
        workers=Peer.replicas(process, count=4, args=range(4)),
    )
    job.wait()
    results = job.get_results()

for r in results["workers"]:
    print(r)
```

- `results["workers"]` is `list[Any]` of length 4 — one entry per replica,
  in index order.
- Singleton peers return a scalar in the same dict, so a mixed job
  produces `{"leader": scalar_value, "workers": [r0, r1, ...]}`.
- `job["workers"]` returns a `ReplicaGroup` handle if you prefer
  working with the per-replica `Job` objects (iterate / index / call
  `.get_results()` on the group).
- `job["workers", 3]` is the individual `Job` for replica 3.

## Per-replica failure policies

`on_failure` applies per-replica within a replica set. Use `"restart"` to
retry flaky workers and `"continue"` for fan-outs where partial success is
acceptable:

```python
Peer.replicas(
    scrape,
    count=100,
    args=lambda i: {"url": urls[i]},
    on_failure="continue",
)

Peer.replicas(
    inference,
    count=16,
    args=lambda i: {"worker_id": i},
    on_failure="restart",
    max_restarts=2,
)
```

Replica peers are **never leaders** — their failure policy applies per
replica, not to the group. Declare the orchestrating peer separately with
`Peer(coordinator, leader=True)` if you need the leader + helpers shape.

## Pin replicas to specific nodes

When pool shape matters, pin each replica to a specific labeled node with
`on_nodes=[...]`:

```python
Pool("main", nodes=4, gpus_per_node=8,
     node_labels=["head", "aux", "spare1", "spare2"])

Peer.replicas(
    worker,
    count=2,
    pool="main",
    on_nodes=["spare1", "spare2"],  # replica 0 → spare1, replica 1 → spare2
)
```

- Length of `on_nodes` must equal `count` — a length mismatch is a
  validation error.
- Labels can be strings (from `Pool.node_labels`) or 0-based ordinals.
- Use `tasks_per_node=N` to control pack density when you want multiple
  replicas on the same node; the SDK defaults to packing densely based on
  pool shape.

## Verification

- `len(results["workers"])` should equal `count`.
- `job.peer_outcomes()` reports per-replica status as `worker[0]`,
  `worker[1]`, …; the replica index is baked into the key.
- In local mode the supervisor launches N subprocesses. `ps` during the run
  shows them as siblings of the supervisor Python process.

## See also

- [Parallel reference](../reference/parallel.md) — the `Peer.replicas`
  docstring has the full parameter list.
- [Deploy a heterogeneous topology](heterogeneous_topology.md) — real-
  world replica sets across different pools.
- [Choosing a Parallelization Pattern](parallelization_patterns.md) — when
  to reach for `task.map` vs. `Peer.replicas`.
