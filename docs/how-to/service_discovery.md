# Discover other peers at runtime

## Problem

You have N peers running in one allocation and one of them needs to dial
another — a worker connecting to a coordinator, an inference replica
fetching weights from the learner, a sibling iterating over the ready
simulators. You do not know the hostnames until Slurm allocates the nodes,
and you want to avoid a shared config file or an out-of-band coordination
server.

## Solution

Every peer in a `parallel(...)` allocation has access to a shared registry
through `JobContext`:

- `ctx.peers[name]` — all replicas of a named peer as a `PeerGroup`.
- `ctx.announce(**fields)` — publish metadata to the registry atomically.
- `ctx.shared_dir` — a directory every peer can read and write, for files
  too large to pass through the registry.

**How the registry gets populated.** Bootstrap seeds the skeleton (pool
layout, pinned-peer hostnames) before any peer runs. At startup, each peer's
runner publishes its actual hostname and `SLURM_STEP_ID` to its own entry —
unpinned peers show up with `hostname=""` until this publish completes, so
callers that need the hostname should either gate on it via
`wait_all(keys=["hostname"])` or rely on the `"ready"` signal the peer
itself announces. `wait_all` treats `hostname` and `step_id` as top-level
runtime fields; any other key name is checked against announced `metadata`.
Mid-run announcements from user code merge atomically into the calling
peer's `metadata` under a file lock — concurrent `announce()` calls from
different peers never overwrite each other's fields.

## Find another peer and connect

A worker dials a coordinator — both are peers in the same allocation:

```python
import socket

from slurm import Cluster, JobContext, Peer, parallel, task


@task(cpus_per_task=1, mem="512M")
def coordinator(ctx: JobContext) -> None:
    host = socket.gethostname()
    ctx.announce(ready=True, endpoint=f"tcp://{host}:50051")
    serve_forever(50051)


@task(cpus_per_task=1, mem="512M")
def worker(ctx: JobContext) -> dict:
    # Block until the coordinator has announced its endpoint.
    ctx.peers["coordinator"].wait_all(keys=["endpoint"], timeout=60)
    coord = ctx.peers["coordinator"].first
    endpoint = coord.metadata["endpoint"]
    return {"dialed": endpoint, "coordinator_host": coord.hostname}


with Cluster.from_env("dev") as cluster:
    job = parallel(
        Peer(coordinator, leader=True),
        workers=Peer.replicas(worker, count=4),
    )
```

How each piece fits:

- `ctx.announce(ready=True, endpoint=...)` appends arbitrary metadata to
  the coordinator's registry entry. The write is atomic (`write_tmp + rename`) so workers reading concurrently never see a partial update.
- `ctx.peers["coordinator"].wait_all(keys=["endpoint"], timeout=60)`
  polls the registry until every coordinator replica has announced an
  `endpoint` key (or times out). Starts at a 50ms poll and backs off to
  1s.
- `ctx.peers["coordinator"].first` returns the first replica's
  `PeerInfo` — a frozen snapshot with `hostname`, `metadata`, `state`,
  and `replica_index`.

Pick a fixed, well-known port for the service (here `50051`) and bind it
with `SO_REUSEADDR`. Peers learn *where* to dial from the announced
hostname; the port is part of your application contract.

## Iterate ready siblings only

When a peer spawns many replicas and you want to connect to whichever
ones are already up, use `ready_only()`:

```python
@task(...)
def aggregator(ctx: JobContext) -> list[str]:
    connections = []
    for sim in ctx.peers["simulator"].ready_only():
        connections.append(sim.hostname)
    return connections
```

A replica's `state` flips to `"ready"` only when the replica itself
calls `ctx.announce(ready=True)` — the runner publishes hostname and step
id at startup but leaves `state="pending"` so readiness remains the
replica's own promise ("I have finished setup, you can dial me").
`ready_only()` skips replicas still in `pending` and replicas that already
`failed`. Callers that need to block until a specific runtime field lands —
even without a `ready` signal — can use
`ctx.peers["simulator"].wait_all(keys=["hostname"])`. The same top-level
field behavior works for `step_id`; other keys still wait on `metadata`.

Call `ctx.peers["simulator"].refresh()` between iterations to pick up
peers that came up after your first read — `PeerGroup` holds a snapshot,
not a live handle.

## Bidirectional handshake

When two peers need each other, have both `announce` on the way up and
both `wait_all` before proceeding:

```python
@task()
def peer_a(ctx: JobContext) -> None:
    ctx.announce(endpoint=f"{socket.gethostname()}:50060")
    ctx.peers["peer_b"].wait_all(keys=["endpoint"], timeout=30)
    b = ctx.peers["peer_b"].first
    handshake(peer=b.metadata["endpoint"])


@task()
def peer_b(ctx: JobContext) -> None:
    ctx.announce(endpoint=f"{socket.gethostname()}:50060")
    ctx.peers["peer_a"].wait_all(keys=["endpoint"], timeout=30)
    a = ctx.peers["peer_a"].first
    handshake(peer=a.metadata["endpoint"])
```

Both peers write then wait. Because the registry is shared and updates are
atomic, the order of `announce` calls doesn't matter — each side blocks
until the other has published.

## Share large artifacts through `ctx.shared_dir`

The registry is for small key/value coordination. When peers need to hand
off bigger payloads — a checkpoint, a dataset shard, a pickled config —
write them to `ctx.shared_dir`, a directory every peer in the allocation
can reach, and announce the path:

```python
@task()
def producer(ctx: JobContext) -> None:
    out = ctx.shared_dir / "weights.pt"
    save_weights(out)
    ctx.announce(weights_path=str(out))


@task()
def consumer(ctx: JobContext) -> None:
    ctx.peers["producer"].wait_all(keys=["weights_path"], timeout=120)
    path = ctx.peers["producer"].first.metadata["weights_path"]
    load_weights(path)
```

## Reserved keys in `announce`

The registry reserves key names that the bootstrap and supervisor manage.
Using any of them in `ctx.announce(...)` raises `ValueError`; the error
lists the full set. Pick a more specific name for your payload
(`endpoint`, `weights_path`, `ready`).

## Verification

- `job.snapshot()` returns per-peer `JobSnapshot` — peek at `stdout_tail`
  to confirm the handshake messages fired.
- `cat $JOB_DIR/registry.json` after a run prints the final state; useful
  for confirming that your `announce(...)` landed as expected.
- In local mode, `ctx.peers["name"].first.hostname` is your workstation
  host, but the service discovery surface is otherwise identical — you can
  debug coordination logic without a real cluster.

## See also

- [Parallel reference](../reference/parallel.md) — `PeerGroup` and
  `PeerInfo` have full docstrings.
- [How parallel allocations work](../explanation/slurm_allocations.md#what-the-registry-actually-contains)
  — the registry layout and write semantics.
- [Your first multi-peer job](../tutorials/parallel_tasks.md#7-discover-other-peers-at-runtime)
  — end-to-end runnable walkthrough.
