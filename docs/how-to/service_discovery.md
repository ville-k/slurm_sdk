# Discover other peers at runtime

## Problem

You have N peers running in one allocation and one of them needs to dial
another — a worker connecting to a coordinator, an inference replica
fetching weights from the learner, a sibling iterating over the ready
simulators. You do not know the hostnames or ports until Slurm allocates
the nodes, and you want to avoid a shared config file or an out-of-band
coordination server.

## Solution

Every peer in a `parallel(...)` allocation has access to a shared registry
through `JobContext`:

- `ctx.peers[name]` — all replicas of a named peer as a `PeerGroup`.
- `ctx.nodes[label_or_ordinal]` — nodes in the allocation as a `NodeGroup`.
- `ctx.my_ports` — any ports reserved for this peer (see below).
- `ctx.announce(**fields)` — publish metadata to the registry atomically.

**How the registry gets populated.** Bootstrap seeds the skeleton (pool
layout, node labels, pinned-peer hostnames) before any peer runs. At
startup, each peer's runner publishes its actual hostname and
`SLURM_STEP_ID` to its own entry — unpinned peers show up with
`hostname=""` until this publish completes, so callers that need the
hostname should either gate on it via `wait_all(keys=["hostname"])` or
rely on the `"ready"` signal the peer itself announces. `wait_all`
treats `hostname`, `step_id`, `node_label`, and `ports` as top-level
runtime fields; any other key name is checked against announced
`metadata`. Ports declared via `@task(ports=...)` are published at the
same time. Mid-run announcements from user code merge atomically into
the calling peer's `metadata` under a file lock — concurrent
`announce()` calls from different peers never overwrite each other's
fields. Discovery treats peer entries as the source of truth for runtime
placement/state and node entries as the source of truth for allocation
inventory; cross-links such as `ctx.node.peers` and `PeerInfo.node_label`
are derived on read rather than trusted from duplicated cached fields.

## Find another peer and connect

A worker dials a coordinator — both are peers in the same allocation:

```python
from slurm import Cluster, JobContext, Peer, parallel, task


@task(cpus_per_task=1, mem="512M", ports={"rpc": "auto"})
def coordinator(ctx: JobContext) -> None:
    port = ctx.my_ports["rpc"]
    ctx.announce(ready=True, endpoint=f"tcp://{ctx.node.hostname}:{port}")
    serve_forever(port)


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

- `@task(ports={"rpc": "auto"})` reserves an ephemeral port and records
  it in the registry before the user function runs. The number is
  available as `ctx.my_ports["rpc"]`.
- `ctx.announce(ready=True, endpoint=...)` appends arbitrary metadata to
  the coordinator's registry entry. The write is atomic (`write_tmp + rename`) so workers reading concurrently never see a partial update.
- `ctx.peers["coordinator"].wait_all(keys=["endpoint"], timeout=60)`
  polls the registry until every coordinator replica has announced an
  `endpoint` key (or times out). Starts at a 50ms poll and backs off to
  1s.
- `ctx.peers["coordinator"].first` returns the first replica's
  `PeerInfo` — a frozen snapshot with `hostname`, `ports`, `metadata`,
  `node_label`, `state`, and `replica_index`.

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
calls `ctx.announce(ready=True)` — the runner publishes hostname, step
id, and ports at startup but leaves `state="pending"` so readiness
remains the replica's own promise ("I have finished setup, you can
dial me"). `ready_only()` skips replicas still in `pending` and
replicas that already `failed`. Callers that need to block until a
specific runtime field lands — even without a `ready` signal — can use
`ctx.peers["simulator"].wait_all(keys=["hostname"])`. The same top-level
field behavior works for `step_id`, `node_label`, and `ports`; other
keys still wait on `metadata`.

Call `ctx.peers["simulator"].refresh()` between iterations to pick up
peers that came up after your first read — `PeerGroup` holds a snapshot,
not a live handle.

## Discover nodes by label or ordinal

`ctx.nodes` indexes the allocation's nodes — useful for per-node roles:

```python
@task(...)
def trainer(ctx: JobContext) -> None:
    head = ctx.nodes["head"]               # by label (see Pool(node_labels=))
    primary = ctx.nodes[0]                  # ordinal — index within my pool
    all_serve = list(ctx.nodes.in_pool("serve"))

    # Who else is on my node?
    for peer_name in ctx.node.peers:
        log.info("sharing node %s with %s", ctx.node.hostname, peer_name)
```

- **String key** (`ctx.nodes["head"]`) resolves by label across every
  pool.
- **Integer key** (`ctx.nodes[0]`) resolves by ordinal within the current
  peer's pool — unambiguous because the pool is known.
- **`ctx.nodes.in_pool(name)`** iterates every node in a named pool.
- **`ctx.node`** is the `NodeInfo` for *this* process's host. Its
  `peers` field lists every peer that landed on the same node — handy
  for "one metrics agent per node" patterns. That membership is derived
  from the live peer entries, so stale `nodes[*].peers` caches do not
  leak into discovery.

## Bidirectional handshake

When two peers need each other, have both `announce` on the way up and
both `wait_all` before proceeding:

```python
@task(ports={"control": "auto"})
def peer_a(ctx: JobContext) -> None:
    ctx.announce(control_port=ctx.my_ports["control"])
    ctx.peers["peer_b"].wait_all(keys=["control_port"], timeout=30)
    b = ctx.peers["peer_b"].first
    handshake(a_port=ctx.my_ports["control"],
              b_host=b.hostname,
              b_port=b.metadata["control_port"])


@task(ports={"control": "auto"})
def peer_b(ctx: JobContext) -> None:
    ctx.announce(control_port=ctx.my_ports["control"])
    ctx.peers["peer_a"].wait_all(keys=["control_port"], timeout=30)
    a = ctx.peers["peer_a"].first
    handshake(a_port=a.metadata["control_port"],
              a_host=a.hostname,
              b_port=ctx.my_ports["control"])
```

Both peers write then wait. Because the registry is shared and updates are
atomic, the order of `announce` calls doesn't matter — each side blocks
until the other has published.

## Reserve additional ports mid-function

If you need a port after user code has started, `ctx.reserve_port(name)`
binds an ephemeral socket, records it in the registry, and returns the
number:

```python
@task()
def server(ctx: JobContext) -> None:
    admin_port = ctx.reserve_port("admin")
    serve_admin(admin_port)
```

The reservation is published to the registry before the function returns,
so peers that `wait_all(keys=["admin"])` see the port as soon as it is
bound. The decorator form (`@task(ports={...})`) is preferred when you
know the names up front — it runs before user code.

## Reserved keys in `announce`

The registry reserves key names that the bootstrap and supervisor manage:
`name`, `replica_index`, `replica_count`, `hostname`, `hostnames`, `pool`,
`node_label`, `step_id`, `state`, `ports`, `restart_count`. Using them in
`ctx.announce(...)` raises `ValueError`. Pick a more specific name for
your payload (`endpoint`, `control_port`, `ready`).

## Verification

- `job.snapshot()` returns per-peer `JobSnapshot` — peek at `stdout_tail`
  to confirm the handshake messages fired.
- `cat $JOB_DIR/registry.json` after a run prints the final state; useful
  for confirming that your `announce(...)` landed as expected.
- In local mode, `ctx.peers["name"].first.hostname` is your workstation
  host, but the service discovery surface is otherwise identical — you can
  debug coordination logic without a real cluster.

## See also

- [Parallel reference](../reference/parallel.md) — `PeerGroup`,
  `PeerInfo`, `NodeGroup`, `NodeInfo` have full docstrings.
- [How parallel allocations work](../explanation/slurm_allocations.md#what-the-registry-actually-contains)
  — the registry layout and write semantics.
- [Your first multi-peer job](../tutorials/parallel_tasks.md#7-discover-other-peers-at-runtime)
  — end-to-end runnable walkthrough.
