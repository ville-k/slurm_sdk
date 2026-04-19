# How parallel allocations work

`parallel(...)` looks like a Python call but it reaches deep into Slurm. This
page explains what actually happens between `parallel(Peer(train), Peer(metrics))`
and the moment your first peer starts printing log lines. Understanding the
mechanics helps when you need to debug a stuck supervisor, interpret a hetjob
job id, or reason about why two peers share a node.

## One allocation, many steps

The SDK maps parallel workloads onto three stacked Slurm primitives:

- A **Slurm allocation** is one `sbatch` submission — a reservation of nodes
  for a bounded walltime. The allocation has one batch job id.
- A **heterogeneous job** (or *hetjob*) lets one submission request several
  node groups with different shapes. Components are separated by
  `#SBATCH hetjob` lines. Slurm gives each component its own nodelist via
  `$SLURM_JOB_NODELIST_HET_GROUP_<n>`.
- A **job step** is one `srun` invocation inside the allocation. Steps can
  overlap if they target different CPUs / GPUs, and a single allocation can
  run many concurrent steps.

`parallel(...)` uses all three. One `parallel(...)` call is always exactly one
allocation, regardless of how many pools or peers it contains. Each `Pool`
becomes one hetjob component. Each `Peer` — whether singleton or replica set —
becomes one job step (`srun`) inside its pool's component.

| SDK concept              | Slurm concept                     |
| ------------------------ | --------------------------------- |
| `parallel(...)` call     | One `sbatch` submission           |
| `Pool` in `Topology`     | One hetjob component              |
| `Peer` (singleton)       | One `srun` step with `--ntasks=1` |
| `Peer.replicas(count=N)` | One `srun` step with `--ntasks=N` |
| `Peer(leader=True)`      | Step whose exit triggers shutdown |

Because every peer shares the same base job id, `squeue -j <id>` prints one
line per component and `sacct -j <id>` attributes step-level accounting back to
the same allocation.

## Submission flow

The batch script does not contain your Python code. It does three things:
stage job state, run a one-shot bootstrap, and hand control to a long-lived
supervisor. Every peer is launched by that supervisor — not by the batch
script — which is what makes per-peer failure policies possible.

```mermaid
sequenceDiagram
    autonumber
    participant User as Your process
    participant Slurm as Slurm controller
    participant Batch as Batch script (head node)
    participant Boot as Bootstrap
    participant Sup as Supervisor
    participant Peer1 as Peer srun step
    participant Peer2 as Peer srun step

    User->>Slurm: sbatch parallel.sh
    Slurm-->>User: job id (hetjob)
    Slurm->>Batch: allocate nodes, start batch script
    Batch->>Boot: python -m slurm.parallel.topology_bootstrap
    Boot->>Boot: expand NODELIST_HET_GROUP_<n>
    Boot->>Boot: resolve on_node / colocate_with
    Boot->>Boot: write registry.json + plan.json
    Batch->>Sup: exec python -m slurm.parallel.topology_supervisor
    Sup->>Peer1: srun --het-group=0 ... --ntasks=1 ...
    Sup->>Peer2: srun --het-group=1 ... --ntasks=N ...
    Peer1-->>Sup: exit code
    Peer2-->>Sup: exit code
    Sup-->>Batch: final status
    Batch-->>Slurm: batch exit code
```

The bootstrap is a short Python program that runs once, on the batch head
node, before any peer launches. Its job is to translate abstract placement
directives into concrete hostnames:

- Parses `SLURM_JOB_NODELIST_HET_GROUP_0`, `_HET_GROUP_1`, etc., and expands
  each to a hostname list via `scontrol show hostnames`.
- Pairs `Pool.node_labels[i]` with `hostnames[i]` in allocation order so
  `on_node="head"` becomes `--nodelist=compute-07`.
- Topologically sorts the `colocate_with` graph and detects cycles.
- Writes `$JOB_DIR/registry.json` with one entry per peer replica. Pool,
  component, and hostname list are recorded up front; pinned peers get
  their hostname written, unpinned peers stay with `hostname=""` and
  `state="pending"`. Each peer's runner publishes its actual hostname
  and `SLURM_STEP_ID` at startup (see "What the registry actually
  contains" below).
- Serialises per-peer argument pickles so the runner can deserialise
  positional and keyword args when the step starts.
- Writes `$JOB_DIR/plan.json` — a static snapshot of every `srun` command
  line, failure policy, and restart budget. The supervisor executes from the
  plan; it never re-examines the spec.

Once the plan exists, the batch script `exec`s into the supervisor. The
supervisor owns everything that happens from there: launching steps, applying
failure policies, restarting peers, propagating shutdown signals, updating the
registry.

## Why a hetjob instead of one `sbatch` per pool?

A naive alternative is "submit one `sbatch` per pool, then stitch them
together." That approach breaks three guarantees `parallel(...)` relies on:

- **Atomic start.** Slurm schedules every hetjob component together or not at
  all. With independent submissions, an RL training run could start the
  learner while the inference fleet is still queued — peers would race
  against an empty registry and the learner would burn expensive node-hours
  waiting.
- **Single job id.** Cancellation, accounting, and dependency tracking
  (`afterok:<id>`) all work against one id. Multiple allocations fragment
  this surface and force userspace plumbing.
- **Cross-pool service discovery.** The bootstrap builds one `registry.json`
  that every peer reads. With separate allocations, the SDK would have to
  ship a side-channel (shared filesystem locks, a coordination server) to
  reach parity.

Hetjobs are the one Slurm primitive that gives you "different node shapes,
one atomic allocation." The SDK never submits multiple allocations for a
single `parallel(...)` call.

## Supervisor lifecycle

The supervisor is a plain Python process. It launches each step with
`subprocess.Popen(["srun", ...])`, watches PIDs with a `select` loop, updates
the registry when steps exit, and applies the failure policy from the plan.
It also installs a `SIGTERM` handler so an external `scancel` propagates into
the same shutdown path that a leader exit takes.

```mermaid
stateDiagram-v2
    [*] --> Launching
    Launching --> Running: all peers started
    Running --> Running: peer exit (continue / success non-leader)
    Running --> ShuttingDown: leader exit
    Running --> Terminating: fatal peer (kill / restart exhausted / callback=kill)
    Running --> Terminating: external scancel / time limit
    ShuttingDown --> Drained: grace_period elapses
    Terminating --> Drained: grace_period elapses
    Drained --> [*]: supervisor exits

    state Running {
        [*] --> Watching
        Watching --> Watching: peer exit ok
        Watching --> Restarting: on_failure=restart + budget
        Restarting --> Watching: new step launched
    }
```

The three terminal paths from `Running` differ only in intent:

- **`ShuttingDown`** — a `leader=True` peer exited (success or failure). The
  supervisor writes `state="shutting_down"` for every remaining peer, sends
  `SIGTERM` through `scancel --signal=TERM` to every step, waits
  `grace_period_seconds`, then hard-cancels stragglers. Helpers whose
  `on_failure="continue"` are reported as `shutdown_by_leader` in
  `peer_outcomes()` — not `fatal`.
- **`Terminating`** — a peer with `on_failure="kill"` failed (or `restart`
  exhausted, or `callback` returned `"kill"`). Same mechanics as shutdown,
  but the overall job exit code is non-zero and `get_results()` raises
  `CompositeJobError`.
- **`Running → Drained`** without intermediate shutdown — every non-leader
  peer has succeeded and there is no leader. This is the symmetric-peer
  default: every peer is load-bearing, they all exit cleanly, the supervisor
  collects results and exits.

Restart is local to `Running`. When a peer with `on_failure="restart"` exits
with a non-zero code and its budget is not exhausted, the supervisor launches
a new `srun` with a fresh step id against the same `--het-group` and
nodelist. The peer's `restart_count` increments in the registry; its
`PeerOutcome.status` becomes `"restarted"` only if the restart eventually
succeeds. When the budget is exhausted, the policy falls through to
`"kill"` — the last failure is treated as fatal.

## Why `scancel --signal=TERM` instead of `kill`?

A bash `kill` in the batch script targets the local `srun` wrapper, not the
remote process running on the compute node. The wrapper eventually notices
its child disappeared and cleans up, but the peer process itself sees no
signal. `scancel --signal=TERM <jobid>.<stepid>` asks Slurm to propagate the
signal through its step daemons, which delivers it to the remote process
group directly. That is what lets a peer handle `SIGTERM`, flush state, and
exit within the grace window.

The same mechanism is why `ctx.shutdown_requested` works at all. The runner
installs a `SIGTERM` handler that flips a thread-safe flag; pure-Python
sidecars poll it (`while not ctx.shutdown_requested:`) and subprocess-based
sidecars (`tensorboard`, `node-exporter`) inherit the signal through their
process group.

## What the registry actually contains

`$JOB_DIR/registry.json` is the system of record for the allocation.
Bootstrap seeds the skeleton; each peer's runner publishes its actual
hostname, step id, and declared ports at startup (via
`update_peer_hostinfo` / `update_peer_ports`); the supervisor writes
terminal outcomes; user code reads the whole thing through `ctx.peers` /
`ctx.nodes`. Every write goes through `write_tmp + rename` for
atomicity, and every read-modify-write takes an `fcntl.flock` on
`<registry>.lock` so concurrent `announce()` calls from different peers
can't overwrite each other's fields.

A minimal registry for a two-peer job looks roughly like:

```json
{
  "peers": {
    "train":   [{"state": "ready", "hostname": "gpu-07", "step_id": "12345.0", ...}],
    "metrics": [{"state": "ready", "hostname": "gpu-07", "step_id": "12345.1", ...}]
  },
  "nodes": {
    "gpu-07": {"pool": "default", "peers": ["train", "metrics"]}
  }
}
```

Readers cache the deserialised snapshot. `ctx.peers["train"].refresh()` re-
reads the file on demand — useful when waiting for a peer that hasn't
announced yet. `PeerGroup.wait_all(...)` encapsulates the poll loop.

## How local mode fakes all of this

When `Cluster(backend_type="local")` runs a `parallel(...)` submission on a
workstation without `sbatch` in the `PATH`, the batch script is still
generated, but the supervisor is invoked directly via `subprocess.Popen`
instead of `sbatch`. Peers launch as plain child processes — no `srun`,
no step ids — but the supervisor state machine, the registry, the
failure policies, and `ctx.peers` all behave identically. `scancel` is
replaced by sending `SIGTERM` to the process group.

This lets the cosmos-rl example run end-to-end on a laptop (with
scaled-down `count=` values) before you commit a 24-hour allocation on a
real cluster. See the [local-mode capacity validation](../CHANGELOG.md)
entry for what the SDK checks before launch.

## Where to look next

- The [parallel reference](../reference/parallel.md) lists every type and its
  full docstring.
- [Slurm Concepts](slurm_concepts.md) covers the underlying hetjob / step
  vocabulary if the terms above are new.
- [Rendering and Runner](rendering_and_runner.md) explains the batch-script
  generation pipeline that `parallel(...)` extends.
