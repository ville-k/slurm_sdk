# Fable Plan — Re-adding Deferred `parallel(...)` Features

> **Status:** forward-looking roadmap.
> **Context:** The first `parallel(...)` release ships the **single-pool core** only
> (see "What shipped" below). This document records the features that were
> *deliberately removed* before that release and sketches how they come back in
> later cycles.
>
> **These are not specifications.** The original implementations existed and worked;
> they were cut to keep the first release small and reviewable. When each feature
> returns, treat its previous shape as a *reference point, not a contract*. The
> designs below may change — and should, where a cleaner approach emerges. What
> must hold is **forward-compatibility of the shipped API**: code written against
> the single-pool core keeps working unchanged as these layers are added back.

---

## What shipped (the single-pool core)

The released `parallel(...)` does exactly one thing, completely:

- **One allocation, N peer steps.** Every peer is a `@task`-decorated function that
  runs as an `srun` step inside a single Slurm allocation (one `#SBATCH` header, no
  hetjob components).
- **Leader + sidecars.** `Peer(..., leader=True)` triggers cascading graceful
  shutdown of the rest when the leader exits; `grace_period_seconds` governs the
  SIGTERM→SIGKILL window. `job.leader_result` returns the leader's value.
- **Failure policies `kill` and `continue`.** Fatal-by-default; `continue` tolerates
  a peer's failure and surfaces it through `peer_outcomes()` / partial results.
- **Replica sets.** `Peer.replicas(task, count=N, args=...)` — multi-task steps with
  per-replica argument generation.
- **Runtime service discovery.** `ctx.peers` (PeerGroup/PeerInfo), `ctx.announce(...)`,
  `group.wait_all(...)`, and `ctx.shared_dir`, all backed by an atomic JSON registry.
- **Local-mode parity.** The same supervisor drives subprocess steps on a
  workstation so the whole surface is testable without a cluster.
- **`with_sidecars(...)` sugar** over the leader+sidecar pattern.

This is the load-bearing capability: *one co-scheduled allocation whose peers can
find and talk to each other.* Everything below is layered placement, lifecycle, and
heterogeneity power that sits **on top** of this core without changing it.

---

## Forward-compatibility contract

Every re-added feature must preserve these invariants so the shipped API never
breaks:

1. **`topology=` stays optional.** The default (no `topology=`) remains a single
   implicit pool sized from peer claims. Multi-pool is purely additive.
2. **Single-pool rendering is the `component_index=None` path.** Hetjob rendering
   must remain a strict superset — the single-component script the core emits today
   must be byte-stable when no multi-pool topology is supplied.
3. **`on_failure` is an open enum.** `kill` / `continue` keep their exact semantics;
   `restart` / `callback` are additions, never redefinitions.
4. **`JobContext` only grows.** `ctx.peers`, `ctx.announce`, `ctx.shared_dir`,
   `ctx.shutdown_requested` are stable. `ctx.nodes` / `ctx.node` / `ctx.my_ports`
   return empty/None today and become populated later — reading them must never
   raise.
5. **The registry schema is append-only.** New fields (node labels, ports, restart
   counts) are optional and ignored by older readers.
6. **`Peer` / `Pool` constructors accept-and-ignore future kwargs gracefully** where
   feasible, or add them as no-ops first, so user code can adopt them before the
   runtime honors them.

A good litmus test for each phase: *the cosmos-rl example from the design docs
should become expressible without rewriting any peer function bodies.*

---

## Removed features and their return path

Ordered by the recommended re-introduction sequence. Each was previously
implemented; LOC figures are the *prior* footprint, given only to convey relative
weight.

### Phase F1 — Lifecycle robustness (`restart`, `callback`)

**What was removed**
- `on_failure="restart"` + `max_restarts` (supervisor relaunch loop, restart-count
  tracking in the registry). ~150 src LOC.
- `on_failure="callback"` + `FailureCallback` (dynamic `module:qualname` resolution,
  snapshot construction, per-failure decision). ~100 src LOC.

**Why it's first**
Smallest, most self-contained, and the highest operational value for long jobs.
Both are localized to the supervisor's policy dispatch plus a couple of registry
fields and `Peer` validation — no rendering or placement entanglement.

**Return design (open)**
- Keep the policy dispatch as the single extension point: `kill` / `continue` /
  `restart` / `callback` all resolve through one function that returns
  `(action, payload)`. Re-adding is re-populating two branches.
- **Reconsider `callback`'s shape.** Dynamic import of a user function into the
  supervisor process was fragile (module availability, lambda rejection). Worth
  evaluating alternatives: a small declarative policy DSL (`restart_if_exit_in=[137]`),
  or running the callback inside the peer's own runner rather than the supervisor.
- Restart in **local mode** required intricate sibling relaunch for replica sets;
  if replica restart proves low-value, ship Slurm-mode restart first (Slurm re-runs
  the step atomically) and defer local replica restart.

**Forward-compat:** `on_failure` already accepts only `kill`/`continue`; adding the
two literals plus `max_restarts`/`callback` kwargs is non-breaking.

---

### Phase F2 — Port auto-reservation

**What was removed**
- `@task(ports={"rpc": "auto"})`, `ctx.my_ports`, `ctx.reserve_port(...)` —
  ephemeral-socket binding in the runner, port publication into the registry,
  base64 plan round-trip. ~280 src LOC.

**Why it's second**
Independent of placement and lifecycle; touches the runner init path and the
registry's per-peer `ports` map only.

**Return design (open)**
- The "bind ephemeral socket, record number, close before user code" trick has an
  inherent race. Most production users pass fixed ports + `SO_REUSEADDR`. Consider
  shipping **fixed-port declaration + discovery** first (`ports={"rpc": 50051}`
  published to the registry, no auto-bind) and treating `"auto"` as a later, clearly
  best-effort addition.
- Keep `ctx.my_ports` present-but-empty in the core (invariant #4) so user code that
  reads it doesn't branch on availability.

**Forward-compat:** registry `ports` field already exists in the schema (append-only);
`my_ports` returns an empty mapping until populated.

---

### Phase F3 — Named-node placement (`node_labels`, `on_node`, `on_nodes`) + `ctx.nodes`

**What was removed**
- `Pool(node_labels=[...])`, `Peer(on_node=...)`, `Peer.replicas(on_nodes=[...])`
  (label/ordinal validation + bootstrap hostname pairing + `--nodelist` injection).
  ~175 src LOC.
- `ctx.nodes` / `ctx.node` (NodeGroup / NodeInfo). ~450 src LOC — node discovery
  rides naturally with placement, since labels are only meaningful once nodes are
  addressable.

**Why it's third**
Placement is only interesting once a pool spans multiple nodes or you need to pin
within it. It depends on the bootstrap resolving `SLURM_JOB_NODELIST_HET_GROUP_<n>`
to hostnames — useful even single-pool (multi-node) but most valuable with F4.

**Return design (open)**
- `node_labels` are sugar over ordinals; ordinals worked without labels. If labels
  prove rarely used, ordinal-only (`on_node=0`, `ctx.nodes[0]`) is a smaller first
  step.
- `ctx.node` resolution (which host am I on) had a precedence chain
  (step nodelist → `HOSTNAME` → `socket.gethostname`). Re-validate that chain
  against real multi-node steps before relying on it.

**Forward-compat:** `ctx.nodes` returns an empty NodeGroup and `ctx.node` an empty
NodeInfo in the core (invariant #4). Adding population is non-breaking.

---

### Phase F4 — Heterogeneous multi-pool topologies (hetjob)

**What was removed**
- `Topology` with >1 `Pool`, per-component `#SBATCH ... hetjob` rendering,
  `--het-group=<n>` step routing, per-component bootstrap hostname resolution.
  ~310 src LOC, scattered across `rendering.py`, `plan.py`, `topology_bootstrap.py`.

**Why it's last**
Largest footprint and the most cross-cutting. It only pays off in combination with
F3 (placement) and F1 (per-peer lifecycle), so it lands once those stabilize.
Single-pool already covers "many peers, one node shape."

**Return design (open)**
- The shipped single-pool path is exactly `component_index=None`; multi-pool is the
  `int` case. Re-adding is restoring the component-index plumbing — but revisit
  whether `Topology`/`Pool` is the right surface or whether per-peer pool specs read
  better.
- Per-component cancellation (`scancel $JOB_ID`, `$JOB_ID+1`, …) and per-component
  `sacct` consolidation are the operational gotchas (Appendix B of the design doc);
  re-validate against the target Slurm version.

**Forward-compat:** `topology=None` stays the default and keeps emitting the
single-component script unchanged (invariant #2).

---

### Phase F5 — `colocate_with` (optional)

**What was removed**
- `Peer(colocate_with=...)` — colocation dependency graph, DFS cycle detection,
  first-free-node assignment. ~160 src LOC.

**Why optional / last**
The design docs themselves debated whether pools alone express co-location
("pool-shared = co-located"). Same-pool placement already covers the common case.
`colocate_with` only adds value for "this peer must land on the *same specific node*
as that peer" within a multi-node pool — a narrow need.

**Return design (open)**
- Consider whether this is needed at all once F3/F4 exist. If reintroduced, the
  cycle-detection + free-list machinery was the densest code in the whole subsystem;
  a simpler model (e.g. explicit shared `on_node` labels) may obviate it.

**Forward-compat:** purely additive `Peer` kwarg; absent today, no user impact.

---

## Sequencing summary

| Phase | Feature | Prior src LOC | Depends on | Notes |
| ----- | ------- | ------------: | ---------- | ----- |
| F1 | `restart` + `callback` | ~250 | core | Reconsider callback delivery mechanism |
| F2 | port reservation | ~280 | core | Ship fixed-port discovery before `"auto"` |
| F3 | named-node placement + `ctx.nodes` | ~625 | core (bootstrap) | Ordinal-first; labels as sugar |
| F4 | multi-pool hetjob | ~310 | F3 | Largest blast radius; revisit `Topology` surface |
| F5 | `colocate_with` | ~160 | F3, F4 | May be unnecessary; smaller alternative likely |

Each phase is independently shippable and independently reversible. None requires
rewriting peer function bodies written against the core. The guiding rule: **the
simple `parallel(a, b)` call must read and run identically across every phase.**
