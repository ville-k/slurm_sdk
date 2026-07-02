# Follow-up: two-node Slurm test cluster + integration-test strengthening

## Why

The current `containers/slurm-test` setup is single-node (`slurm-control`
only). That's sufficient for a lot of integration coverage but forces
three concrete compromises in the parallel integration suite:

1. `test_two_pool_hetjob_header_accepted` can only assert sbatch header
   acceptance + scontrol registration, not actual hetjob scheduling —
   each component wants `--nodes=1`, the cluster has one node, both
   components block on `Reason=Resources`.
2. Service discovery, named-node placement, and `colocate_with`
   integration coverage all trivially pass on one node because every
   peer ends up on the same hostname. The SDK could regress cross-node
   behaviour without the integration suite noticing.
3. `Peer.replicas(count=N)` integration coverage can't prove per-peer
   hostname distribution — all replicas share one host.

A two-node test cluster closes all three gaps. None of this requires
macOS-specific changes; Docker Desktop's Linux VM handles multi-container
networking identically to Linux.

## Scope

### Containers

- `containers/docker-compose.yml`
  - Add a `slurm-worker` service (image: same `containers/slurm`).
  - Shared volumes with `slurm-test`:
    - munge key (`/etc/munge/munge.key`) — otherwise slurmctld rejects
      the worker's RPCs with authentication errors.
    - `/home/slurm` — production Slurm assumes shared-filesystem
      semantics; the SDK's wheel upload and job-dir scratch both
      depend on it.
    - `/etc/slurm` — same `slurm.conf` on both sides.
  - Same Docker network.
  - `depends_on: slurm-test` with a healthcheck so slurmctld is up
    before the worker starts registering.

- `containers/slurm/Dockerfile` — no image change needed.

- `containers/slurm/entrypoint.sh` (or equivalent)
  - Branch on `SLURM_ROLE` env var: `controller` (default) runs
    `slurmctld` + `slurmd`; `worker` runs `slurmd` only.
  - Worker waits for the controller's slurmctld to be reachable before
    `slurmd` tries to register.

- `containers/slurm/slurm.conf`
  - Add `NodeName=slurm-worker CPUs=4 State=UNKNOWN` (matching the
    controller's per-node shape).
  - Update `PartitionName=debug Nodes=slurm-control,slurm-worker
    Default=YES MaxTime=INFINITE State=UP`.
  - Keep `SlurmctldHost=slurm-control` — one controller is enough.

### Tests

- `tests/integration/conftest.py`
  - Add a `multi_node_cluster` fixture that `pytest.skip`s when
    `scontrol show nodes` reports < 2 nodes.
  - (Optional) `integration_test_multi_node` pytest marker for clarity
    in test collection.

- `tests/integration/test_parallel_end_to_end.py` — extend with the
  tests below. Each is ~30-60 min to write + debug against a running
  cluster.

  1. **`test_two_pool_hetjob_full_scheduling`** — replace the current
     header-acceptance-only form. Two pools, each `nodes=1`, both run
     to completion. Assert both peers' results land and their
     `hostname` values differ (proves each component allocated a
     distinct node).

  2. **`test_cross_node_service_discovery`** — coordinator pinned to
     one labelled node, worker pinned to the other, worker's observed
     `coord.hostname` differs from its own `ctx.node.hostname`. Proves
     `wait_all(["endpoint"])` crosses a real node boundary and the
     bootstrap's `SLURM_JOB_NODELIST_HET_GROUP_<n>` parsing produces
     distinct hostnames per component.

  3. **`test_named_nodes_pin_per_role`** — `Pool(node_labels=["head",
     "worker"])` + `Peer(on_node="head")` / `Peer(on_node="worker")`.
     Assert each peer lands on the expected hostname via the
     placement resolver's `scontrol show hostnames` path.

  4. **`test_colocate_with_shares_host`** — pin peer A to `head`, peer
     B colocates with A, third peer C floats. Assert `A.hostname ==
     B.hostname` and `C.hostname` may be either. Exercises the
     colocate resolver under real scheduling, not just the unit-test
     placement pure-function.

  5. **`test_srun_spans_two_nodes`** — single peer with
     `@task(nodes=2)`. Inside the task, read `$SLURM_JOB_NODELIST` and
     expand it — assert it contains both `slurm-control` and
     `slurm-worker`. Proves multi-node step dispatch and that the
     outer allocation correctly reserves > 1 node.

- Strengthen existing `test_replica_set_per_replica_results` —
  currently passes with all replicas on one host. Add an assertion
  `len({r["hostname"] for r in replicas}) == 2` when the multi-node
  fixture is active. Keep the existing single-node coverage as the
  fallback when the fixture skips.

## Effort breakdown

| Phase | Hours | Notes |
|---|---|---|
| compose + slurm.conf + entrypoint split | 2-3 | Mostly iterating munge / node registration edges |
| conftest multi-node fixture | 0.5 | |
| 5 new tests + 1 strengthened | 3-4 | Each test includes debug-in-real-Slurm time |
| CI runner sizing | 1 | See risk section |
| CHANGELOG + docs | 0.5 | |
| **Total** | **7-9 hours** | ~1 focused day |

## Risks & mitigations

### 1. CI runner resources

GitHub's default runners are 7 GB RAM / 2 CPU. Running `slurmctld` +
two `slurmd`s + a dev container + the test runner is feasible but
tight. Options:

- **Default tier, watch for OOM** (likely fine — slurmd is lean;
  estimated 300-500 MB total for the new worker container). Try this first.
- **Larger runner tier** (`ubuntu-latest-8-cores`) for the Integration
  Tests job only. Costs extra minutes-per-run but moves the ceiling.
- **Self-hosted runner for multi-node tests.** Pinpoint mitigation; the
  single-node subset still runs on default tier.

### 2. Shared filesystem assumption

Production Slurm clusters typically have `/home` on NFS or similar —
jobs submitted from the login node see the same filesystem as compute
nodes. Without a shared volume, each worker would need its own wheel
install + venv setup, which (a) wastes time on every job, and (b)
might expose a real SDK assumption we'd want to surface.

Simplest path: use a docker volume mounted at `/home/slurm` on both
containers. This matches the shared-FS production assumption without
needing an NFS server.

### 3. Slurm version quirks

Some multi-node slurm.conf fields (`SlurmdTimeout`,
`InactiveLimit`, node state transitions) have version-sensitive
defaults. Plan to allocate a chunk of the 2-3 hours above for
iterating if the worker shows up in `drained` or `down` states
initially. Running `slurmctld -Dvvv` + `slurmd -Dvvv` by hand in the
container diagnoses most issues in minutes.

### 4. Clock skew between containers

Both containers use the host's clock via Docker Desktop, so this is a
non-issue on Mac. Documented here because multi-node Slurm is
famously sensitive to clock drift on real clusters (non-mitigation —
just noting it's not a concern in this setup).

## What this does NOT buy us

- **Heterogeneous node types** (GPU pool vs CPU pool) — both
  containers are symmetric. Still needs a production-class cluster
  to test cross-partition scheduling where each pool lives on
  physically distinct hardware.
- **NVLink / network-topology-aware scheduling** — same story.
- **Real Pyxis container execution under srun** — can be tested in
  this setup but lands on whichever node; true multi-image-per-step
  coverage still wants a real cluster.

## When to do this

Not a blocker for landing the current 7-test integration suite.
Worth doing before the next major parallel feature (likely
fault-tolerance work, per-peer tracing, or GPU-topology handling),
because any of those would also benefit from stronger multi-node
CI coverage.

## Handoff pointer

Files to touch (in order of dependency):

1. `containers/slurm/entrypoint.sh` — role branching.
2. `containers/slurm/slurm.conf` — second node + partition.
3. `containers/docker-compose.yml` — add `slurm-worker` service.
4. `tests/integration/conftest.py` — multi-node fixture + marker.
5. `tests/integration/test_parallel_end_to_end.py` — 5 new tests,
   strengthen 1 existing.
6. `docs/CHANGELOG.md` — note the stronger multi-node coverage.
7. `.github/workflows/*.yml` — bump runner tier if needed.

Estimated PR size: ~300 lines of test code, ~40 lines of container
config, ~10 lines of conftest.
