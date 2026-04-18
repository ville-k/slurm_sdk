# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Local-mode parity for `parallel(...)` — run multi-peer allocations on a
  developer workstation without Slurm installed. `LocalBackend.submit_job`
  detects parallel-rendered scripts via a supervisor sentinel and, when
  `sbatch` is absent (or `SLURM_SDK_FORCE_LOCAL_PARALLEL=1`), invokes the
  Python supervisor directly via `subprocess.Popen`. Peers launch without
  `srun`; per-peer `SLURM_PROCID` / `SLURM_NTASKS` / `SLURM_JOB_ID` are
  synthesized so `JobContext` stays coherent. Shutdown uses process-group
  `SIGTERM` / `SIGKILL` with the same grace window semantics as Slurm mode
- Local-host capacity validation — when the backend is `local` and Slurm
  is not present, `parallel(...)` checks aggregate per-peer CPU, memory
  (`/proc/meminfo`, `psutil`, `sysctl` on macOS) and GPU
  (`CUDA_VISIBLE_DEVICES` / `nvidia-smi -L`) demand against the host
  before rendering and reports "local host has N CPUs, parallel() requires
  M — scale down or use a real cluster." via `TopologyError`
- `slurm.examples.parallel_simple` — smoke example demonstrating a
  leader + sidecar `parallel(...)` running entirely locally
- `ParallelJob.snapshot()` returns a `dict[str, JobSnapshot | list[JobSnapshot]]`
  — one `JobSnapshot` per singleton peer, one list of snapshots per replica
  peer (ordered by replica index). Forwards `tail_lines=` to every per-peer
  `Job.snapshot()` so remote backends only transfer the tail. The returned
  shape mirrors `get_results()` so callers can zip results and snapshots by
  key
- `ParallelJob.leader_result` property — shorthand for the leader+helpers
  idiom. Returns the unique `leader=True` peer's deserialised result.
  Raises `RuntimeError` with actionable messages when there are zero or
  multiple leaders so callers never get a silent wrong-peer pick
- `ParallelJob.after(*deps)` — accepts one or more upstream `Job` /
  `ArrayJob` / `ParallelJob` handles. Because `parallel(...)` submits
  eagerly, `.after(...)` cancels the original allocation and re-submits
  with `#SBATCH --dependency=afterok:<id>[:<id>...]` propagated to every
  hetjob component. Call `.after(...)` promptly after `parallel(...)` so
  the cancel is a no-op in practice
- `parallel(..., after=...)` kwarg — direct pre-submission dependency
  wiring that skips the cancel/resubmit round-trip. Accepts a single Job /
  ArrayJob / ParallelJob or a list thereof; renders `#SBATCH --dependency=afterok:...` onto every hetjob component

### Changed

- `ParallelJob` now emits exactly one `SubmitEndContext` and one
  `CompletedContext` per allocation (previously one per peer). Only the
  representative peer's Job carries `on_completed`; non-representative
  peer Jobs no longer fire duplicate lifecycle events when their
  `get_status()` is called directly. The single shared Slurm job id
  makes per-peer completion events redundant — use
  `ParallelJob.peer_outcomes()` for per-peer detail

- Heterogeneous per-peer packaging for `parallel(...)` allocations — each
  peer can carry its own `@task(packaging=...)` declaration, so a single
  allocation can mix container images, wheels, and bare-node (`"none"`)
  steps without restriction. Peers with equivalent packaging configs share
  one `prepare()` call (dedup keyed on the resolved config, ignoring
  volatile auto-tags) so a container image is pulled / probed exactly once
  per unique reference. Each peer's `srun` is wrapped with its own
  strategy's container directives — a `"none"` peer emits a bare srun even
  when its neighbours are containerised. `packaging="inherit"` on a peer
  now clones the leader's packaging (or the first peer's, when there is no
  leader) at submission time, so an inheriting peer is indistinguishable
  from a same-image peer after resolution. `packaging="inherit"` on the
  first peer of a leader-less topology is rejected at validation time
  (nothing to inherit from)

- Port reservation at the `@task` decorator — tasks can now declare
  `@task(ports={"rpc": "auto", "metrics": 50051})`. Fixed integer ports pass
  through verbatim; `"auto"` entries trigger an ephemeral-socket
  bind-and-release dance inside the runner before user code starts, and the
  resolved port numbers are written into the peer's registry entry so
  sibling peers can discover them via
  `ctx.peers["<name>"].first.ports["<label>"]`. Peers also get
  `ctx.my_ports` (read-only mapping of the peer's own resolved ports) and
  `ctx.reserve_port(name)` for mid-function reservations

- `ctx.shared_dir` — parallel allocations now expose a shared directory
  at `$JOB_DIR/shared/` created by the bootstrap before any peer runs. The
  supervisor exports `SLURM_SDK_SHARED_DIR` into each peer's environment so
  every peer's `ctx.shared_dir` resolves to the same path. `None` outside a
  parallel allocation so ordinary task runs are unaffected

- `ctx.shutdown_requested` — a thread-safe flag driven by a process-wide
  `threading.Event` that the runner flips when it receives SIGTERM. Peers
  with long-running main loops can poll it to exit cleanly within the
  supervisor's grace window. Peer Popens now run under
  `start_new_session=True` so SIGTERM propagates through the peer's process
  group to any tools the peer launched (tensorboard, node-exporter, ...)

- `SlurmTask.with_sidecars(*sidecars, grace_period_seconds=10)` — sugar for
  the common leader+sidecars shape. Returns a `BoundLeaderBundle`; calling
  the bundle with the leader's args desugars to the full `parallel(...)`
  call with `Peer(leader=True)` on the leader and
  `Peer(on_failure="continue")` defaults on each sidecar. Accepts
  `SlurmTask`, `BoundTask`, or `Peer` sidecars; an explicit user-provided
  `Peer(..., on_failure="kill")` keeps its policy. Rejects `topology=` to
  steer callers to the explicit `parallel(...)` form when pools are needed

- Named-node placement for parallel allocations — peers can now pin to
  specific hosts within their pool via `Peer(on_node="<label>")` or
  `Peer(on_node=<ordinal>)`, and replica sets can pin per-replica via
  `Peer.replicas(on_nodes=[...])`. Pools accept a `node_labels` tuple that
  maps logical names (e.g. `"head"`, `"ops"`) to the pool's nodes in Slurm
  allocation order. `Peer(colocate_with="<other>")` inherits the target
  peer's pin (auto-pinning the target to the first unused node if it has no
  explicit pin) so co-located services land on the same host from a single
  declarative flag

- `ctx.nodes` and `ctx.node` runtime discovery — user code can iterate every
  node in the allocation (`ctx.nodes.in_pool("gpu")`, `ctx.nodes["head"]`,
  `ctx.nodes.by_hostname(...)`) and introspect the host it is running on
  (`ctx.node`) through the new `NodeGroup` / `NodeInfo` types, matching the
  `ctx.peers` / `PeerInfo` discovery surface from Phase 7

- Bootstrap now resolves placement intent to concrete hostnames before the
  supervisor launches any step. Pinned peers receive `--nodelist=<host>`
  automatically; unpinned peers remain free for Slurm to place. The plan
  records each peer's resolved nodelist so restart and cascade paths honour
  the same pinning

- Peer service-discovery runtime API — peer functions can now inspect every
  other peer in the allocation via `ctx.peers["<name>"]`, a read-only mapping
  of `PeerGroup` views. Each `PeerGroup` exposes replicas, their hostnames,
  declared ports, announced metadata, and lifecycle state; iteration, indexing,
  `first`, `hostnames`, `ready_only()`, `refresh()`, and `wait_all(keys=..., timeout=...)` are available. A blocking `wait_all()` polls the registry with
  exponential backoff (50 ms → 1 s cap) and raises `TimeoutError` with a list
  of laggard replicas on expiry

- `ctx.announce(ready=True, **fields)` publishes runtime metadata to the peer
  registry via an atomic tmp-and-rename write. Announced fields merge into the
  current replica's `metadata`; `ready=True` flips the replica's `state` to
  `"ready"`. Reserved registry keys (`name`, `hostname`, `step_id`, `ports`,
  `state`, etc.) are rejected; the reserved set is shared with the existing
  static `Peer.announce=` validator so mistakes surface the same way in both
  paths

- `PeerInfo` and `PeerGroup` are now exported at the top level
  (`from slurm import PeerInfo, PeerGroup`) as the typed view surface over
  peer registry entries

- The supervisor now exports `SLURM_SDK_REGISTRY_PATH` to each peer it
  launches so `JobContext.peers` / `JobContext.announce()` can find the
  registry without any extra wiring in user code

- Multi-pool `parallel(...)` submissions now render as Slurm heterogeneous
  jobs (`hetjob`). Each pool declared in a `Topology` compiles to its own
  `#SBATCH` header block separated by `#SBATCH hetjob` dividers; every peer's
  `srun` step gets `--het-group=<component_index>` so it lands in the right
  component. One `sbatch` call submits the whole allocation

- The allocation bootstrap resolves per-component nodelists via
  `SLURM_JOB_NODELIST_HET_GROUP_<N>` and seeds each peer's registry entry
  with hostnames from its own pool

- The supervisor now cancels every hetjob component on shutdown — `scancel`
  receives `<jobid>`, `<jobid>+1`, `<jobid>+2`, ... in one call so Slurm tears
  the whole allocation down atomically

- Validation flags peers pinned to GPU-less pools when they declare
  `gpus_per_task > 0`, pointing callers at the pools that actually have GPUs

- `PlanComponent` metadata is serialised into `plan.json` so the supervisor
  and bootstrap can drive per-component scancel / nodelist resolution without
  reconstructing the mapping from `peers`

- Replica sets via `Peer.replicas(count=N, args=...)` — one Slurm step runs
  `--ntasks=N` tasks; each replica picks its per-index args from a pickle
  selected by `SLURM_PROCID`. `args=` accepts `None`, a length-`count` list,
  a `range`, or a `Callable[[int], dict|tuple|scalar]` evaluated eagerly at
  submission time

- Runner `--step peer:<name>:by-taskid` dispatch reads `SLURM_PROCID` and
  loads the matching `peer_<name>_<i>_args.pkl` file

- `JobContext.replica_index` / `JobContext.replica_count` are populated for
  tasks running inside a replica peer; `None` for singleton peers

- `ReplicaGroup` handle — `job["<replica_peer>"]` returns a `ReplicaGroup`
  with `__len__`, `__iter__`, `__getitem__(i)`, `wait()`, and `get_results()`
  that collects results in replica-index order

- `ParallelJob["<name>", i]` tuple indexing returns the individual
  per-replica `Job`

- `ParallelJob.get_results()` returns a `list` of length `count` for replica
  peers (mixed with scalar values for singleton peers)

- `ParallelJob.peer_outcomes()` flattens replica peers to `"<name>[<i>]"`
  keys, matching how `PeerFailureError` names replica failures

- `on_failure="restart"` policy with `max_restarts` budget — the supervisor
  re-launches a failed peer (keeping name / args / placement) until it
  succeeds or the budget is exhausted. Exhausted restarts fall through to
  the `"kill"` path and trigger cascading shutdown

- `on_failure="callback"` policy — users supply a top-level function resolved
  by `module:qualname` at supervisor startup. On failure the supervisor
  invokes the callback with a `JobSnapshot` and dispatches on its return
  value (`"kill"` or `"continue"`). Lambdas and nested functions are rejected
  at spec-validation time

- `ParallelJob.peer_outcomes()` returns a `{peer_name: PeerOutcome}` dict
  describing exactly what happened to each peer: `success`, `restarted`,
  `continue_on_failure`, `fatal`, `shutdown_by_leader`, or `not_started`

- `PeerOutcome` frozen dataclass exposing `status`, `exit_code`,
  `restart_count`, and a diagnostic `message`

- `ParallelJob.get_results()` now raises `CompositeJobError` aggregating
  every fatal peer's `PeerFailureError` when any peer's outcome is `"fatal"`
  or `"not_started"`

- `parallel(...)` entry point now submits single-pool allocations end-to-end.
  Each peer runs as its own `srun` step inside one `sbatch` job, with
  per-peer result files accessible via `job["<peer_name>"].get_result()`
  and aggregate access via `job.get_results()`

- `ParallelJob` result type with `__getitem__`, `wait()`, and `get_results()`
  for retrieving per-peer outputs from `parallel(...)` submissions

- `JobContext.peer_name` / `JobContext.peer_pool` fields are populated inside
  peer steps so user code can identify which peer it is running as

- Runner `--step peer:<name>` dispatch flag for tasks launched inside a
  `parallel(...)` allocation

- Python supervisor (`slurm.parallel.topology_supervisor`) owns the lifecycle
  of a `parallel(...)` allocation — launches each peer's `srun`, applies
  `on_failure="kill"` / `"continue"` policies, propagates leader exits to
  siblings with a configurable grace window before hard-kill

- Bootstrap (`slurm.parallel.topology_bootstrap`) resolves hostnames and
  writes a `registry.json` skeleton at allocation start for downstream
  service-discovery APIs (expanded in later phases)

### Fixed

- `loads_pickled()` now raises a descriptive `ValueError` when the pickle
  payload is truncated after the header prefix, instead of a bare `IndexError`
- `rendering.serialize_task_arguments()` replaces `assert` guards with explicit
  `RuntimeError`, ensuring correctness under `python -O`

### Changed

- `BackendBase.job_base_dir` is now a documented attribute of the base class;
  internal call sites access it directly instead of probing with `getattr`
- Eliminated the circular import between `slurm.context` and `slurm.cluster`;
  `Cluster` now exposes a self-referential `cluster` property so context
  resolution works structurally without runtime imports
- Internal callers no longer probe backends with `hasattr`/`getattr` for
  `is_remote`, `tail_file`, or `hostname` — these are part of the documented
  `BackendBase` contract and are called directly
- Documented Slurm exit-code format (`"exit:signal"`) on `JobSnapshot.exit_code`
- Clarified `WorkflowTask` docstring to note it is the return type of
  `@workflow` and should not be instantiated directly

## [0.4.6] - 2026-04-11

### Fixed

- `Job.get_status()` now falls back to `sacct` accounting data when the job has
  left the Slurm scheduler queue, instead of raising `BackendError`
- `ArrayJob` no longer calls `packaging_strategy.prepare()` twice during
  submission; the duplicate call could cause redundant builds/uploads
- Prebuilt dependency images are now cleared at the start of each workflow
  submission, preventing stale images from leaking between sequential workflows
  on the same `Cluster` instance

### Changed

- Workflow Slurmfile generation uses `tomlkit` for typed TOML serialization
  instead of manual string formatting; booleans, lists, and numeric values
  now round-trip correctly
- Packaging resolution logic extracted into `_packaging_resolver` module as the
  single source of truth for config precedence across task submission, workflow
  Slurmfile generation, and dependency container prebuilds
- Introduced `WorkflowTask(SlurmTask)` subclass; `@workflow` now returns a
  `WorkflowTask` instance instead of setting a `_is_workflow` flag. Workflow
  detection throughout the SDK uses `isinstance` instead of attribute checks.
- `Cluster.submit()` now returns `_SubmittableTask` or `SubmittableWorkflow`
  instead of a raw closure, providing a consistent callable interface
- Decorator `TYPE_CHECKING` overloads now return `SlurmTask`/`WorkflowTask`
  directly, giving type checkers accurate attribute access (`.map()`, `.after()`,
  `.with_options()`)
- `Cluster.from_backend()` now initializes all core attributes (`env_name`,
  `slurmfile_path`, `packaging_defaults`, `_prebuilt_dependency_images`, etc.)
  symmetrically with `__init__()`, eliminating manual patching in tests
- Replaced `getattr(cluster, ...)` defensive access with direct attribute access
  throughout internal modules
- Standardized `Slurmfile.toml` as the preferred configuration filename; legacy
  variants (`Slurmfile`, `slurmfile`, `slurmfile.toml`) emit `DeprecationWarning`
- Extracted shared Slurm command output parsers (`scontrol`, `sacct`, `squeue`,
  `sinfo`) into `api/_parsing.py`, eliminating ~350 lines of duplication between
  SSH and local backends
- Decomposed `render_job_script()` (420 lines) into 8 focused helper functions
  with `render_job_script()` as a thin orchestrator
- Added `upload_file()` to `BackendBase` as a public abstract method; array job
  submission now uses it instead of reaching into private `_upload_file`
- Consolidated SSH reconnect-and-retry logic into `_with_reconnect_retry()` helper,
  simplifying `execute_command()` and `download_file()` wrappers
- Split `callbacks/callbacks.py` (1453 lines) into 6 focused modules:
  `contexts.py`, `base.py`, `logger.py`, `rich_logger.py`, `benchmark.py`,
  and `_metrics.py` (shared workflow metrics I/O deduplicated from
  LoggerCallback and BenchmarkCallback)
- Deleted dead `run_task_with_callbacks()` from runner (exported but never called)
- Renamed `runner/argument_loader.py` to `runner/initialization.py` to reflect
  its actual scope (logging config, sys.path restoration, callback loading)
- Decoupled `SubmissionError` from Rich: moved `rich.console` and `rich.syntax`
  imports from module level into the methods that use them
- Standardized all callback imports to use `slurm.callbacks` package path
  instead of `slurm.callbacks.callbacks` internal module
- Promoted 8 ty type checker rules from `warn` to `error` after fixing all violations;
  only ParamSpec-related rules remain as warnings pending upstream ty support

### Deprecated

- `Cluster.from_file()` — use `Cluster.from_env()` with a `Slurmfile.toml` instead
- Slurmfile variants `Slurmfile`, `slurmfile`, `slurmfile.toml` — rename to
  `Slurmfile.toml`

### Removed

- `SlurmTask.task` property (returned self)
- `SlurmTask.dependencies` property (use `pending_dependencies`)
- `SlurmTaskWithDependencies` alias (use `SlurmTask` directly)
- `SlurmTask(**slurm_options)` parameter (was unused/reserved)
- `BackendBase` now declares abstract `read_file()` and `execute_command()` methods
- `BenchmarkCallback` helper methods moved from module-level monkey-patching to proper
  class methods for better type safety and pickling
- Dev releases now use timestamped versions (`X.Y.Z.devYYYYMMDDHHMM`) instead of
  static `.dev0`, allowing multiple dev releases per day without version conflicts

### Fixed

- Fixed ~90 type annotation issues across the codebase, improving type safety for
  `Cluster`, `SlurmTask`, SSH backend, runner argument loading, and callback classes
- Fixed lowercase `any` (builtin) used instead of `Any` (typing) in runner module
- Fixed missing `TYPE_CHECKING` imports for `SSHCommandBackend` and `Cluster` in CLI modules
- Fixed missing imports in parallel train-eval workflow tutorial
- Added `JobContext` to API reference documentation
- Updated import paths in tutorials and how-to guides to use public API
  (`from slurm.callbacks import ...`) instead of internal modules
- Removed unused imports from workflow graph visualization tutorial
- Fixed `_convert_to_enroot_format` to handle domain-name registries (nvcr.io,
  ghcr.io) without explicit ports, not just `registry:PORT` format
- Rendered job scripts now export `PYTHONUNBUFFERED=1` and use `srun --unbuffered`
  to ensure real-time stdout streaming with `job.tail()`
- `Job.snapshot()` derives `is_terminal`/`is_successful` from a single status
  query to avoid inconsistency when the status cache expires mid-call
- `Job.snapshot()` uses `tail_file(follow=False)` for efficient log tails instead
  of downloading entire stdout/stderr files on remote backends

### Added

- `parse_packaging_config()` as a public API for parsing packaging specification
  strings into configuration dicts; previously the private `_parse_packaging_config()`
- `PackagingConfig` TypedDict in `slurm.packaging` documenting all valid packaging
  configuration keys
- `Job.snapshot()` method returning a frozen `JobSnapshot` dataclass with current
  state, output tails, elapsed time, and terminal/success flags
- `Job.tail()` method for live log streaming with configurable `output` parameter
  accepting any writable IO object (`sys.stdout`, `io.StringIO`, file objects)
- `BackendBase.tail_file()` method with implementations for SSH and local backends
- `slurm jobs tail <job-id>` CLI command with `--stderr`, `--no-follow`, and
  `--lines` options
- Container image digest resolution via registry HTTP API; resolved digest is
  recorded as a comment in the job script for provenance and debugging
- Usage examples in docstrings for `SlurmTask.__call__()`, `ArrayJob.get_results()`,
  `WorkflowContext`, and `JobContext`
- `llms.txt` file with complete API recipes, decision tree, and method signatures
  for AI coding agent consumption
- How-to guide for creating custom task and workflow decorators using existing
  `@task`, `@workflow`, and `with_options()` APIs
- `write_file()` and `close()` methods on `BackendBase` interface for unified
  file operations and explicit resource cleanup
- Pickle version headers for cross-version mismatch detection; result files now
  include Python version and SDK version metadata, with clear warnings on mismatch
- SSH lazy reconnection on transport errors (automatic retry once) and explicit
  `cluster.reconnect()` for long-lived sessions (e.g. Jupyter notebooks)
- `reconnect()` method on `BackendBase` interface (no-op for local backend)

### Changed

- Pre-existing container images no longer require `docker pull` for digest
  resolution when the registry API is accessible; digest is recorded in the
  job script as a comment (`use_digest` defaults to `False` because enroot
  < 3.5 does not support `@sha256:` in image URIs)
- Expanded container packaging explanation with details on multi-word Python
  executables, container mounts, working directory, and array job naming
- Restructured GPU, container dependency, and parallelization how-to guides
  with proper problem statements, prerequisites, steps, and verification
  sections following Diataxis how-to guide format
- Input validation for `account` and `partition` sbatch options is now enforced
  at submission time
- Removed redundant SBATCH option normalization in `render_job_script()`
- Merged `SlurmTaskWithDependencies` into `SlurmTask`; `.after()` now returns a
  `SlurmTask` with bound dependencies. The `SlurmTaskWithDependencies` name is
  kept as an alias for backward compatibility
- Extracted `_resolve_cluster()` helper in context module, eliminating duplicated
  context resolution logic across task submission methods
- Consolidated packaging config resolution into `resolve_packaging_config()` with
  documented precedence; eliminates duplicated logic between submission and
  workflow dependency building
- Replaced 12 positional parameters on `render_job_script()` with structured
  `RenderContext` dataclass
- Removed submission pipeline wrapper methods from `Cluster`; internal modules
  now call extracted functions directly
- `Job` now depends on `BackendBase` interface instead of `Cluster`; accepts
  `backend` and `on_completed` keyword arguments. The `cluster` parameter is
  kept for backward compatibility
- `BackendBase` now provides `download_file()` (default: local copy) and
  `hostname` class attribute (default: `"localhost"`)
- Decomposed `cluster.py` into private modules (`_polling`, `_submission`,
  `_workflow`) for maintainability; public API unchanged
- Extracted private methods from `ContainerPackagingStrategy.prepare()` for
  improved testability
- Callback exceptions are now logged at WARNING level with full tracebacks
  (previously logged at DEBUG)
- SSH backend `host_key_policy` default changed from `"warn"` to `"reject"` for
  improved security; pass `host_key_policy="warn"` to restore previous behavior
- Extracted `_dispatch_callbacks()` helper on `Cluster` to deduplicate callback
  dispatch logic
- Consolidated `_runner_impl.py` into the `runner/` package; a thin
  backwards-compatible shim remains for external references
- Slurmfile TOML modification now uses `tomlkit` for proper round-trip parsing
  instead of fragile line-by-line string manipulation

### Removed

- Removed unused `_runner_impl.py` backward-compatibility shim

### Fixed

- Corrected callback method names in callbacks and events explanation; expanded
  from stub to comprehensive coverage of all 11 hooks, execution loci, and
  serialization behavior
- Moved `base64` import to module level in rendering to prevent potential
  `NameError`
- Temp file leak in `Job.get_result()` when downloading results via SSH; files
  are now cleaned up in a `finally` block
- Thread-safety issue with `Job` status cache; reads and writes to
  `_status_cache`, `_status_cache_time`, and `_completed` are now protected
  by an `RLock`
- Race condition in `_job_pollers` dict access between main and poller threads
- Job name validation and quoting in rendered sbatch scripts
- Replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)`
- Environment metadata files are now written with `0o600` permissions
- `LocalBackend.execute_command()` no longer uses `shell=True`

### Dependencies

- Added `tomlkit>=0.12` as a required dependency

## [0.4.5] - 2026-02-05

### Added

- Interactive TUI commands (requires `pip install slurm-sdk[tui]`):
  - `slurm dash` - Interactive dashboard for monitoring jobs and cluster status
    - Two-pane layout with navigation tree and detail panel
    - Displays user's jobs, account jobs, and partition status
    - Hybrid refresh: auto-refresh when focused, toggleable with 'a' key
    - Job cancellation support with keyboard shortcut
  - `slurm docs` - Documentation viewer with full-text search
    - Browsable navigation tree following mkdocs.yml structure
    - Markdown rendering with syntax highlighting
    - SQLite FTS5-powered full-text search with prefix matching and context snippets
    - Keyboard navigation: `/` to search, arrow keys and Enter for results
    - Documentation bundled with package for offline access
- Container-aware job connection: `slurm jobs connect` now automatically attaches to the container running inside a job when using `container` packaging:
  - Containers are named `slurm-sdk-{pre_submission_id}` at submission time (with `_{task_id}` suffix for array jobs)
  - Connect command detects container jobs and uses `--container-name` flag to attach
  - New `--no-container` flag to bypass container attachment and connect to bare node
  - Multi-node job support with interactive node selection prompt
  - Array job support: each array task gets a unique container name to prevent collisions
- `slurm jobs cancel` command to cancel running or pending jobs:
  - Shows job name and state before cancelling
  - Prompts for confirmation (use `--force` to skip)
  - Handles terminal states gracefully
- `slurm` CLI command for job and cluster management:
  - `slurm jobs list` - view jobs in the SLURM queue with color-coded states
  - `slurm jobs show <job-id>` - display detailed job information
  - `slurm jobs watch` - live dashboard for monitoring jobs in real-time
  - `slurm jobs connect <job-id>` - attach interactive shell to running jobs
  - `slurm jobs debug <job-id>` - attach debugger to running jobs with SSH port forwarding
  - `slurm cluster list` - list configured environments from Slurmfile (offline)
  - `slurm cluster show` - view cluster partition information
  - `slurm cluster watch` - live dashboard for monitoring cluster partition utilization
  - `slurm cluster connect` - open SSH session to cluster login node
  - Rich output formatting with tables and panels
  - User-friendly error handling with actionable hints
- MCP (Model Context Protocol) server for AI assistant integration:
  - `slurm mcp run` - start MCP server exposing SLURM SDK APIs
  - `slurm mcp status` - display MCP server configuration
  - Tools: `list_jobs`, `get_job`, `cancel_job`, `list_partitions`, `list_environments`
  - Resources: `slurm://queue`, `slurm://jobs/{id}`, `slurm://partitions`, `slurm://config`
  - Supports stdio (Claude Desktop) and HTTP transports
- `DebugCallback` for enabling debugpy debugging in SLURM jobs:
  - Configurable via Slurmfile or environment variables (`DEBUGPY_PORT`, `DEBUGPY_WAIT`)
  - Automatic debugpy setup when jobs start on compute nodes
  - Integration with `slurm jobs debug` CLI command
- SSH host key verification with configurable policies (`auto`, `warn`, `reject`)
- Modular runner architecture with dedicated modules for argument loading, callbacks,
  context injection, placeholder resolution, result saving, and workflow building
- Input validation module (`slurm.validation`) for job names, accounts, and partitions
- Security documentation explaining the SDK's trust model and best practices
- How-to guide for hardening SSH connections in production
- Bandit security scanning as dev dependency and CI workflow
- GitHub Actions CI workflow for running unit tests on PRs and main branch pushes
- Integration tests in CI with Docker-based Slurm cluster, gated by unit tests, lint, and security checks
- Basic monitoring APIs for job status tracking
- Mermaid diagrams throughout documentation for improved understanding:
  - Parallelization pattern diagrams (fan-out/fan-in, pipeline, sweep, dynamic dependencies)
  - Workflow execution sequence diagram and directory structure
  - System architecture and component relationship diagrams
  - Callback timeline and execution loci diagram
  - Job state machine diagram in docstrings
  - Task API flow diagram in docstrings
  - Two-phase submission pattern diagram in docstrings

### Changed

- Refactored `slurm.runner` from monolithic module into focused package with 7 modules

- Removed legacy underscore-prefixed function exports from `slurm.runner` module:

  - `_run_callbacks` → `run_callbacks`
  - `_function_wants_workflow_context` → `function_wants_workflow_context`
  - `_bind_workflow_context` → `bind_workflow_context`
  - `_write_environment_metadata` → `write_environment_metadata`

- Integration test registry port changed from 5000 to 20002 to avoid conflicts with
  common services (e.g., macOS AirPlay Receiver)

- Local backend now uses `shell=False` for SLURM commands (more secure)

- Default SSH host key policy changed from `auto` to `warn` (logs warning for unknown hosts)

- Job script permissions now default to `0o750` (configurable via `script_permissions`)

- SSH passwords are cleared from memory immediately after successful authentication

- Documentation restructured to follow Diataxis framework with four distinct types:

  - Renamed `docs/guides/` to `docs/how-to/` for consistency
  - Moved architecture content from `docs/reference/architecture/` to `docs/explanation/`
  - Updated navigation in `mkdocs.yml` to reflect new structure

### Security

- Fixed missing `shlex.quote()` calls in SSH backend path handling
- Added security-focused Bandit `# nosec` comments with justifications throughout codebase
- Improved temporary directory handling to use secure paths

### Fixed

- Fixed potential issue with `update_job_metadata` when job ID is None (now defaults to "unknown")
- Replaced Linux-only `flock` with cross-platform `mkdir`-based locking in wheel
  packaging for macOS compatibility
- `Cluster.get_job()` now correctly extracts `pre_submission_id` from stdout path for jobs with timestamp-based IDs (e.g., `20260118_090851_0f492fb`)
- Type annotations added to public APIs to resolve mkdocstrings warnings:
  - `Cluster.from_file()`, `Cluster.add_argparse_args()`, `Cluster.from_args()`, `Cluster.submit()`
  - `task()` and `workflow()` decorator return types
  - `SlurmTask.unwrapped`, `.map()`, `.after()`, `.with_options()`, `.with_dependencies()`

## [0.4.4] - 2026-01-10

### Added

- Workflow support with `@workflow` decorator for multi-step job orchestration
- Monitoring APIs for tracking job status and progress
- Container packaging integration with Pyxis/enroot
- Native SLURM array jobs support for efficient batch processing
- Signal handlers for RichLoggerCallback
- Environment inheritance for workflow child tasks
- Local backend for testing without SLURM access
- Python 3.9 support

### Changed

- Improved container packaging reproducibility
- API simplification and cleanup
- Enhanced error messaging with actionable resolution steps

### Fixed

- Container integration issues with workflow execution
