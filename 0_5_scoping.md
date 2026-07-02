# Slurm SDK 0.5.0 Scoping

## Context

After the 0.4.x cycle of cleanups, refactors, and documentation improvements, the SDK's core submission loop is solid: `@task`, `.map()`, `.after()`, `@workflow`, container packaging, SSH backend, CLI/TUI tooling. The codebase is clean and the API is stable.

The 0.5.0 release should focus on the two biggest gaps for production adoption: **fault tolerance** and **observability during execution**.

---

## Priority 1: Live Log Streaming

### Problem

After submitting a job, users wait in the dark. The CLI has `slurm jobs connect` (interactive shell) and `slurm jobs watch` (status dashboard), but there's no way to see task output in real-time without SSH-ing in manually. This kills iteration speed during development.

### Proposed API

**Python API:**
```python
job = train(config=cfg)
job.tail()              # Block and stream stdout to terminal
job.tail(stderr=True)   # Stream stderr instead
job.tail(follow=False)  # Print current content and return (like tail without -f)
```

**CLI:**
```bash
slurm jobs tail <job-id>              # Stream stdout
slurm jobs tail <job-id> --stderr     # Stream stderr
slurm jobs tail <job-id> --no-follow  # Print and exit
```

### Design considerations

- **Implementation**: SSH backend reads the stdout file path (already known from `Job.stdout_path`) via `tail -f` over a persistent SSH channel. For local backend, use Python's file tailing.
- **Job not yet running**: If the job is PENDING, `tail` should wait (with a message) until output appears, then start streaming. Poll `squeue` until RUNNING, then start tailing.
- **Job already completed**: Print the full output file content and return.
- **Array jobs**: `array_job[i].tail()` tails the specific element. `slurm jobs tail <job-id>_<index>` from CLI.
- **Container jobs**: The stdout path is outside the container (Slurm captures it), so tailing works regardless of packaging strategy.
- **Interruption**: Ctrl+C should cleanly stop tailing without killing the job.

### Implementation sketch

1. Add `tail(follow=True, stderr=False, lines=50)` method to `Job`
2. SSH backend: execute `tail -f <path>` over a streaming SSH channel; yield lines
3. Local backend: use `subprocess.Popen(["tail", "-f", path])` or Python file polling
4. Add `BackendBase.tail_file(path, follow=True)` to the backend interface
5. Add `slurm jobs tail` CLI command in `src/slurm/cli/jobs.py`
6. Handle PENDING state with polling + "Waiting for job to start..." message

### Scope boundary

- No multiplexed streaming of multiple jobs simultaneously (future feature)
- No web-based log viewer (TUI is sufficient)
- No log search/filtering in 0.5.0

---

## Priority 2: Container Image Digest Pinning by Default

### Problem

The SDK supports `use_digest=True` to pin container images by SHA256 digest (`registry/image@sha256:abc...`) instead of mutable tags (`registry/image:latest`). This guarantees the image on the compute node is byte-identical to what was built during submission. However, the default is `use_digest=False` because the current implementation requires a full `docker pull` on the submission host to resolve the digest — which fails when Docker isn't available locally and adds minutes of latency for large images.

### Proposed change

Replace the `docker pull` + `docker inspect` digest resolution with a lightweight registry HTTP API call:

```python
def _get_image_digest_from_registry(self, image_ref: str) -> Optional[str]:
    """Resolve digest via registry API without pulling the image."""
    # HEAD https://registry/v2/<repo>/manifests/<tag>
    # Returns header: Docker-Content-Digest: sha256:abc123...
```

This is a single HTTP HEAD request that returns the digest without downloading any image layers. Once this is in place, change the default to `use_digest=True`.

### Design considerations

- **Registry authentication**: Registries require auth tokens. The SDK should support unauthenticated registries, Docker Hub token flow (`/v2/token`), and bearer tokens from `~/.docker/config.json`.
- **Fallback chain**: Try registry API first → fall back to `docker inspect` if available → fall back to tag reference with a warning.
- **Private registries with self-signed certs**: Respect the existing `packaging_tls_verify` option.
- **Built images**: When the SDK builds an image from a Dockerfile, the digest is already available from the build output or local inspect — no registry call needed.
- **Pushed images**: After `docker push`, the push output contains the digest. Parse it directly instead of making a separate call.

### Implementation sketch

1. Add `_get_image_digest_from_registry()` using `urllib` or `requests` to call the registry v2 manifest API
2. Parse `Docker-Content-Digest` header from the response
3. Handle auth: read `~/.docker/config.json` for credentials, implement Docker Hub token exchange
4. Update `_resolve_final_reference()` fallback chain: registry API → local inspect → tag with warning
5. Change default: `use_digest` defaults to `True`
6. For `docker push` output, parse the digest directly (avoids an extra call)

### Scope boundary

- Support Docker Hub, GitHub Container Registry, and generic v2 registries
- No support for v1 registries (obsolete)
- Auth from `~/.docker/config.json` only — no interactive login flow

---

## Priority 3: SDK Extensibility for Higher-Level Orchestrators

### Problem

The [Pipy managed workflows](../Pipy/pipy/managed_workflows) project builds a replayable orchestration service on top of this SDK. It currently depends on one private API (`slurm.decorators._parse_packaging_config`) and works around several missing public interfaces. Since managed workflows is the primary downstream consumer and the pattern of building higher-level orchestrators on top of the SDK will likely repeat, we should open up the necessary APIs cleanly.

### What to open up

#### 4a. Make `parse_packaging_config` public (trivial)

`_parse_packaging_config(packaging: str, kwargs: dict) -> Optional[dict]` in `decorators.py` is a pure function that parses packaging specification strings (e.g. `"container:registry/image:tag"`) into config dicts. There's no reason it's private.

**Change**: Rename to `parse_packaging_config`, export from `slurm` package.

**Downstream impact**: Managed workflows can drop its fallback parser at `slurm_sdk/api.py:239-249`.

#### 4b. Formalize the packaging config schema

The managed workflows project reverse-engineers the packaging config dict structure in multiple places (`_packaging_spec_from_config`, `PackagingSpec.to_slurm_config`), guessing at valid keys like `image`, `dockerfile`, `context`, `runtime`, `platform`, `push`, `tls_verify`, `mounts`, `srun_args`.

**Change**: Export a `PackagingConfig` TypedDict from `slurm.packaging` that documents all valid keys:

```python
class PackagingConfig(TypedDict, total=False):
    type: str                      # "auto", "wheel", "none", "container", "inherit"
    image: str                     # Container image reference
    dockerfile: str                # Path to Dockerfile
    context: str                   # Docker build context
    runtime: str                   # "docker" or "podman"
    platform: str                  # e.g. "linux/amd64"
    push: bool                     # Push image to registry
    tls_verify: bool               # Verify TLS for registry
    registry: str                  # Registry URL
    python_executable: str         # Python executable inside container
    mounts: List[str]              # Additional container mounts
    srun_args: List[str]           # Extra srun arguments
    use_digest: bool               # Pin by SHA256 digest
```

This makes the contract explicit instead of implicit.

#### 4c. Add `Job.snapshot()` for structured status + logs

The managed workflows Slurm backend (`slurm_backend.py:173-212`) assembles job status snapshots by combining `job.status()`, stdout tail, and stderr tail into a dict. This is a common need.

**Change**: Add `Job.snapshot()` that returns a structured object:

```python
@dataclass
class JobSnapshot:
    state: str                     # SLURM state (PENDING, RUNNING, COMPLETED, etc.)
    exit_code: Optional[int]
    stdout_tail: Optional[str]     # Last N lines of stdout
    stderr_tail: Optional[str]     # Last N lines of stderr
    reason: Optional[str]          # SLURM state reason
    is_terminal: bool              # Whether the job has finished

snapshot = job.snapshot(tail_lines=80)
```

This replaces ad-hoc status assembly in downstream projects and pairs naturally with the log streaming feature (Priority 2).

### What NOT to change

- **Don't add a `submit_raw()` API** — the two-phase submit pattern (`cluster.submit(task)` → `submitter(args)`) works fine for programmatic use. Managed workflows passes the `SlurmTask` object through metadata, which is slightly indirect but not worth a new API surface for.
- **Don't absorb managed workflow orchestration** — the replayable program model, suspend/resume, and service-side state management are correctly a separate concern.
- **Don't standardize the runner task pattern** — the three runner tasks (`ORCHESTRATOR_RUNNER_TASK`, etc.) are managed-workflows-specific.

### Implementation sketch

1. Rename `_parse_packaging_config` → `parse_packaging_config` in `decorators.py`
2. Add to `__init__.py` exports and `__all__`
3. Define `PackagingConfig` TypedDict in `slurm/packaging/__init__.py`, export it
4. Add `Job.snapshot(tail_lines=80)` method and `JobSnapshot` dataclass to `job.py`
5. Update `llms.txt` and API reference

### Scope boundary

- Only formalize APIs that have a known downstream consumer
- Don't preemptively design plugin/extension interfaces without concrete use cases

---

## Future (post-0.5.0)

These are valuable but larger in scope. Documenting them here so they don't get lost.

### Result caching

Skip re-execution when the same function is called with the same arguments and the cached result still exists.

```python
@task(time="01:00:00", cache=True)
def preprocess(data: str) -> dict:
    ...
```

Cache key = hash of (function source + serialized arguments + SDK version). Requires a cache storage backend (shared filesystem or object store) and invalidation strategy. High value for iterative ML workflows but significant design surface.

### Workflow resumption

Resume a failed workflow from the last successful step instead of re-running everything.

```python
job = workflow_func.resume(workflow_job_id="12345")
```

The `shared_dir` and `list_task_runs()` infrastructure is already in place. The missing piece is detecting completed steps and skipping them. Requires careful handling of partial results and side effects.

### Experiment tracking integration

Callbacks that log to MLflow, Weights & Biases, or TensorBoard.

```python
from slurm.callbacks import WandbCallback
cluster = Cluster.from_env("prod", callbacks=[WandbCallback(project="my-exp")])
```

Natural extension of the callback system. Low implementation effort, high user value for ML teams. Could ship as optional extras (`pip install slurm-sdk[wandb]`).

### Automatic retries (needs design rethink)

The current SDK architecture has a fundamental tension for retries:

- **`_JobStatusPoller`** runs as a daemon thread on the Cluster, dies when the client disconnects, and isn't explicitly stopped on `Cluster.__exit__()`. Retries here don't survive the `with Cluster(...):` block.
- **`Job.get_result()`/`Job.wait()`** polls inline, but retries only trigger if someone calls these methods. Fire-and-forget jobs never get retried.
- **Managed workflows** has a better architecture for this: the `ExecutionReconciler` service runs persistently, tracks `ExecutionAttempt` records, and already has `relaunch_after_loss` logic.

Open design questions before implementing:
- Should the SDK's retry be purely a convenience for scripts (accept the "must call get_result" limitation), or should it work for all jobs?
- If all jobs: the poller lifetime needs fixing first — pollers should be explicitly managed and survive beyond the current daemon-thread model.
- Should retries be implementable as a managed workflows extension on top of SDK primitives, rather than baked into the SDK core?
- Could a `RetryCallback` pattern work — a callback that observes `on_completed_ctx` and resubmits, keeping retry logic in userland rather than the SDK?

Future enhancements (once the core design is settled):
- Exponential backoff with jitter
- Retry budgets (max total retries across a workflow)
- Circuit breakers (stop retrying if N consecutive failures)
- Dead letter queue for permanently failed jobs

---

## What to skip

- **Web UI**: The TUI is sufficient and a web UI is a maintenance burden disproportionate to its value at this stage.
- **Multi-cluster federation**: Niche use case that adds massive complexity to the backend layer.
- **Kubernetes support**: Dilutes the "Slurm-native, no control plane" positioning that differentiates this SDK.
- **Custom DSL / YAML workflow definitions**: The Python decorator API is the right abstraction. Adding YAML would create a parallel definition surface to maintain.

---

## Implementation order

```
1. Extensibility  -- smallest scope, mostly renames and new TypedDict/dataclass
2. Digest pinning -- self-contained in container.py
3. Log streaming  -- builds on existing SSH backend
```

Extensibility first because it's trivial (rename a function, add a TypedDict, add a method) and immediately unblocks the managed workflows project. Digest pinning second because it's contained. Log streaming third. All three are independently shippable.

---

## Release criteria for 0.5.0

- [ ] `parse_packaging_config` is public and exported from `slurm`
- [ ] `PackagingConfig` TypedDict exported from `slurm.packaging`
- [ ] `job.snapshot()` returns structured `JobSnapshot` with state, logs, exit code
- [ ] `use_digest=True` is the default; digest resolved via registry API without pulling
- [ ] Digest resolution works for Docker Hub, GHCR, and generic v2 registries
- [ ] Graceful fallback to tag reference when digest resolution fails
- [ ] `job.tail()` streams output for SSH and local backends
- [ ] `slurm jobs tail <id>` CLI command works
- [ ] All new features covered by unit tests with local backend
- [ ] Documentation: how-to guides for log streaming and extensibility
- [ ] `llms.txt` updated with new recipes
- [ ] CHANGELOG entries for all new features
- [ ] Managed workflows project updated to drop private API usage
