# Container Startup Optimization for Slurm SDK, Pyxis, and Enroot

Snapshot date: 2026-04-25.

This note investigates how Slurm SDK can reduce the time spent building,
pushing, pulling, importing, creating, and starting containers when jobs run
through Pyxis and Enroot. The main design goal is to keep container overhead out
of the GPU-critical path, measure the remaining overhead at SDK level, and expose
good defaults without forcing users to understand every site-specific Enroot
setting.

## Executive Summary

Container startup time matters for ML iteration because several expensive phases
can happen before user code starts:

1. Client-side image build.
1. Registry push.
1. Compute-node registry pull.
1. Enroot import or load.
1. Pyxis container create/start.
1. Python runner startup.

The SDK already has one important property: Docker or Podman build and push
happen before job submission, not after the Slurm allocation starts. That avoids
burning allocated GPU time for local image builds. The remaining high-impact
problem is compute-node container materialization, because Pyxis/Enroot work
happens inside the Slurm step and can consume allocated GPU wall time before
training starts.

The biggest SDK-level wins are:

- Prefer prebuilt, immutable image references for day-to-day iteration.
- Stop generating random image tags for unchanged build contexts; use stable
  content-derived tags or explicit user tags.
- Add SDK timings for build, push, submit, allocation-to-runner, and
  container-startup phases.
- Add a typed Pyxis/Enroot policy instead of relying only on free-form
  `srun_args`.
- Support stable, hash-derived container names when reuse is desired.
- Add a compute-node diagnostics command that checks Pyxis, Enroot, cache paths,
  user namespaces, OverlayFS support, and whether the cluster is configured for
  `enroot load`.
- Add an optional warmup step for repeated development runs and heterogeneous
  jobs, with metrics that distinguish cold cache from warm cache behavior.

## Corrections to the Collected Guidance

The collected guidance points in the right direction, but the exact control
surface should be aligned with current Pyxis and Enroot behavior.

- `ENROOT_NATIVE_OVERLAYFS` is now documented upstream. Current Enroot command
  docs say `enroot load` requires `ENROOT_NATIVE_OVERLAYFS=yes`, and the current
  config template lists it as the default for `enroot load` and direct SquashFS
  startup.
- The current fast path to target is Pyxis `use_enroot_load=1` plus Enroot 4.0+.
  Pyxis documents `use_enroot_load` as the setting that chooses single-step
  `enroot load` instead of `enroot import` plus `enroot create`.
- Pyxis 0.21.0 is the useful minimum for this feature path, because that release
  added `enroot load` support, importer plugins, Pyxis environment-variable
  configuration, and logging for time spent importing and creating containers.
  Pyxis 0.22.0 adds more direct SquashFS startup support.
- I did not find a public Pyxis `--container-cache-path` user flag in current
  Pyxis usage docs. Cache and data paths are primarily Enroot configuration
  (`ENROOT_CACHE_PATH`, `ENROOT_DATA_PATH`) and Pyxis plugstack/admin
  configuration. The SDK should not design around a per-job `srun` cache-path
  flag unless it is verified on a specific cluster.
- `--container-name` is a real user-level lever. Pyxis documents that if a named
  container already exists, the existing container is used and image import is
  skipped. The SDK currently generates per-job names, which is good for
  `slurm jobs connect` but intentionally prevents cross-job reuse.

## Current SDK Behavior

The relevant implementation is `src/slurm/packaging/container.py`.

Current strengths:

- A prebuilt image with `push=False` takes a fast path that avoids Docker/Podman
  runtime detection and local image operations.
- Image build and push happen on the submitting side during packaging
  preparation, before the Slurm script is submitted.
- The SDK wraps execution in `srun --container-image=...` and supports
  `packaging_srun_args` as an escape hatch for site-specific Pyxis flags.
- Job directory mounts and container workdir are handled centrally.
- Heterogeneous parallel jobs deduplicate equivalent packaging configs within
  one allocation.

Current limitations:

- The default tag is `build-{uuid}`. That gives uniqueness, but it defeats
  cross-submission reuse unless the user manually provides a stable tag.
- `push=True` is the default. For explicit prebuilt image workflows, users must
  remember to set `push=False` to avoid unnecessary runtime work.
- The default container name is `slurm-sdk-{job_id}` with array suffixes. That
  is useful for attach/debug, but it prevents Pyxis named-container reuse across
  repeated development runs.
- There is no first-class concept of container startup policy:
  read-only/writable, stable name, cache preference, warmup, or load support are
  all implicit or passed through stringly `srun_args`.
- The SDK does not currently measure container startup overhead as a separate
  phase. It can report packaging callbacks on the client, but not compute-node
  Pyxis/Enroot time in a structured way.
- SDK docs mention automatic image caching by hashing code and Dockerfiles, but
  the current code path uses random tags unless the user supplies one. The docs
  and implementation should be brought back into alignment.

## Optimization Model

The SDK should treat container work as four separate cost centers:

1. Build cost: Docker/Podman build time on the submitter.
1. Distribution cost: push from submitter, pull from compute nodes, registry
   latency, and layer reuse.
1. Materialization cost: Enroot import/load/create and Pyxis named-container
   lookup.
1. Runtime entry cost: `srun`, Pyxis startup, shell setup, Python runner import,
   and user function dispatch.

Different mitigations apply to each phase. For example, a better Dockerfile
layer order helps build and registry distribution, while Enroot 4 `load` helps
compute-node materialization. A stable `--container-name` helps repeated starts
only when the existing node-local container state is still valid and visible.

## SDK Design Recommendations

### 1. Add Container Startup Metrics

Add structured timing at both the client and compute-node layers.

Client-side metrics:

- `container.prepare.resolve_image_seconds`
- `container.prepare.build_seconds`
- `container.prepare.push_seconds`
- `container.prepare.digest_seconds`
- `container.prepare.total_seconds`
- `submission.sbatch_seconds`

Compute-node metrics:

- `slurm.allocated_at`
- `container.srun_started_at`
- `container.runner_entered_at`
- `container.user_code_started_at`
- `container.total_startup_seconds`
- `container.pyxis_import_seconds` when Pyxis logs expose it
- `container.pyxis_create_seconds` when Pyxis logs expose it
- `container.cache_state`, one of `unknown`, `cold`, `warm_name`,
  `warm_layer_cache`, or `warm_sqsh`

Implementation sketch:

- Have the generated batch script write JSONL records to
  `$JOB_DIR/container_metrics.jsonl`.
- Record a timestamp immediately before each SDK-generated containerized `srun`.
- Have the Python runner write another timestamp as the first action after
  import/config extraction, before calling user code.
- Parse Pyxis logs opportunistically for import/create timings. Pyxis 0.21.0
  added explicit logging for import/create time, but the SDK should gracefully
  fall back to coarse timestamps when logs differ by site version.
- For heterogeneous jobs, include `peer_name`, `replica_index`, `node`,
  `image_reference`, and `container_name_policy` in each metric record.

This measurement should ship before aggressive optimization. Otherwise users
cannot tell whether they are paying for registry pulls, Enroot materialization,
Python startup, or actual application initialization.

### 2. Replace Random Tags With Stable Build Keys

Random tags are useful for immutability, but they make cache reuse poor. The SDK
should support a deterministic tag strategy:

```python
packaging_tag_strategy="content_hash"
```

The build key should include:

- Dockerfile bytes.
- Build context file digests, respecting `.dockerignore`.
- Build args that affect the image.
- Platform.
- Base image reference as written by the user, and optionally the resolved base
  digest when available.
- SDK package version when SDK code is copied into the image.

Recommended modes:

- `tag_strategy="explicit"`: user-provided tag is used as-is.
- `tag_strategy="content_hash"`: SDK computes `sdk-{short_hash}`.
- `tag_strategy="uuid"`: current behavior, retained for forced clean builds.

This change would make the current documentation claim about hashing true again
and would greatly improve repeated development runs where the build context did
not change.

### 3. Expose Typed Pyxis Options

`srun_args` should remain as an escape hatch, but common Pyxis controls should be
first-class:

```python
packaging_pyxis={
    "readonly": True,
    "mount_home": False,
    "remap_root": False,
    "entrypoint": False,
    "env": ["NCCL_DEBUG", "HF_HOME"],
}
```

Typed options should render to:

- `--container-readonly` or `--container-writable`
- `--container-mount-home` or `--no-container-mount-home`
- `--container-remap-root` or `--no-container-remap-root`
- `--container-entrypoint` or `--no-container-entrypoint`
- `--container-env=...`

Defaulting to read-only is important because fast `enroot load` paths and
immutable container reuse are easier to reason about than writable rootfs state.
For ML jobs, writable locations should normally be explicit mounts for
checkpoints, datasets, model caches, and experiment logs.

### 4. Add Stable Container Name Policies

The SDK currently uses a job-specific name:

```text
slurm-sdk-{job_id}${SLURM_ARRAY_TASK_ID:+_$SLURM_ARRAY_TASK_ID}
```

That should remain the default for attach/debug correctness. Add an opt-in reuse
policy:

```python
packaging_container_name_policy="stable"
```

Stable names should be derived from:

- user identity;
- image digest or stable image reference;
- mounts;
- workdir;
- read-only/writable mode;
- entrypoint/remap-root/mount-home settings;
- SDK runner ABI version.

Example rendered name:

```text
slurm-sdk-u1001-img7f4a2c-mnt19c0-ro
```

Risks and guardrails:

- A stable name with an incomplete hash can silently reuse stale filesystem
  state.
- Writable containers should either be excluded from stable reuse or get a
  separate stateful name policy.
- The SDK should include a cleanup command or documented cleanup flow because
  `container_scope=global` can leave named containers behind after the job.
- `slurm jobs connect` still needs a current-job attach name. If stable reuse is
  enabled, the attach name should be exposed in job metadata.

### 5. Add Container Warmup as a First-Class Operation

Pyxis documents a useful pattern: run a no-op command with
`--container-image` and `--container-name` to prepare a container before the real
application step. The SDK can encapsulate this as:

```python
cluster.warm_container(
    image="nvcr.io#nvidia/pytorch:25.10-py3",
    nodes="allocated",
    tasks_per_node=1,
    name_policy="stable",
)
```

For regular task submission:

```python
@task(
    packaging="container:nvcr.io#nvidia/pytorch:25.10-py3",
    packaging_warmup=True,
    packaging_container_name_policy="stable",
)
def train() -> None:
    ...
```

Warmup should support two modes:

- In-allocation warmup: simple and reliable because it targets the actual nodes,
  but it still consumes allocation wall time.
- Pre-allocation or prior-job warmup: can avoid GPU allocation waste, but it is
  only useful when the later job lands on the same nodes or the cache is on a
  shared filesystem.

For heterogeneous jobs, the SDK can warm only the node groups that need each
image. For the new heterogeneous job support, this is a strong showcase: a
policy worker image, rollout image, and evaluation image can each be warmed once
per relevant node group instead of repeatedly per peer.

### 6. Add Compute-Node Diagnostics

Add a command or API similar to:

```python
cluster.diagnose_container_runtime()
```

It should run on compute nodes, not only on the login node. Suggested probes:

```shell
enroot version
enroot config
srun --help | grep -A40 container
scontrol show plugin
cat /proc/sys/user/max_user_namespaces
grep overlay /proc/filesystems
stat -f -c %T "${ENROOT_CACHE_PATH:-$HOME/.cache/enroot}"
stat -f -c %T "${ENROOT_DATA_PATH:-$HOME/.local/share/enroot}"
df -h "${ENROOT_CACHE_PATH:-$HOME/.cache/enroot}"
df -h "${ENROOT_DATA_PATH:-$HOME/.local/share/enroot}"
```

The diagnostic report should classify the site:

- `fast_load_ready`: Enroot 4+, native OverlayFS enabled, Pyxis
  `use_enroot_load=1`.
- `layer_cache_only`: Enroot cache paths configured, but Pyxis still uses
  import/create.
- `node_local_cache`: fast on repeated runs on the same node, cold on new nodes.
- `shared_cache`: better cross-node reuse, but filesystem throughput and
  metadata behavior must be checked.
- `unknown`: insufficient user-level visibility; include admin questions.

The SDK should emit actionable warnings:

- "The image is explicit and `push=True`; set `push=False` for prebuilt images."
- "The tag is random; use `content_hash` or an explicit tag for reuse."
- "The job uses `--container-writable`; this may disable fast immutable paths."
- "No stable container name is configured, so Pyxis will import/create on each
  job even if layers are cached."
- "Cache/data paths appear to be on a slow or quota-limited filesystem."

### 7. Model Admin-Level Capabilities Separately From User-Level Flags

Some optimizations are not safely controlled by user code:

- `ENROOT_CACHE_PATH`
- `ENROOT_DATA_PATH`
- `ENROOT_RUNTIME_PATH`
- `ENROOT_NATIVE_OVERLAYFS`
- Pyxis `runtime_path`
- Pyxis `container_scope`
- Pyxis `use_enroot_load`
- Pyxis `importer`

The SDK should expose these as diagnostics and recommendations, not silently set
them in job scripts. User-level overrides can be useful on permissive clusters,
but cluster admins often control Enroot/Pyxis paths for security, cleanup, quota,
and filesystem locality.

A good SDK API is:

```python
cluster.container_runtime_report()
cluster.recommend_container_settings()
```

The first reports observed state. The second prints suggested admin config and
user config separately.

### 8. Support Pyxis Importer Plugins as an Advanced Integration

Current Pyxis setup docs describe an `importer` plugstack argument that delegates
image importing to a custom script. This is not a user-level SDK feature, but it
is relevant for large clusters.

Possible SDK support:

- Document the importer path as an admin integration point.
- Generate a site example importer that maps known image digests to pre-staged
  SquashFS files or a local registry mirror.
- Make SDK diagnostics detect that an importer is configured when visible.
- Include importer timing in the metrics schema.

This is likely a second-phase feature because it requires admin deployment.

## Image Construction Best Practices to Encode

The SDK can nudge users toward image layouts that cache well:

- Put large, stable base layers first: CUDA, PyTorch, system packages.
- Put volatile training code and local project files in the last small layer.
- Use explicit base image tags or digests.
- Avoid `apt-get update` without pinning or cache strategy in inner-loop images.
- Keep model and dataset caches out of the image; mount them from fast storage.
- Prefer registry images close to the cluster, such as a site mirror or NGC
  mirror, over public internet pulls from compute nodes.
- Use BuildKit registry cache when available:

```shell
docker buildx build \
  --cache-from=type=registry,ref=REGISTRY/IMAGE:buildcache \
  --cache-to=type=registry,ref=REGISTRY/IMAGE:buildcache,mode=max \
  --tag REGISTRY/IMAGE:sdk-HASH \
  --push .
```

This should be optional because not all clusters allow buildx or remote cache
exports, but the SDK can generate the command or document the equivalent config.

## Proposed SDK API Sketch

The exact public API can follow the existing packaging kwargs style first, then
grow dataclasses later.

Minimal kwargs:

```python
cluster = Cluster(
    default_packaging="container",
    default_packaging_image="nvcr.io#nvidia/pytorch:25.10-py3",
    default_packaging_push=False,
    default_packaging_pyxis={
        "readonly": True,
        "mount_home": False,
    },
    default_packaging_tag_strategy="content_hash",
    default_packaging_container_name_policy="stable",
    default_packaging_warmup=True,
    default_packaging_measure_startup=True,
)
```

Richer future API:

```python
container = ContainerImage(
    image="nvcr.io#nvidia/pytorch:25.10-py3",
    push=False,
    startup=ContainerStartupPolicy(
        measure=True,
        warmup=True,
        read_only=True,
        name_policy="stable",
        prefer_enroot_load=True,
    ),
    pyxis=PyxisOptions(
        mount_home=False,
        remap_root=False,
        env=["NCCL_DEBUG", "HF_HOME"],
    ),
)
```

The first implementation should probably use kwargs to match the current SDK.
Dataclasses can be added once the shape stabilizes.

## Measurement Experiments

The SDK should support a small benchmark matrix that users can run on their own
cluster:

1. Prebuilt image, no stable name, cold node.
1. Prebuilt image, no stable name, warm layer cache.
1. Prebuilt image, stable name, same node.
1. Prebuilt image, stable name, different node.
1. Built image with random tag.
1. Built image with content-hash tag and unchanged context.
1. Writable container.
1. Read-only container.
1. `use_enroot_load=0`.
1. `use_enroot_load=1`.

Report fields:

- image reference;
- node list;
- Enroot version;
- Pyxis version if visible;
- cache/data/runtime path filesystem types;
- cold/warm classification;
- median/p95 startup time;
- GPU allocation delay before runner start;
- bytes pulled or imported when visible.

This can start as an example script rather than a benchmark callback system. The
important distinction is that it measures SDK and runtime phases, not only user
function wall time.

## Phased Implementation Plan

Phase 1: make existing behavior measurable.

- Add client-side packaging subphase timings.
- Add runner-entry and user-code-start timestamps.
- Write `container_metrics.jsonl`.
- Parse Pyxis import/create timings when present.
- Update docs so random tags are no longer described as automatic content-hash
  caching.

Phase 2: add low-risk user controls.

- Add typed Pyxis options for read-only, writable, mount-home, remap-root,
  entrypoint, and env preservation.
- Add `tag_strategy="content_hash"`.
- Add warnings for explicit images with `push=True`, random tags in iterative
  workflows, and writable containers when fast load is preferred.

Phase 3: add reuse and warmup.

- Add stable container name policy.
- Add warmup step rendering for regular and heterogeneous jobs.
- Include warmup metrics separately from real application step metrics.
- Add cleanup helpers for stable named containers.

Phase 4: add runtime diagnostics.

- Add compute-node diagnostics.
- Detect Enroot 4 load readiness, native OverlayFS support, user namespace
  settings, and cache/data path filesystems.
- Generate user-facing and admin-facing recommendations.

Phase 5: integrate advanced site features.

- Document Pyxis importer plugin integration.
- Add optional support for pre-staged SquashFS or registry mirror workflows.
- Support site profiles so SDK defaults can be tuned once per cluster.

## Risks and Tradeoffs

- Stable container names improve warm starts but can hide stale state if the hash
  omits a relevant input.
- Writable containers are convenient for debugging but make reuse less
  predictable and can bypass immutable fast paths.
- Shared caches reduce cross-node misses, but a shared filesystem can become the
  bottleneck if many jobs import or load large images concurrently.
- Node-local NVMe caches are often faster, but repeated jobs need the same nodes
  to benefit. Slurm placement may not provide that without explicit node
  requests.
- Pyxis and Enroot features vary across clusters. The SDK should probe and
  degrade instead of assuming every site has Enroot 4+, Pyxis 0.21+, or admin
  visibility into plugstack config.
- Measuring Pyxis internals from logs is version-sensitive. Coarse SDK
  timestamps are less precise but more portable.

## Immediate Recommendations

Users can reduce overhead today by doing the following:

- Use prebuilt images for inner-loop development:

```python
@task(packaging="container:nvcr.io#nvidia/pytorch:25.10-py3", packaging_push=False)
def train() -> None:
    ...
```

- Supply explicit stable tags for SDK-built images until content-hash tagging is
  implemented.
- Keep large dependencies in stable image layers and put frequently changing
  project files in late layers.
- Avoid writable root filesystems unless they are required.
- Put datasets, model caches, checkpoints, and logs on explicit mounts rather
  than inside the container rootfs.
- Ask cluster admins whether Pyxis is configured with Enroot 4+,
  `use_enroot_load=1`, native OverlayFS, fast `ENROOT_DATA_PATH`, and a sensible
  `ENROOT_CACHE_PATH`.
- Use current `packaging_srun_args` only for site-specific flags that the SDK
  does not yet model.

## References

- NVIDIA Enroot repository: <https://github.com/NVIDIA/enroot>
- Enroot `load` command documentation:
  <https://raw.githubusercontent.com/NVIDIA/enroot/main/doc/cmd/load.md>
- Enroot configuration template:
  <https://raw.githubusercontent.com/NVIDIA/enroot/main/conf/enroot.conf.in>
- NVIDIA Pyxis repository: <https://github.com/NVIDIA/pyxis>
- Pyxis setup documentation: <https://github.com/NVIDIA/pyxis/wiki/Setup>
- Pyxis usage documentation: <https://github.com/NVIDIA/pyxis/wiki/Usage>
- Pyxis releases: <https://github.com/NVIDIA/pyxis/releases>
