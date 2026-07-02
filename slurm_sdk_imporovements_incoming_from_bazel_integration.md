# Slurm SDK Integration - Improvement Recommendations

Recommendations for improving the `slurm-sdk` (external pip package) and the `maglev.workflows` infrastructure to make the Bazel container integration smoother and more maintainable. These observations are grounded in the actual code in `maglev/workflows/launching/` and the `slurm-sdk` interfaces described in the design document.

---

## Improvements to `slurm-sdk` (External Package)

### 1. Decouple Image Building from the `ContainerPackagingStrategy`

The existing `ContainerPackagingStrategy` in `slurm-sdk` bundles image building (Docker/Podman), registry push, and `srun` command generation into a single class. Our `BazelContainerPackagingStrategy` only needs the latter two -- Bazel handles the build. But the base `PackagingStrategy` interface doesn't separate these concerns, so we have to implement a full strategy from scratch rather than composing pieces.

**Recommendation**: Split the container strategy into composable parts:

```python
class ContainerRegistry(ABC):
    @abstractmethod
    def push_image(self, local_ref: str, remote_ref: str) -> str: ...

    @abstractmethod
    def resolve_image_ref(self, image_ref: str) -> str: ...

class ImageBuilder(ABC):
    @abstractmethod
    def build_image(self, context: Path, tag: str) -> str: ...
```

A `BazelImageBuilder` would be a no-op (Bazel already built it), and a `BazelContainerRegistry` would delegate to `BazelImageHandler.push_image()`. This composability would let us avoid duplicating the Pyxis `srun` command generation that already exists in `ContainerPackagingStrategy`.

**Impact on our integration**: Without this, we must reimplement `wrap_execution_command()` from scratch in `BazelContainerPackagingStrategy`, duplicating the Pyxis flag logic that `ContainerPackagingStrategy` already has. If `slurm-sdk` changes its Pyxis integration, our strategy won't pick up the changes automatically.

---

### 2. Validate Packaging Config at Declaration Time, Not Execution Time

The `slurm-sdk` `@task` decorator accepts a `packaging` dict that is only validated when the task is actually submitted to SLURM. For the `bazel_container` strategy, this means a misconfigured `image_metadata_path` or missing `WORKFLOWS_CLI_IMAGE_METADATA` env var won't surface until the workflow is run, potentially after a lengthy Bazel build.

**Recommendation**: Add a config validation hook to `PackagingStrategy` that runs at strategy registration or task decoration time:

```python
class PackagingStrategy(ABC):
    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> None:
        """Validate config at decoration time. Raise on invalid config."""
        pass
```

Alternatively, use a typed config class (dataclass or Pydantic model) instead of a raw dict:

```python
@dataclass
class BazelContainerConfig:
    local_image: Optional[str] = None
    remote_registry: str = "nvcr.io/nvidia"
    push_images: bool = True
    mounts: List[str] = field(default_factory=list)
    workdir: str = "/app/execroot"
```

**Impact on our integration**: With the current design, we accept a raw `Dict[str, Any]` in the constructor and do all validation there. Users won't see errors until runtime. A typed config would provide IDE autocompletion and catch typos like `"remote_registy"` immediately.

---

### 3. Add Digest-Based Push Caching

Every invocation of a `slurm-sdk` workflow currently triggers an image push, even if the image hasn't changed. `BazelImageHandler` already has `--skip-unchanged-digest` support in its push script, but this still requires running the push script to check. There is no higher-level cache that says "this digest is already in the registry, skip entirely."

**Recommendation**: Add a digest cache to the `PackagingStrategy` interface:

```python
class PackagingStrategy(ABC):
    def is_prepared(self) -> bool:
        """Return True if prepare() can be skipped (e.g., image already pushed)."""
        return False
```

For our `BazelContainerPackagingStrategy`, this could check a local cache file mapping `(local_image, digest) -> remote_url` and skip the push if the entry exists and the remote is reachable.

**Impact on our integration**: Repeated `dazel run` invocations during development would be significantly faster. Currently each run pushes images even when nothing changed.

---

### 4. Support Per-Task Registry Configuration

The `slurm-sdk` currently assumes a single registry for all tasks in a workflow. In practice, teams may need different registries for different security domains (e.g., internal vs. external facing tasks) or for geographic proximity to different SLURM clusters.

**Recommendation**: Allow `remote_registry` to be specified per-task in the packaging config, rather than only at the workflow level.

**Impact on our integration**: The `BazelContainerPackagingStrategy` already accepts `remote_registry` per-task in its config dict. But if `slurm-sdk` overrides this at the workflow level, the per-task setting may be ignored. The SDK should respect per-task overrides.

---

### 5. Expose a Strategy Plugin Entry Point + Registry-Aware String Parsing

**Issue**: There are two coupled gaps that together prevent custom packaging strategies from being usable through the public string-form API:

1. There is no public registration function — third parties have to mutate the private `slurm.packaging._STRATEGIES` dict directly:

   ```python
   from slurm.packaging import _STRATEGIES
   _STRATEGIES["bazel_container"] = BazelContainerPackagingStrategy
   ```

   This relies on an implementation detail that could break on any version bump.

2. Even with the strategy registered, `slurm.decorators._parse_packaging_config()` only recognises a closed set of built-in names (`auto`, `wheel`, `none`, `inherit`, `container`) and the `container:image:tag` prefix. Any other string falls through to the raw-image branch and silently produces `{"type": "container", "image": "<the-name>"}`, which dispatches to the built-in `ContainerPackagingStrategy` with the strategy name itself as the image reference. So `@task(packaging="bazel_container")` doesn't reach our strategy at all.

The dict form (`packaging={"type": "bazel_container", ...}`) doesn't help either: `@task` has `packaging: str` in its signature, and `Cluster.from_args(default_packaging=...)` is a string. There is currently no way for a user to select a custom strategy through any documented API.

**Recommendation — Change 1**: Add a public `register_strategy` / `unregister_strategy` API to `slurm.packaging`:

```python
# slurm/packaging/__init__.py

from typing import Type

_BUILTIN_STRATEGY_NAMES = frozenset({
    "auto", "wheel", "none", "inherit", "container",
})


def register_strategy(
    name: str,
    strategy_class: Type[PackagingStrategy],
    *,
    override: bool = False,
) -> None:
    """Register a custom packaging strategy.

    Once registered, the strategy can be selected by its ``name`` anywhere
    a packaging string is accepted: the ``@task`` decorator, the
    ``Cluster`` constructor, ``Cluster.from_args``, and ``Slurmfile.toml``.
    """
    if not (isinstance(strategy_class, type)
            and issubclass(strategy_class, PackagingStrategy)):
        raise TypeError(
            f"strategy_class must be a PackagingStrategy subclass, got "
            f"{strategy_class!r}"
        )
    if name in _BUILTIN_STRATEGY_NAMES:
        raise ValueError(
            f"Cannot register strategy under reserved built-in name {name!r}"
        )
    if name in _STRATEGIES and not override:
        existing = _STRATEGIES[name]
        raise ValueError(
            f"Strategy {name!r} is already registered as "
            f"{existing.__module__}.{existing.__qualname__}. "
            f"Pass override=True to replace."
        )
    _STRATEGIES[name] = strategy_class


def unregister_strategy(name: str) -> None:
    """Remove a previously registered custom strategy."""
    if name in _BUILTIN_STRATEGY_NAMES:
        raise ValueError(f"Cannot unregister built-in strategy {name!r}")
    if name not in _STRATEGIES:
        raise ValueError(f"Strategy {name!r} is not registered")
    del _STRATEGIES[name]
```

`_STRATEGIES` stays where it is and keeps its underscore prefix — it's still an implementation detail; `register_strategy` / `unregister_strategy` are the supported surface. `unregister_strategy` exists so tests can clean up between cases without touching the private dict.

**Recommendation — Change 2**: Make `_parse_packaging_config()` consult the registry before falling back to the raw-image branch.

In `slurm/decorators.py`, replace the current final `else` branch:

```python
    else:
        # Assume it's a raw container image reference without "container:" prefix
        config["type"] = "container"
        config["image"] = packaging
```

with:

```python
    else:
        # Before treating the string as a raw container image reference,
        # check whether a custom strategy has been registered under this name.
        from .packaging import _STRATEGIES
        if packaging in _STRATEGIES:
            config["type"] = packaging
        else:
            # Legacy fallback: treat as a raw container image reference.
            # Users should prefer the explicit "container:..." form.
            config["type"] = "container"
            config["image"] = packaging
```

Behaviour table — the only strings whose interpretation changes are those that match a registered custom strategy name, which today silently produce a wrong `ContainerPackagingStrategy` with a garbage image and cannot have been working for anyone:

| `packaging` string                          | Before                                    | After                              |
|---------------------------------------------|-------------------------------------------|------------------------------------|
| `"auto"`, `"wheel"`, `"none"`, `"inherit"`, `"container"` | Built-in, unchanged                  | Built-in, unchanged                |
| `"container:foo:bar"`                       | `{type: container, image: foo:bar}`       | Unchanged                          |
| `"foo:bar"` (looks like image ref, not registered) | `{type: container, image: foo:bar}` | Unchanged — legacy fallback        |
| `"bazel_container"` (registered by 3rd party) | `{type: container, image: bazel_container}` (silently wrong) | `{type: bazel_container, ...}` (correct) |
| `"bazel_container"` (not registered)        | Same silent fallback                      | Same silent fallback — unchanged   |

`Cluster.default_packaging` is already stored verbatim and fed through `_parse_packaging_config` in `_packaging_resolver.resolve_packaging_config`, so Change 2 automatically enables `Cluster.from_args(args, default_packaging="bazel_container", default_packaging_mounts=[...])` and `Slurmfile.toml` `default_packaging = "bazel_container"` — no further changes required in `cluster.py` or `_packaging_resolver.py`.

**Backwards compatibility**: No existing API changes shape or behaviour; no existing string resolves to a different strategy. Private `_STRATEGIES` remains a dict mutable by existing internal code paths; only the recommended third-party surface changes. No bump to minimum Python version is required.

**Tests** to add (subject to project conventions):

1. `register_strategy` happy path: registers a toy strategy, then `parse_packaging_config("toy")` yields `{"type": "toy"}` and `get_packaging_strategy({"type": "toy"})` returns the toy class.
2. `register_strategy` rejects built-in names (`ValueError`).
3. `register_strategy` rejects duplicate registration without `override=True` (`ValueError`).
4. `register_strategy` rejects non-`PackagingStrategy` classes (`TypeError`).
5. `unregister_strategy` removes the entry; subsequent `parse_packaging_config("toy")` falls back to the raw-image branch.
6. Parser regression: strings that used to fall through (`"foo:bar"`, `"my-image"`) continue to produce `{"type": "container", "image": ...}` when no strategy of that name is registered.
7. `Cluster(default_packaging="toy", default_packaging_foo="bar")` resolves to `{"type": "toy", "foo": "bar"}` via the existing resolver.
8. `Slurmfile.toml` with `default_packaging = "toy"` resolves the same way.

**Alternative considered**: Python entry points (`[project.entry-points."slurm.packaging_strategies"]`). Cleaner for pip-installed plugins but not a fit for monorepo-Bazel-only consumers. The `register_strategy` API does not preclude adding entry-point discovery later — the entry-point loader can simply call `register_strategy`.

**Impact on our integration**: We currently maintain a local mirror of both pieces in `auto/slurm_packaging/_compat.py` §1 (`register_strategy` / `unregister_strategy` + a monkey-patch on `_parse_packaging_config`). When this lands upstream, that whole section deletes — `__init__.py` collapses to `from slurm.packaging import register_strategy; register_strategy("bazel_container", BazelContainerPackagingStrategy)`.

---

## Improvements to `maglev.workflows` (Internal)

### 6. `ImageHandler` Interface Is Larger Than What `slurm-sdk` Needs

`BazelImageHandler` implements the full `ImageHandler` ABC, which requires `build_image`, `load_image_locally`, `map_local_image_to_local`, `map_local_image_to_remote`, and `push_image`. The `BazelContainerPackagingStrategy` only needs two of these: resolving a local image to a remote URL (`map_local_image_to_remote`) and pushing it (`push_image`).

The strategy has to instantiate the full `BazelImageHandler` even though it only uses a subset of its capabilities. If `ImageHandler` gains new abstract methods in the future, `BazelImageHandler` would need to implement them even though they're irrelevant to the SLURM integration.

**Recommendation**: Factor out the resolution and publishing capabilities into smaller, focused interfaces:

```python
class ImageResolver(ABC):
    @abstractmethod
    def resolve(self, local_image: str, remote_registry: str) -> str: ...

class ImagePublisher(ABC):
    @abstractmethod
    def publish(self, local_image: str, remote_url: str, num_retries: int = 0) -> None: ...
```

`BazelImageHandler` would implement all of these plus `ImageHandler` for backward compatibility. `BazelContainerPackagingStrategy` would only depend on `ImageResolver` and `ImagePublisher`.

**Impact on our integration**: Cleaner dependency. The strategy would only depend on the interfaces it uses, and tests can mock smaller interfaces instead of the full `ImageHandler`.

---

### 7. `BazelImageHandler` Constructor Reads Files Eagerly

`BazelImageHandler.__init__` opens and reads the metadata JSON, then opens every digest file referenced in the metadata. This means constructing the handler can fail with `FileNotFoundError` if paths are stale. It also means tests must always create real files on disk just to instantiate the handler (see `test_bazel_image_handler.py` fixture setup).

**Recommendation**: Defer file reads to first use, or accept pre-parsed metadata as an alternative constructor:

```python
class BazelImageHandler(ImageHandler):
    @classmethod
    def from_metadata_dict(cls, metadata: Dict, basename: Optional[str] = None):
        """Construct from pre-parsed metadata (useful for testing and composition)."""
        ...
```

**Impact on our integration**: Tests for `BazelContainerPackagingStrategy` currently need to set up a full mock filesystem (metadata JSON, digest files, push scripts). A `from_metadata_dict` constructor would simplify test setup significantly.

---

### 8. The `--skip-unchanged-digest` Logic Is Hard to Follow

In `BazelImageHandler.push_image()`, the decision to append `--skip-unchanged-digest` depends on three conditions checked via a triple negation:

```python
if (
    (not len(self._override_remote_tag))
    and (not self._image_push_flag)
    and (
        not os.environ.get("IMAGE_PUSH_FLAG", "FALSE")
        in ["True", "TRUE", "true"]
    )
):
    args += ["--skip-unchanged-digest"]
```

The third condition has a subtle operator precedence issue: `not ... in [...]` binds as `not (... in [...])`, which happens to be correct here but reads as if it might be `(not ...) in [...]`.

**Recommendation**: Refactor to a property with a clear name:

```python
@property
def _should_skip_unchanged_digest(self) -> bool:
    if self._override_remote_tag:
        return False
    if self._image_push_flag:
        return False
    if os.environ.get("IMAGE_PUSH_FLAG", "").lower() == "true":
        return False
    return True
```

**Impact on our integration**: The `BazelContainerPackagingStrategy` calls `push_image()` on the handler, so it inherits this behavior. Making it clearer reduces the risk of accidental regressions when modifying push behavior.

---

### 9. Image Metadata Has No Version or Schema Information

The `bazel_docker_metadata` JSON format is a flat dict with no version field:

```json
{
  "bazel/pkg:task_image": {
    "digest_path": "...",
    "push_script_path": "...",
    "local_loader_path": "..."
  }
}
```

If the format needs to evolve (e.g., to include provenance data, registry hints, or multi-platform digests), there's no way to distinguish old from new format.

**Recommendation**: Add a version field and nest images under a key:

```json
{
  "version": "1.0",
  "images": {
    "bazel/pkg:task_image": {
      "digest_path": "...",
      "push_script_path": "...",
      "local_loader_path": "..."
    }
  }
}
```

`BazelImageHandler._read_image_metadata` would check the version and handle both formats during a transition period.

**Impact on our integration**: If we later want to add Bazel target or git commit provenance to the metadata (useful for debugging which image a SLURM job ran), we'll need versioning.

---

### 10. No Programmatic Way to Get Image Digest Without File I/O

`BazelImageHandler` reads the digest from a file on disk at construction time and stores it as a truncated 12-char hex string for use as a tag. There is no method to retrieve the full digest programmatically.

The `_local_tag_to_manifest_sha` dict stores the full SHA but it's a private attribute with no accessor.

**Recommendation**: Add a public method:

```python
def get_image_digest(self, local_image: str) -> str:
    """Return the full sha256 digest for a local image."""
    return self._local_tag_to_manifest_sha[local_image]
```

**Impact on our integration**: The `BazelContainerPackagingStrategy` could use the digest for cache keying (see recommendation 3) and for logging which exact image a SLURM job will run.

---

## Improvements to `slurm-sdk` Based on Bazel Container Patterns Observed in NDAS

The recommendations above were grounded in what `slurm-sdk` and
`maglev.workflows` did differently *during* the integration work.
This section captures structural advantages that the Bazel-based
container path (`auto/slurm_packaging` + the vendored
`BazelImageHandler` + the Bazel `bazel_docker_metadata` /
`container_image_py` rules) has over `slurm-sdk`'s built-in
`ContainerPackagingStrategy` — and how `slurm-sdk` could adopt them.

### 11. Default to Content-Addressed Image Tags

**Issue**: `ContainerPackagingStrategy` defaults to a UUID-suffixed
tag (`build-<uuid8>`) when no explicit tag is provided.  Every build
of identical content produces a different tag, which means:

- The registry accumulates many essentially-duplicate images.
- Two developers building the same code on different machines push
  to different tags — no cross-developer cache hits.
- Re-submitting an unchanged workflow re-pushes a "new" image even
  though the content is byte-identical.
- `--skip-unchanged-digest`-style optimisations have nothing to
  compare against because the tag itself changes.

By contrast, the Bazel pipeline derives the tag from the manifest
SHA: `<registry>/<name>:<sha[:12]>`.  Same content always yields the
same tag, which means re-pushes are no-ops, cross-developer caches
hit naturally, and the tag itself certifies content identity.

**Recommendation**: Make tag derivation pluggable on
`ContainerPackagingStrategy`, with two modes:

```python
class ContainerPackagingStrategy(PackagingStrategy):
    def __init__(self, config):
        ...
        # "uuid" (current default), "content_hash", or explicit value
        self.tag_strategy: str = config.get("tag_strategy", "uuid")
```

For `tag_strategy="content_hash"`:

- Compute a SHA over (Dockerfile bytes, sorted-by-path context file
  contents, build_args).  Use the first N hex chars as the tag.
- After the build, also resolve `RepoDigests[0]` (the same path the
  current `use_digest=True` mode takes) and *rename/re-tag* if the
  manifest SHA differs from the input-content hash, since BuildKit's
  optimisations can produce identical manifest digests for slightly
  different inputs.
- Either way: deterministic, content-addressed, idempotent.

**Effort**: Medium.  Mostly contained inside `_resolve_image_reference`
+ a helper to compute the hash.

**Impact on us**: Symmetry — `auto/slurm_packaging` and slurm-sdk's
own container strategy would both produce content-addressed tags,
and users could reason about either with the same model.  Also
makes recommendation 3 (push caching) trivial as a side effect:
push attempts where the local hash matches the registry's manifest
sha are obvious no-ops.

---

### 12. Skip-Unchanged-Digest Fast-Path on Push

**Issue**: `_push_container_image` in `slurm-sdk` always runs
`docker push` (or `podman push`) and lets the daemon do whatever
de-duplication it does, which is layer-level — uploading takes
*much* longer than just confirming "yes, this manifest is already
there."

The Bazel-emitted Go pusher has a `--skip-unchanged-digest` flag.
What it does is roughly:

1. Compute the local image's manifest SHA (already known from the
   build outputs).
2. `HEAD https://<registry>/v2/<repo>/manifests/<tag>` and read the
   `Docker-Content-Digest` response header.
3. If they match, exit 0 immediately.

This makes idempotent pushes effectively free.  In our `dazel run`
loops, repeated submits of an unchanged workflow finish the push
phase in well under a second.

**Recommendation**: Add a similar fast-path to
`ContainerPackagingStrategy._push_container_image`:

```python
def _push_container_image(self, runtime, image_ref, console=None):
    if self._registry_already_has(image_ref):
        logger.info("Skipping push (registry already has %s)", image_ref)
        return
    # ... existing push logic ...

def _registry_already_has(self, image_ref) -> bool:
    """HEAD the manifest endpoint; compare to local digest."""
    local_digest = self._get_image_digest(self.runtime, image_ref)
    if not local_digest:
        return False
    # Parse image_ref → registry, repo, tag
    # HEAD https://{registry}/v2/{repo}/manifests/{tag}
    # Compare Docker-Content-Digest to local_digest.
    ...
```

Implementation can shell out to `crane manifest digest <ref>` if a
Go-based tool is available; otherwise use `requests` directly with
the docker registry v2 protocol (a few hundred lines, well-known
format).

**Effort**: Medium.  Independent of recommendation 11 but composes
nicely with it.

**Impact on us**: Validates that what we get for free from the
Bazel-emitted pusher is actually a generic optimisation, not a
Bazel-specific quirk.  Users not on Bazel get the same fast iteration
loop.

---

### 13. Optional Daemonless Push Path

**Issue**: `ContainerPackagingStrategy._push_container_image` shells
out to `docker push` or `podman push`.  This requires:

- A docker (or podman) daemon running on the launcher machine.
- The daemon to have credentials configured for the target registry.
- The user to be in the `docker` group, or for podman to be in
  rootless mode.

In CI environments, on shared submit hosts, and increasingly on
workstations where Docker Desktop is no longer free, this is a
real friction point.

The Bazel pipeline pushes via a self-contained Go binary (rules_docker's
pusher) that talks the OCI registry HTTP API directly.  No daemon, no
group membership, no rootless gymnastics.  Credentials come from
`~/.docker/config.json` like any other OCI tool — the same file
docker / podman / crane / skopeo already read.

**Recommendation**: Offer a daemonless push backend in
`ContainerPackagingStrategy`, selected via config:

```python
ContainerPackagingStrategy(config={
    ...,
    "push_backend": "crane",   # or "docker" (default), "skopeo", "in_process"
})
```

- `push_backend="crane"`: shell out to `crane push <tarball> <ref>`.
  Crane is a small, single-binary OCI tool from Google's `go-containerregistry`;
  packaging it as a slurm-sdk dep is straightforward.
- `push_backend="in_process"`: implement the docker registry v2
  protocol directly in Python.  ~500 LoC, no new deps; tested
  ecosystem (pyoci, oci-python-sdk).

**Effort**: Medium.  The structural prep is recommendation 1
(decouple build from push); once that's done, swapping push backends
becomes a small change.

**Impact on us**: Confirms that the Bazel chain's no-daemon property
is reproducible outside Bazel — useful for users who want
slurm-sdk's flexibility without docker-daemon dependency.

---

### 14. Stable Env-Var Forwarding Contract for Container Strategies

**Issue**: `slurm-sdk`'s rendering pipeline `export`s several
`SLURM_SDK_*` env vars (`SLURM_SDK_PACKAGING_CONFIG`,
`SLURM_SDK_PREBUILT_IMAGES`, `SLURM_SDK_SLURMFILE`, `SLURM_SDK_ENV`)
into the SLURM step's host environment.  When the step runs `srun
--container-image=…`, Pyxis on *some* sites auto-forwards these env
vars into the container; on *other* sites it doesn't, and the
forwarding has to be requested explicitly via `--container-env=NAME`.

`slurm-sdk`'s built-in `ContainerPackagingStrategy.wrap_execution_command`
emits no `--container-env` flags at all.  On clusters where Pyxis
silently strips the env vars, the runner inside the container can't
find them, and behaviour quietly diverges:

- `Parent packaging type: none` (because `SLURM_SDK_PACKAGING_CONFIG`
  is missing, and the in-container fallback detection finds no
  `SLURM_CONTAINER_IMAGE` either).
- `cluster._prebuilt_dependency_images` is never populated, so child
  task submissions inside `@workflow` bodies fail to dispatch via
  the prebuilt-images path.

We hit both of these in `auto/slurm_packaging` and worked around them
by emitting `--container-env=NAME` for each required var.

**Recommendation**: Have `ContainerPackagingStrategy` (and any future
container strategies) take responsibility for forwarding the env
vars *they* depend on into the container, rather than relying on
unspecified Pyxis defaults.  Ship a documented list of `SLURM_SDK_*`
env vars that the runner consumes, and emit `--container-env=NAME`
for each:

```python
# slurm/packaging/container.py — wrap_execution_command
SLURM_SDK_RUNTIME_ENV = (
    "JOB_DIR",
    "SLURM_SDK_ENV",
    "SLURM_SDK_PACKAGING_CONFIG",
    "SLURM_SDK_PREBUILT_IMAGES",
    "SLURM_SDK_SLURMFILE",
)
for var in SLURM_SDK_RUNTIME_ENV:
    srun_parts.append(f"--container-env={var}")
```

A separate config knob (`packaging_forwarded_env`, list-of-strings)
lets users extend the list with their own vars.

**Effort**: Trivial (~10 LoC).  The harder part is documenting the
contract — what env vars the runner reads, when, and which are
required vs. optional.

**Impact on us**: We currently maintain the same list locally as
`_DEFAULT_FORWARDED_ENV` in `BazelContainerPackagingStrategy`; if
slurm-sdk ships the canonical list, we drop ours and import from
slurm-sdk.

---

### 15. Fix `runner.py` From-Env Cluster Fallback

**Issue**: In `slurm/runner.py`, the `prebuilt_images` dict decoded
from `SLURM_SDK_PREBUILT_IMAGES` is only assigned to
`cluster._prebuilt_dependency_images` inside the `if slurmfile_path:`
branch.  The fallback `if cluster is None: cluster = Cluster.from_env(env=env_name)`
branch — which is taken whenever the local launcher uses
`Cluster.from_args` rather than a Slurmfile, and is the standard
pattern in `slurm-sdk`'s own `examples/` directory — never makes
that assignment.

Result: child task submissions inside `@workflow` bodies fall
through `_prepare_packaging_strategy`'s prebuilt-images check
unmatched, the per-task strategy gets constructed fresh on the
worker, and any custom container strategy that depends on having
its build-time metadata available locally fails (see our
`BazelContainerPackagingStrategy` `WORKFLOWS_CLI_IMAGE_METADATA`
error during the integration work).

**Recommendation**: Lift the assignment out of the slurmfile branch
and apply it once after either branch successfully constructs
`cluster`.  Three-line patch:

```python
# After both cluster-init branches:
if prebuilt_images and cluster is not None:
    cluster._prebuilt_dependency_images = prebuilt_images
    logger.debug(
        "Stored %d pre-built dependency images on cluster",
        len(prebuilt_images),
    )
```

**Effort**: Trivial.

**Impact on us**: Lets us delete the
`Cluster._prepare_packaging_strategy` monkey-patch and
`_recover_prebuilt_dependency_images` in `auto/slurm_packaging/__init__.py`
(~50 LoC + 5 tests).  Tracked in our cleanup plan as Category 3
item 3.3.

---

### 16. Relax `_build_dependency_containers` Type Filter

**Issue**: `SubmittableWorkflow._build_dependency_containers` (in
`slurm/cluster.py`) hard-codes:

```python
if effective_packaging.get("type") != "container":
    continue
```

This silently skips every dependent task whose packaging type isn't
the built-in `"container"`.  For any custom container subtype (e.g.,
our `bazel_container`), the loop walks through and prepares nothing
— `prebuilt_images` stays empty, `with_dependencies()` has no
effect, and the workflow body fails to find pre-pushed images on
the worker.

This is a usability blocker for any third-party packaging strategy
that produces a container reference — the very thing
recommendation 5 (public plugin API) is supposed to enable.

**Recommendation**: Replace the hard-coded type check with a check
against the registered strategy registry (see recommendation 5):

```python
strategy_class = _STRATEGIES.get(effective_packaging.get("type"))
if strategy_class is None:
    continue  # unknown packaging type
# (proceed; if the strategy doesn't set _image_reference,
#  the existing post-prepare check already handles that.)
```

Built-in `"container"` callers see no change.  Custom container
subtypes participate as expected.

**Effort**: Trivial (a few lines).

**Impact on us**: Lets us delete the `pre_push_dependencies` helper
and switch `hello_workflow.py` back to the upstream-idiomatic
`cluster.submit(workflow).with_dependencies([greet, shout])` chain
(saves ~110 LoC of helper + tests).  Tracked as Category 3 item
3.2 in our cleanup plan.

---

### 17. Recognise Custom Container Strategies in `parent_packaging_type` Detection

**Issue**: `slurm/runner.py` (~lines 798–806) determines the parent
workflow's packaging type via:

```python
if parent_packaging_type not in {"wheel", "container"}:
    parent_packaging_type = (
        "wheel" if os.environ.get("VIRTUAL_ENV")
        else "container" if (
            os.environ.get("SINGULARITY_NAME")
            or os.environ.get("SLURM_CONTAINER_IMAGE")
        )
        else "none"
    )
```

`parent_packaging_type` initially comes from the decoded
``SLURM_SDK_PACKAGING_CONFIG`` env var, which contains the *type
string* of the parent's packaging strategy.  For `slurm-sdk`'s
serializer this is the lowercased class-name suffix:
`BazelContainerPackagingStrategy` becomes `"bazelcontainer"`.

`"bazelcontainer"` is not in the `{"wheel", "container"}` whitelist,
so the override fires and the env-var fallback runs.  Pyxis on most
clusters does not set `SLURM_CONTAINER_IMAGE`, so the chain ends with
`parent_packaging_type = "none"` — which is wrong, because the parent
*is* in a container, just not one slurm-sdk recognises.

The downstream effect: `InheritPackagingStrategy` reads the parent's
`.slurm_environment.json`, sees `packaging_type: "none"`, and decides
the parent has no container — so child tasks that wanted to inherit
the parent's container fail to do so.  We don't currently rely on
inherit semantics for `hello_workflow` (we use the prebuilt-images
path instead), so this manifests in our worker logs as a stray
`Parent packaging type: none` message that *looks* like a problem but
doesn't actually break us today.  It is a real upstream rough edge
for *any* custom container subclass that wants child tasks to inherit
its image.

**Recommendation**: Recognise any registered `PackagingStrategy`
whose class is a subclass of `ContainerPackagingStrategy` (or which
exposes an `_image_reference`) as a "container" parent type, rather
than gating on the `{"wheel", "container"}` set:

```python
# In runner.py, around the existing check:
def _is_container_parent_type(name: Optional[str]) -> bool:
    if name in {"wheel", "container"}:
        return name == "container"
    cls = _STRATEGIES.get(name)
    if cls is None:
        return False
    return issubclass(cls, ContainerPackagingStrategy)


if not _is_container_parent_type(parent_packaging_type):
    parent_packaging_type = (
        "wheel" if os.environ.get("VIRTUAL_ENV")
        else "container" if (
            os.environ.get("SINGULARITY_NAME")
            or os.environ.get("SLURM_CONTAINER_IMAGE")
        )
        else "none"
    )
elif parent_packaging_type not in {"wheel", "container"}:
    # Custom container subclass — collapse to "container" for the
    # downstream environment-metadata write, since the inherit
    # strategy and the env-metadata schema only know those names.
    parent_packaging_type = "container"
```

The collapse-to-`"container"` step lets the existing
`.slurm_environment.json` consumers (notably
`InheritPackagingStrategy`) work unchanged — they always saw a string
from `{"wheel", "container", "none"}` and they continue to.

**Alternative**: Use the *registered* strategy name (e.g.,
`"bazel_container"`) when serialising
`SLURM_SDK_PACKAGING_CONFIG.type`, instead of the lowercased class
name (`"bazelcontainer"`).  The serializer in `rendering.py` would
need to walk `_STRATEGIES` to find the name the strategy was
registered under, which is straightforward.  Then `parent_packaging_type`
checks would resolve via the registry the same way Change 2 of
recommendation 5 resolves the dispatch parser.  This is the more
principled fix and is independent of the `_is_container_parent_type`
helper above.

**Effort**: Low.  A few lines in `runner.py`, plus a small helper or
the rendering-side rename if the alternative path is taken.

**Impact on us**: We don't have a workaround for this today because
`hello_workflow` uses the prebuilt-images dispatch path (which
sidesteps the inherit semantics entirely).  Landing this would let
custom container subclasses use upstream's "child tasks inherit the
parent's container image" mechanism cleanly.  Also removes the
misleading `Parent packaging type: none` log line from worker output.

---

## Summary Table

| # | Area | Issue | Effort | Priority |
|---|------|-------|--------|----------|
| 1 | slurm-sdk | Image building coupled with strategy | High | High |
| 2 | slurm-sdk | Config validated too late | Medium | High |
| 3 | slurm-sdk | No push caching | Medium | Medium |
| 4 | slurm-sdk | Single registry per workflow | Low | Low |
| 5 | slurm-sdk | No public plugin API | Low | High |
| 6 | maglev.workflows | ImageHandler interface too broad | Medium | Medium |
| 7 | maglev.workflows | Eager file reads in constructor | Low | Medium |
| 8 | maglev.workflows | Confusing skip-digest logic | Low | Low |
| 9 | maglev.workflows | Metadata has no versioning | Low | Low |
| 10 | maglev.workflows | No public digest accessor | Low | Low |
| 11 | slurm-sdk | UUID tag default loses cross-build idempotency | Medium | Medium |
| 12 | slurm-sdk | No skip-push-if-unchanged fast path | Medium | Medium |
| 13 | slurm-sdk | Push requires Docker/podman daemon | Medium | Low |
| 14 | slurm-sdk | Container strategy doesn't forward `SLURM_SDK_*` env vars | Trivial | High |
| 15 | slurm-sdk | `runner.py` from-env fallback drops `prebuilt_images` | Trivial | High |
| 16 | slurm-sdk | `_build_dependency_containers` hard-codes type=="container" | Trivial | High |
| 17 | slurm-sdk | `parent_packaging_type` detection rejects custom container subclasses | Low | Medium |

**Implementation order for the next slurm-sdk release**:

1. **Item 5** (register_strategy + registry-aware parser) — biggest single unblocker; nothing else is meaningful for third-party strategies without it.  Self-contained, additive, fully specified above.
2. **Item 15** (3-line `runner.py` from-env fallback fix) — trivial bug fix; lets us delete one of our `_compat.py` shims immediately.
3. **Item 14** (forward `SLURM_SDK_*` env vars from `ContainerPackagingStrategy`) — trivial change; needs documentation of the runtime env-var contract more than code.
4. **Item 16** (relax `_build_dependency_containers` type filter) — depends on item 5 being in place so the registry lookup is meaningful.  Lets us delete the `pre_push_dependencies` helper.
5. **Item 17** (`parent_packaging_type` recognises custom container subclasses) — finishes off the "custom container strategies are first-class" story by making nested-workflow inherit semantics work for them.
6. **Items 1, 2** — bigger structural changes; valuable but not blockers.
7. **Items 11, 12, 13** — quality-of-life upgrades for the built-in container strategy; close the gap with the Bazel pipeline's structural advantages but don't directly affect third-party strategies.

**Highest priority for the auto/slurm_packaging integration**: items **5, 14, 15, 16** are blockers that we patched around in `auto/slurm_packaging/_compat.py` and `pre_push_dependencies`; landing them upstream lets us delete the corresponding shims and helper (~290 LoC + tests; tracked as Category 3 of `slurm_packaging_simplification_and_cleanup.md`).  Items **6 and 7** are the most impactful improvements on the `maglev.workflows` side (less relevant to our integration after we vendored `BazelImageHandler`, but useful for any other consumer).  Items **11, 12, 13** are quality-of-life upgrades for slurm-sdk's built-in container strategy.
