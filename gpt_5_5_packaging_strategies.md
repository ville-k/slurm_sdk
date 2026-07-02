# Extensible Packaging Strategies Design

Snapshot date: 2026-04-28.

This note evaluates the request to support third-party packaging strategies in
Slurm SDK, using a Bazel-built OCI image strategy as the motivating case. The
core ask is valid: the SDK has a real extensibility seam in `PackagingStrategy`,
but the public API does not currently let external strategies participate in
the same string-based configuration flow as built-in strategies.

## Summary Recommendation

Add a public packaging strategy registry and make packaging string parsing
registry-aware.

The minimal design should be:

1. Add `register_packaging_strategy()` and `unregister_packaging_strategy()` as
   public APIs under `slurm.packaging`.
1. Keep built-in strategy names reserved and non-overridable.
1. Make `parse_packaging_config("<name>")` resolve to `{"type": "<name>"}`
   when `<name>` is a registered custom strategy.
1. Preserve the existing raw-image fallback for unknown strings, including
   strings such as `"python:3.11"` and `"my-image"`.
1. Document the lifecycle clearly: the plugin must be imported, and therefore
   registered, before task decorators or cluster defaults parse that strategy
   name.

The additional suggestions from the integrating repo are valid, but they are
not all the same size. Abstract registry operations, pluggable image builders,
digest caching, and multi-registry support should be treated as a second layer:
they make the built-in container packaging strategy more composable, but they
are not required to unblock third-party strategy selection. Configuration
validation is small enough to include immediately after the registry MVP.

I would not add generic `custom:name` prefixes or entry-point discovery in the
first pass. They solve adjacent problems, but the core ask can be handled with a
small additive registry API and one parser change.

## Current State

The current implementation is close but not public:

- `src/slurm/packaging/__init__.py` has a private `_STRATEGIES` dict used by
  `get_packaging_strategy()`.
- `src/slurm/decorators.py` has a closed parser for `"auto"`, `"wheel"`,
  `"none"`, `"inherit"`, `"container"`, and `"container:..."`.
- Unknown strings intentionally become `{"type": "container", "image": value}`
  for backwards-compatible raw-image shorthand.
- `Cluster.__init__()` stores `default_packaging_*` kwargs in
  `cluster.default_packaging_kwargs`.
- `_packaging_resolver.resolve_packaging_config()` already merges task-level
  `packaging_*` kwargs with cluster defaults and feeds the resulting string
  through `_parse_packaging_config()`.
- `Cluster.from_env()` currently stores the Slurmfile `[env.packaging]` table
  directly as `cluster.packaging_defaults`.

That means the resolver and config-merging model are already usable. The missing
pieces are public registration and parser dispatch to registered names.

## Goals

- Third parties can register a `PackagingStrategy` without mutating private SDK
  state.
- A registered strategy is selectable through the same public surfaces as
  built-ins:
  - `@task(packaging="<name>", packaging_<key>=...)`
  - `Cluster(default_packaging="<name>", default_packaging_<key>=...)`
  - `Cluster.from_args(args, default_packaging="<name>", ...)`
  - Slurmfile `[env.packaging] type = "<name>"`
- Existing container shorthand behavior is preserved.
- Conflicts are explicit and fail loudly.
- The extension lifecycle is documented enough that users understand when and
  where registration must happen.
- Optional config validation has a natural place to fit later.

## Non-Goals

- Do not redesign all packaging configuration.
- Do not require third-party strategies to be pip-installed packages.
- Do not add automatic entry-point discovery in the MVP.
- Do not remove the raw-image shorthand fallback.
- Do not make Bazel a first-class dependency of Slurm SDK.
- Do not require the built-in container strategy to support every external build
  system before third-party strategies can register themselves.

## Assessment of Additional Suggestions

### Abstract Registry Operations

Validity: valid, but larger than the registry/parser MVP.

The current `ContainerPackagingStrategy` resolves image names, builds images,
pushes images, asks the registry for digests, and renders Pyxis `srun` commands
from one class. That coupling makes the built-in container strategy hard to
reuse for external image producers such as Bazel, Buildpacks, Buildah, or
kaniko.

The proposed `ContainerRegistry` abstraction is directionally right, but I would
shape it around image publishing and digest resolution rather than a generic
"registry" concept:

```python
class ContainerRegistryClient(Protocol):
    def qualify(self, image_ref: str, *, registry: str | None = None) -> str:
        """Return a fully qualified image reference."""

    def resolve_digest(self, image_ref: str) -> str | None:
        """Return an immutable digest reference when available."""

    def exists(self, image_ref: str) -> bool:
        """Return whether the image manifest exists remotely."""

    def push(self, local_ref: str, remote_ref: str) -> str:
        """Push an image and return the best available digest reference."""
```

This would let the built-in Docker/Podman strategy use the same registry client
as a Bazel-produced image. It should not be required for the initial third-party
strategy registration work because a custom strategy can own its own push logic
once it is reachable through the public API.

### Pluggable Image Builders

Validity: valid, but it should be a follow-up after public strategy
registration.

`ImageBuilder` is useful if the SDK wants one built-in "container strategy"
that can swap build systems:

```python
class ImageBuilder(Protocol):
    def build(self, config: Mapping[str, Any]) -> str:
        """Build or locate an image and return a local or remote image ref."""
```

However, `BazelContainerPackagingStrategy` does not need to be an
`ImageBuilder` plugin inside `ContainerPackagingStrategy` to solve the current
integration bug. It can remain a full `PackagingStrategy` because Bazel may have
different assumptions around metadata, target resolution, sandboxing, and remote
execution.

The better sequencing is:

1. Make third-party packaging strategies selectable.
1. Extract shared Pyxis execution rendering so custom container strategies do
   not copy the built-in `srun --container-image` logic.
1. Consider splitting the built-in container strategy into `ImageBuilder`,
   `ContainerRegistryClient`, and `PyxisExecutor` components.

This gives external strategies immediate support without prematurely forcing
all builders into one SDK-owned abstraction.

### Configuration Schema Validation

Validity: strongly valid and close to the MVP.

The integration report is correct that typo-prone `packaging_*` kwargs currently
flow deep into runtime behavior. The SDK should add a validation hook on
`PackagingStrategy`:

```python
class PackagingStrategy(ABC):
    @classmethod
    def validate_config(cls, config: Mapping[str, Any]) -> None:
        return None
```

I would not add Pydantic as a required dependency. Dataclasses, `TypedDict`, or
plain validation functions are enough and avoid a dependency decision. A strategy
can still use Pydantic internally if it already depends on it.

Validation should run when `get_packaging_strategy()` resolves a concrete class.
That catches bad config before `prepare()` performs builds, pushes, or remote
submission. Later, the parser/resolver can optionally call validation even
earlier once the strategy is known.

### Caching and Idempotency

Validity: high value, but it belongs to the container optimization roadmap
rather than the extensibility MVP.

The built-in strategy currently defaults to random image tags and `push=True`,
which makes repeated pushes likely unless callers manually use stable tags and
disable push for prebuilt images. Digest-aware caching would improve iteration
time and registry load.

Recommended follow-up behavior:

- Add content-hash tag generation for SDK-built images.
- Use registry manifest `HEAD` checks to skip pushing an image whose manifest
  already exists.
- Resolve and record digest references after push.
- Maintain an optional local cache from build key to remote digest reference.
- Keep cache correctness conservative: if authentication, registry capabilities,
  or manifest checks are uncertain, push rather than silently reusing an unknown
  image.

This is orthogonal to custom strategy registration. A Bazel strategy may have
its own action cache and remote cache, but both strategies can eventually share
the same `ContainerRegistryClient`.

### Multi-Registry Support

Validity: partially valid today, useful as a clearer API later.

The SDK already supports registry as part of each packaging config:

```python
@task(packaging="container", packaging_registry="registry-a.example.com/project")
def a() -> None:
    ...


@task(packaging="container", packaging_registry="registry-b.example.com/project")
def b() -> None:
    ...
```

Cluster defaults can make this feel like "single registry per workflow", but
task-level packaging kwargs can override the cluster default through the
existing resolver. Heterogeneous jobs can also carry different per-peer
packaging configs.

The missing piece is fluency and policy:

- named registry profiles, such as `registry_profile="secure-us"` or
  `registry_profile="scratch-dev"`;
- per-task registry selection without repeating full URLs;
- validation that a task's registry is allowed for its cluster, partition, or
  security domain;
- better docs showing per-task registry overrides.

This should be a follow-up API on top of the registry client, not part of the
strategy-registration MVP.

## Proposed Public API

Add these functions to `slurm.packaging`:

```python
def register_packaging_strategy(
    name: str,
    strategy_class: type[PackagingStrategy],
    *,
    override: bool = False,
) -> None:
    """Register a third-party packaging strategy by name."""


def unregister_packaging_strategy(name: str) -> None:
    """Remove a previously registered custom strategy."""


def is_packaging_strategy_registered(name: str) -> bool:
    """Return whether a strategy name is known to the registry."""


def list_packaging_strategies() -> dict[str, type[PackagingStrategy]]:
    """Return a copy of the current strategy registry."""
```

Names should be validated:

- Must be a non-empty string.
- Must not contain `:`, because colon is already part of the packaging string
  grammar for `container:IMAGE`.
- Should match a conservative identifier pattern, such as
  `^[a-zA-Z][a-zA-Z0-9_-]*$`.
- Must not collide with reserved built-ins:
  `auto`, `wheel`, `none`, `inherit`, `container`.

`register_packaging_strategy()` should reject:

- non-`PackagingStrategy` subclasses with `TypeError`;
- built-in names with `ValueError`;
- duplicate custom names with `ValueError` unless `override=True`.

`unregister_packaging_strategy()` should reject:

- built-in names;
- names that are not registered.

I prefer the longer function names over `register_strategy()` because the
package may grow other registries later. The explicit name is clearer in user
code:

```python
from slurm.packaging import register_packaging_strategy

register_packaging_strategy("bazel_container", BazelContainerPackagingStrategy)
```

An alias `register_strategy = register_packaging_strategy` is acceptable, but
the documentation should use the explicit name.

## Registry Internals

The current private `_STRATEGIES` dict can stay as an implementation detail, but
the cleaner layout is to move registry helpers into a small module:

```text
src/slurm/packaging/registry.py
```

Suggested structure:

```python
_RESERVED_STRATEGY_NAMES = frozenset(
    {"auto", "wheel", "none", "inherit", "container"}
)

_BUILTIN_STRATEGIES = {
    "wheel": WheelPackagingStrategy,
    "none": NonePackagingStrategy,
    "container": ContainerPackagingStrategy,
    "inherit": InheritPackagingStrategy,
}

_CUSTOM_STRATEGIES: dict[str, type[PackagingStrategy]] = {}
```

Then expose only copies or lookup helpers:

```python
def get_packaging_strategy_class(name: str) -> type[PackagingStrategy] | None:
    return _CUSTOM_STRATEGIES.get(name) or _BUILTIN_STRATEGIES.get(name)
```

This avoids encouraging third-party code to depend on a mutable private dict.
For source compatibility, `slurm.packaging._STRATEGIES` can remain as a merged
private mapping for one release, but internal code should stop importing it.

## Parser Semantics

Change `parse_packaging_config()` so the final fallback checks the public
registry before treating the string as an image reference.

Recommended order:

1. `None` or empty string returns `None`.
1. Built-in exact names are handled first.
1. `container:<image>` is handled second.
1. Registered custom exact names become `{"type": name, **kwargs}`.
1. Everything else remains the raw-image shorthand:
   `{"type": "container", "image": packaging, **kwargs}`.

Behavior table:

| Input string                               | Registered? | Result                                                           |
| ------------------------------------------ | ----------- | ---------------------------------------------------------------- |
| `"auto"`                                   | built-in    | `{"type": "auto"}`                                               |
| `"wheel"`                                  | built-in    | `{"type": "wheel"}`                                              |
| `"container"`                              | built-in    | `{"type": "container"}`                                          |
| `"container:nvcr.io/nvidia/pytorch:24.01"` | irrelevant  | `{"type": "container", "image": "nvcr.io/nvidia/pytorch:24.01"}` |
| `"python:3.11"`                            | no          | `{"type": "container", "image": "python:3.11"}`                  |
| `"my-image"`                               | no          | `{"type": "container", "image": "my-image"}`                     |
| `"bazel_container"`                        | yes         | `{"type": "bazel_container"}`                                    |
| `"bazel_container"`                        | no          | `{"type": "container", "image": "bazel_container"}`              |

This preserves compatibility for unregistered strings. The only changed case is
an exact string that the current process explicitly registered as a strategy.

If a user intentionally has a container image named the same as a registered
strategy, they can disambiguate with the existing explicit form:

```python
@task(packaging="container:bazel_container")
def run() -> None:
    ...
```

## Configuration Merging

The current `packaging_*` and `default_packaging_*` forwarding behavior should
remain unchanged.

Task-level example:

```python
@task(
    packaging="bazel_container",
    packaging_target="//auto/train:image",
    packaging_mounts=["/data:/data:ro"],
)
def train() -> None:
    ...
```

Expected parsed config:

```python
{
    "type": "bazel_container",
    "target": "//auto/train:image",
    "mounts": ["/data:/data:ro"],
}
```

Cluster default example:

```python
cluster = Cluster(
    backend_type="ssh",
    hostname="cluster.example.com",
    default_packaging="bazel_container",
    default_packaging_target="//auto/train:image",
)
```

Expected resolved config for an auto-packaged task:

```python
{
    "type": "bazel_container",
    "target": "//auto/train:image",
}
```

No resolver rewrite is needed for this path. The existing resolver already
merges cluster defaults with task-level packaging kwargs before parsing the
strategy string.

## Slurmfile Semantics

The proposal mentions a Slurmfile `default_packaging = "<name>"` entry, but the
current Slurmfile path in this repo uses an `[env.packaging]` table:

```toml
[default.packaging]
type = "wheel"
```

I would keep that shape and document custom strategies through the existing
table:

```toml
[default.packaging]
type = "bazel_container"
target = "//auto/train:image"
mounts = ["/data:/data:ro"]
```

This is better than adding a second Slurmfile spelling because:

- it already supports arbitrary strategy-specific keys;
- `Cluster.from_env()` already stores it as `cluster.packaging_defaults`;
- `resolve_packaging_config()` already uses it as a fallback;
- it does not require encoding non-string config into a `default_packaging_*`
  namespace inside TOML.

The important requirement is that the plugin module must be imported before the
first submission that resolves this packaging table. If a workflow job submits
child jobs from inside a runner, the runner process must also import the module
that performs registration.

## Registration Lifecycle

Registration is process-local. A third-party package should register at import
time:

```python
# auto/slurm_packaging/__init__.py
from slurm.packaging import register_packaging_strategy

from .bazel_container import BazelContainerPackagingStrategy

register_packaging_strategy(
    "bazel_container",
    BazelContainerPackagingStrategy,
)
```

User code then needs to import that package before using the strategy string:

```python
import auto.slurm_packaging  # registers "bazel_container"

from slurm import task


@task(packaging="bazel_container", packaging_target="//auto/train:image")
def train() -> None:
    ...
```

This is simple and works for Bazel monorepos where entry points are not a good
fit.

Documentation should call out the ordering rule explicitly:

- Register before decorators that parse custom packaging strings.
- Register before `Cluster(default_packaging="<custom>")` is used for
  submission.
- Register inside workflow runner code paths if nested workflow submissions use
  the custom strategy.

## Optional Future Discovery

Python entry-point discovery is useful for pip-installed plugins, but it should
not block the MVP. It can be added later as:

```toml
[project.entry-points."slurm.packaging_strategies"]
bazel_container = "auto.slurm_packaging:BazelContainerPackagingStrategy"
```

The entry-point loader can call `register_packaging_strategy()` internally.
This keeps the registry API as the stable primitive and avoids making packaging
extension depend on packaging/distribution metadata.

For monorepo and Bazel users, explicit import-time registration should remain
the recommended path.

## Validation Hook

Config validation is useful but should be a second step. The base class can
grow a no-op classmethod without breaking strategies:

```python
class PackagingStrategy(ABC):
    @classmethod
    def validate_config(cls, config: Mapping[str, Any]) -> None:
        return None
```

`get_packaging_strategy()` can call it immediately before instantiation:

```python
strategy_class.validate_config(config)
return strategy_class(config)
```

This catches typos like `packaging_taret` before a job is submitted. Keep the
hook pure: it should validate shape and required keys, not contact Slurm, run
Bazel, or inspect remote state.

For `BazelContainerPackagingStrategy`, validation might require exactly one of:

- `target`;
- `image`;
- `sqsh_path`;

depending on the external strategy contract.

## Unknown Strategy Handling

Today `get_packaging_strategy({"type": "does_not_exist"})` falls back to
`NonePackagingStrategy`. That behavior is lenient but dangerous for extension
users, because a missing registration silently runs without the expected
packaging layer.

I would not change this in the MVP if strict backwards compatibility is the
priority. Instead:

1. Add tests that registered custom types resolve correctly.
1. Add a warning when `get_packaging_strategy()` sees an unknown non-empty
   `type`.
1. In a future release, promote that warning to `PackagingError`.

The parser's raw-image fallback is separate and should stay.

## Worked Custom Strategy Example

The docs should include a small complete example:

```python
from typing import Any

from slurm.packaging import PackagingStrategy, register_packaging_strategy


class ExistingImageStrategy(PackagingStrategy):
    def prepare(self, task, cluster) -> dict[str, Any]:
        image = self.config["image"]
        return {"status": "success", "image": image}

    def generate_setup_commands(self, task, job_id=None, job_dir=None) -> list[str]:
        return []

    def generate_cleanup_commands(self, task, job_id=None, job_dir=None) -> list[str]:
        return []

    def wrap_execution_command(self, command, task, job_id=None, job_dir=None) -> str:
        image = self.config["image"]
        return f"srun --container-image={image} {command}"


register_packaging_strategy("existing_image", ExistingImageStrategy)
```

Usage:

```python
import my_project.slurm_packaging

from slurm import task


@task(
    packaging="existing_image",
    packaging_image="nvcr.io#nvidia/pytorch:25.10-py3",
)
def run() -> None:
    ...
```

The example should remain intentionally small. A separate example can show Bazel
or Pyxis-specific behavior if this becomes a published integration guide.

## Implementation Plan

Phase 1: registry and parser.

- Add public registry helpers under `slurm.packaging`.
- Reserve built-in names, including `auto`.
- Reject invalid names and duplicate custom names.
- Update `get_packaging_strategy()` to use the registry helper.
- Update `parse_packaging_config()` to check
  `is_packaging_strategy_registered()` before raw-image fallback.
- Export the new helpers from `slurm.packaging.__all__`.

Phase 2: validation hook.

- Add `PackagingStrategy.validate_config()` as a no-op classmethod.
- Call it from `get_packaging_strategy()` after resolving the strategy class and
  before instantiation.
- Raise `PackagingError` or `ValueError` with clear messages for invalid config;
  prefer `PackagingError` for user-facing submission failures.
- Keep Pydantic optional; strategies can use dataclasses, plain validation, or
  their own model library internally.

Phase 3: tests.

- Register a toy strategy and assert `parse_packaging_config("toy")` returns
  `{"type": "toy"}`.
- Assert `get_packaging_strategy({"type": "toy"})` instantiates the toy class.
- Assert built-in names cannot be registered or unregistered.
- Assert duplicate registration fails unless `override=True`.
- Assert invalid strategy classes fail.
- Assert unregistering a custom strategy restores raw-image fallback.
- Assert `"python:3.11"` and `"my-image"` still parse as container image refs
  when unregistered.
- Assert `Cluster(default_packaging="toy", default_packaging_foo="bar")` resolves
  to `{"type": "toy", "foo": "bar"}` through the existing resolver.
- Assert a Slurmfile `[packaging] type = "toy"` reaches `get_packaging_strategy`
  once registered.
- Assert a toy strategy's `validate_config()` runs and reports missing or
  misspelled required keys.

Phase 4: docs.

- Add a "Custom packaging strategies" section near the packaging docs.
- Document the base class methods.
- Document registration and unregistration.
- Document import ordering and workflow/nested-submission considerations.
- Show task, cluster-default, and Slurmfile examples.
- Mention entry points as future-compatible, not required.
- Document that task-level `packaging_registry` can override cluster registry
  defaults today.

Phase 5: hardening.

- Warn on unknown explicit `type` in `get_packaging_strategy()`.
- Consider promoting unknown explicit types to `PackagingError` in a later
  release.

## Second-Order Improvements

Once custom strategies are reachable, the next pain point will be avoiding code
copying between custom container strategies and the built-in container strategy.
The current `ContainerPackagingStrategy` owns both image build/push behavior and
Pyxis `srun` rendering. A Bazel strategy likely wants the Pyxis execution part
without the Docker/Podman build part.

The first clean follow-up is to extract a small public Pyxis rendering helper,
for example:

```python
class PyxisExecutionConfig(TypedDict, total=False):
    image: str
    mounts: list[str]
    workdir: str
    srun_args: list[str]
    python_executable: str
    mount_job_dir: bool


def render_pyxis_execution_command(
    command: str,
    config: PyxisExecutionConfig,
    *,
    job_id: str | None = None,
    job_dir: str | None = None,
) -> str:
    ...
```

Then third-party image builders can compose:

- their own `prepare()` implementation;
- the shared Pyxis execution renderer;
- the shared mount/workdir quoting behavior.

This is not required for the initial extensibility fix, but it would make
external container strategies much less brittle.

After that, split the built-in container pipeline into composable pieces:

```python
class ImageBuilder(Protocol):
    def build(self, config: Mapping[str, Any]) -> str:
        """Build or locate an image and return a local image reference."""


class ContainerRegistryClient(Protocol):
    def qualify(self, image_ref: str, *, registry: str | None = None) -> str:
        """Return a fully qualified image reference."""

    def exists(self, image_ref: str) -> bool:
        """Return whether the remote manifest exists."""

    def push(self, local_ref: str, remote_ref: str) -> str:
        """Push an image and return a digest reference when possible."""

    def resolve_digest(self, image_ref: str) -> str | None:
        """Return a digest reference without pulling the image when possible."""
```

The built-in container strategy would then become orchestration glue:

```text
Packaging config
  -> ImageBuilder
  -> ContainerRegistryClient
  -> Pyxis execution renderer
```

That split unlocks:

- Docker/Podman as the default `ImageBuilder`.
- Bazel, Buildah, kaniko, or Buildpacks as optional builders.
- Digest-based push skipping shared across builders.
- Per-task registry profiles and policy checks.
- Reuse of Pyxis mount/workdir/container-name rendering by third-party
  strategies.

Do this after strategy registration because builder/registry composition is a
larger design and is not necessary for third-party strategies to be reachable.

## Registry and Caching Roadmap

Container registry support should evolve conservatively because registry
behavior varies across Docker Hub, private registries, NGC, GHCR, and on-prem
mirrors.

Recommended sequence:

1. Introduce `ContainerRegistryClient` internally, backed by the current Docker
   Registry HTTP API digest resolver and Docker/Podman push commands.
1. Add `exists(image_ref)` using manifest `HEAD`.
1. Add content-hash tags for SDK-built images.
1. Add `push_policy`, with values like:
   - `"always"`: current behavior.
   - `"if_missing"`: skip push when the remote manifest exists.
   - `"never"`: prebuilt-image behavior.
1. Add an optional local cache file keyed by build inputs and registry target.
1. Add registry profiles:

```python
cluster = Cluster(
    ...,
    registry_profiles={
        "dev": {"registry": "registry-dev.example.com/team"},
        "secure": {"registry": "registry-secure.example.com/team"},
    },
)


@task(packaging="container", packaging_registry_profile="secure")
def train_secure() -> None:
    ...
```

Registry profiles are mainly a fluency improvement. They also make it possible
to attach policy later, such as disallowing a public registry on a sensitive
partition.

## Rollout

- Ship the registry/parser change as an additive minor or patch release.
- Update docs in the same PR.
- Let the integrating repo replace private `_STRATEGIES` mutation with
  `register_packaging_strategy("bazel_container", BazelContainerPackagingStrategy)`.
- Update integration examples to import the registration module before task
  decorators.
- If the integrating repo uses nested workflows, ensure the registration import
  is available in the runner path too.
- Follow with a separate container-composition PR for shared Pyxis rendering,
  then builder/registry abstractions and digest caching.

## Final Position

The proposal's main direction is right, but I would make two adjustments:

- Name the public API `register_packaging_strategy()` rather than the shorter
  `register_strategy()`.
- Preserve the existing Slurmfile `[packaging] type = ...` table instead of
  adding a separate `default_packaging = ...` TOML spelling.
- Treat `ContainerRegistry`, `ImageBuilder`, digest caching, and multi-registry
  profiles as important follow-up work rather than prerequisites for the
  third-party strategy registration fix.

With those adjustments, the design is small, compatible, and enough to unblock
`BazelContainerPackagingStrategy` without making the SDK responsible for Bazel
or any other third-party build system.
