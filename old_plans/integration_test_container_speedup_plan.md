# Integration test container speedup plan

## Goal
Speed up `tests/integration/` by eliminating redundant container builds/pushes when tests are not validating the container build pipeline itself.

## Current bottlenecks (as of today)
- `tests/integration/test_container_packaging_basic.py` builds + pushes a fresh image per test (Dockerfile written to `tmp_path`, random tag).
- `tests/integration/test_container_packaging_advanced.py` builds + pushes a fresh image per test (same Dockerfile content repeated).
- `tests/integration/test_container_packaging_comprehensive.py` *shares a Dockerfile* but still builds + pushes a fresh image per test because `ContainerPackagingStrategy` defaults to a random tag when `packaging_tag` isn’t set.
  - This file has ~14 tests that each wrap a task/workflow with `packaging_dockerfile=...` → ~14 builds + pushes.
- `tests/integration/test_examples_end_to_end.py` runs multiple examples that hard-code `default_packaging="container"` + `default_packaging_dockerfile=...`, which forces container builds even when the test is verifying workflow/SDK behavior rather than image construction.
- `tests/integration/conftest.py` starts compose with `--build` unless `SLURM_TEST_COMPOSE_NO_BUILD` is set; this is a secondary slowdown.

## Guiding principle
Split “container **build pipeline** coverage” (needs docker/podman, builds/pushes) from “container **runtime** coverage” (runs jobs in a container image, but should reuse a prebuilt image).

## Proposed design

### 1) Introduce a shared “integration test runtime image”
Create a single base image that contains:
- `slurm-sdk` installed (from repo source)
- Any Python deps required by container-runtime tests (e.g. `numpy` if we want `numpy_task` without a second image)
- Modules needed by runtime tests (preferably importable from `src/` so we don’t need to copy `tests/` into the image)

Recommendation:
- Prefer moving any “must be importable remotely” task functions from `tests/integration/*.py` into `src/slurm/examples/` (similar to `src/slurm/examples/container_test_functions.py`) so the image only needs `src/` + `pyproject.toml` + `README.md`.

Output:
- One (or two) images pushed to the local registry once per test session:
  - `registry:20002/slurm-sdk/it-base:<tag>`
  - Optional: `registry:20002/slurm-sdk/it-numpy:<tag>` (only if we don’t want numpy in base)

### 2) Add a session-scoped fixture that builds/pushes once
Add a fixture in `tests/integration/conftest.py` (session scope) that:
1. Computes a deterministic tag (to enable reuse within a session and across reruns when the registry volume is kept):
   - Suggested inputs: Dockerfile contents + `pyproject.toml` + relevant `src/slurm/**` + any files copied into the image.
   - Tag example: `it-<short_sha256>`.
2. Builds the image via the host container runtime (`docker`/`podman`) with `--platform` derived from the host (not hard-coded `linux/arm64`).
3. Pushes to the local registry (`registry:20002`) once.
4. Returns the final image reference string for tests to use.

Optional improvements:
- Pre-pull into the Slurm container / warm enroot cache once per session (if there’s a supported mechanism).
- If the local registry is persistent (named volume) and `SLURM_TEST_KEEP=1`, attempt “pull first, build only if missing”.

### 3) Refactor tests to use the shared image (stop building per test)

#### `test_container_packaging_comprehensive.py`
Target outcome: only **one** container build/push per session for this file (or even zero if we reuse a prebuilt image).

Steps:
- Replace per-test `task(... packaging_dockerfile=...)` / `workflow(... packaging_dockerfile=...)` wrappers with:
  - `packaging="container:<prebuilt_image_ref>"` and `packaging_push=False`, or
  - `packaging="container"` + `packaging_image=<prebuilt_image_ref>` + `packaging_push=False`.
- Keep **one** small test that actually exercises `packaging_dockerfile` → build/push (to validate the build pipeline still works).

Special case: `test_workflow_with_different_child_container`
- Keep the intent (“child task has a different container than the parent; use `with_dependencies()` so the child image is available”).
- But avoid rebuilding the same Dockerfile twice:
  - Option A (fast): use two *tags* pointing to the same built image (`it-base:<tag>` and `it-base-child:<tag2>`) by retagging + pushing once (fixture can do this).
  - Option B (coverage): keep one dependency build test, but make it the only dependency-build case and mark it separately (see markers below).

#### `test_container_packaging_basic.py` and `test_container_packaging_advanced.py`
Target outcome: remove duplicated Dockerfile writes and avoid per-test builds where not needed.

Steps:
- Convert these to “runtime” tests that use the session prebuilt image:
  - `test_basic_container_task_execution`: run `hello_container` in `it-base`.
  - `test_container_with_dependencies`: either move to wheel packaging, or run in `it-numpy` / `it-base` (if numpy included).
  - `test_array_job_with_containers`: run in `it-base`.
  - `test_container_with_mounts`: run in `it-base` (mounts are runtime behavior).
- Keep **at most one** “build from Dockerfile” test in the whole suite (or one per runtime, if needed).

### 4) Reduce example-driven container builds (`test_examples_end_to_end.py`)
This is currently forced because several examples hard-code container defaults (e.g. `src/slurm/examples/map_reduce.py`, `src/slurm/examples/hello_world.py`).

Two approaches (can be combined):

**A. Make examples respect `--packaging` overrides (preferred)**
- Update examples to follow the pattern used in `src/slurm/examples/workflow_graph_visualization.py`:
  - Only set `default_packaging="container"` / `default_packaging_dockerfile=...` when the user did not pass `--packaging`.
  - If `--packaging` is provided and is `wheel`/`none`, do not force container defaults.
  - If `--packaging` is `container` and no Dockerfile/image was provided, set a default Dockerfile.
- Add missing CLI args to `Cluster.add_argparse_args` (or example-local args) so tests can do:
  - `--packaging container:registry:20002/slurm-sdk/it-base:<tag>`
  - `--packaging-push false` (or equivalent) to avoid pushing prebuilt images.

**B. Narrow the containerized examples we run**
- Keep 1–2 containerized examples for smoke coverage.
- Run the rest in `wheel` packaging (faster and still validates core orchestration).

### 5) Markers and skip behavior (make the fast path widely runnable)
Right now `tests/integration/conftest.py` skips *all* `@pytest.mark.container_packaging` tests if docker CLI is unavailable.

Refactor to split markers:
- `container_runtime`: needs a cluster capable of running containers, but not docker CLI access from the test runner.
- `container_build`: needs docker/podman CLI access to build/push.

Then:
- Only skip `container_build` when `DOCKER_CLI_AVAILABLE` is false.
- Allow `container_runtime` to run in devcontainer/CI contexts where the cluster is up and images are already present.

## Optional SDK-level improvements (nice-to-have, not required for test speedup)
These would reduce the chance of accidental rebuilds outside tests too:
- In `src/slurm/packaging/container.py`, consider defaulting `push` to:
  - `True` when building from a Dockerfile (`dockerfile`/`context` present)
  - `False` when an explicit `image` is provided
- Add an opt-in `tag_strategy="hash"` mode to avoid random tags and enable reuse for identical Dockerfiles/build contexts.
- Add a `skip_build_if_image_exists` mode (best-effort) by checking `docker manifest inspect` / `podman manifest inspect` before building.

## Rollout plan (incremental PRs)
1. ✅ Add prebuilt image fixture + deterministic tag (no test behavior changes yet).
2. ✅ Refactor `test_container_packaging_comprehensive.py` to reuse the prebuilt image; leave 1 build/push test behind.
3. ✅ Refactor `test_container_packaging_basic.py` and `test_container_packaging_advanced.py` similarly; consolidate repeated Dockerfile content.
4. ✅ Fix `test_examples_end_to_end.py` by reducing which examples require containers and running the rest with wheel packaging.
5. ✅ Split markers (`container_runtime` vs `container_build`) and update skip logic.
6. (Optional) Apply SDK-level improvements to make reuse the default where safe.

## Success criteria
- Container builds in a full integration run drop from “~N per test” to “1–2 per session”:
  - `test_container_packaging_comprehensive.py`: ~14 → 0–1
  - `basic/advanced`: ~4 → 0–1 total
  - examples: many → 0–2
- Integration runtime improves materially (target: 3–10× faster depending on machine/registry).
- Coverage is preserved:
  - At least one test still validates Dockerfile build + push.
  - Container runtime semantics (Pyxis, mounts, callbacks, workflows, `with_dependencies`) still exercised.

