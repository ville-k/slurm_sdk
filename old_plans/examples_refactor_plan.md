# Examples Refactor Plan

## Goals
- All examples use container packaging; no wheel/non-container variants.
- Teach core SDK concepts in a clear progression (hello → container patterns → dependencies → workflows → distributed).
- Keep one canonical set under `src/slurm/examples/`, with minimal duplication in `examples/` (redirects only).
- Pair each example with a Diátaxis-aligned doc (tutorial vs. how-to guide) under `docs/tutorials` or `docs/guides`.
- Add integration coverage that exercises each example end-to-end on docker-compose SLURM, plus a final all-examples run.
- Remove Slurmfile reliance; examples configurable via CLI/env (cluster, registry, etc.).

## Proposed Structure (Container-First)
- `src/slurm/examples/hello_world.py`: containerized getting-started task; basic submit + result.
- `src/slurm/examples/dependencies.py` (from `dependent_jobs.py`): submitless chaining, automatic deps, result collection.
- `src/slurm/examples/workflows.py` (from `ml_workflow.py`): `@workflow`, array `.map`, conditional branches, shared dir usage.
- `src/slurm/examples/parallel_patterns.py` (from `parallelization_patterns.py`): map/reduce, fan-out/fan-in, time limits/partitions.
- `src/slurm/examples/distributed_training.py` (from `hello_torch.py`): multi-node CUDA/torch with container packaging.
- `src/slurm/examples/preflight_launch.py` (from `launch_with_preflight.py`): preflight checks and environment validation.
- `src/slurm/examples/container_mounts.py`: focused demo for mounts/placeholders (mirrors integration mount tests).
- `src/slurm/examples/workflow_graph_visualization.py`: graph viz; keep Dockerfile aligned.

## Docs (Diátaxis)
- Tutorials (`docs/tutorials/`): step-by-step learning journeys.
  - Getting Started with slurm-sdk (hello_world): install → container setup → first task run.
  - Building Your First Workflow (workflows): author workflow, run, inspect outputs.
- How-to Guides (`docs/guides/`): task-oriented recipes.
  - Container Dependencies & Chaining (dependencies).
  - Parallel Patterns & Map/Reduce (parallel_patterns).
  - Distributed Training with Torch (distributed_training).
  - Preflight Checks and Safe Launches (preflight_launch).
  - Working with Container Mounts (container_mounts).
  - Visualizing Workflow Graphs (workflow_graph_visualization).

## De-duplication / Cleanup
- Redirect `examples/*.py` to `src/slurm/examples/` (thin wrappers or symlinks), avoid divergence.
- Remove Slurmfile usage in examples; rely on CLI/env arguments instead.
- Move or drop stale Dockerfiles; co-locate Dockerfiles with their examples under `src/slurm/examples/`.

## Testing
- Integration-first: as each example is refactored, add an integration test that runs it end-to-end on docker-compose SLURM (skip if stack unavailable).
- Distributed: support CUDA/NCCL in code; integration test runs CPU mode to match compose.
- Final sweep: run all new integration tests together to validate the suite.

## Milestones & Tasks
1) Inventory & Cleanup
   - [ ] Identify canonical example scripts to keep; remove/redirect duplicates in `examples/`.
   - [ ] Consolidate Dockerfiles under `src/slurm/examples/`; prune stale ones.
   - [ ] Strip Slurmfile dependencies; ensure CLI/env drive configuration.
2) Example-by-Example (container-only, with docs + per-example integration test)
   - Hello World (tutorial: getting started)
     - [ ] Container-only; CLI accepts cluster/registry.
     - [ ] Tutorial under `docs/tutorials/` (install → container setup → first run).
     - [ ] Integration test: run hello_world on docker-compose SLURM (CPU).
   - Dependencies (guide: container dependencies & chaining)
     - [ ] Container-only; CLI for cluster/registry.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test on docker-compose.
   - Workflows (tutorial: building your first workflow)
     - [ ] Container-only; CLI for cluster/registry.
     - [ ] Tutorial under `docs/tutorials/`.
     - [ ] Integration test on docker-compose.
   - Parallel Patterns (guide)
     - [ ] Container-only; CLI for cluster/registry.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test on docker-compose.
   - Distributed Training (guide; CUDA/NCCL capable, CPU in tests)
     - [ ] Container-only; support CUDA/NCCL via args, default CPU-friendly path.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test runs CPU mode on docker-compose.
   - Preflight Launch (guide)
     - [ ] Container-only; CLI for cluster/registry.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test on docker-compose.
   - Container Mounts (guide)
     - [ ] Container-only; demonstrate mounts/placeholders.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test on docker-compose.
   - Workflow Graph Visualization (guide)
     - [ ] Container-only; Dockerfile co-located.
     - [ ] How-to guide under `docs/guides/`.
     - [ ] Integration test generates graph artifact on docker-compose.
3) Docs & Index
   - [ ] Update `examples/README.md` and `examples/EXAMPLES_SUMMARY.md` with new layout/commands.
   - [ ] Add navigation entries in `mkdocs.yml` for tutorials/guides.
4) Integration Harness
   - [ ] Add docker-compose runner that executes each example with test-friendly args; mark/skip if compose not available.
   - [ ] Add final suite that runs all example integrations together.
5) Final Polish
   - [ ] Verify Dockerfiles/contexts aligned per example.
   - [ ] Run all new integration tests; document commands/results.
