---
hide:
  - navigation
  - toc
---
# SLURM SDK

A batteries-included Python toolkit for packaging workloads, submitting jobs, and tracking results
on [Slurm](https://slurm.schedmd.com/documentation.html) clusters. The SDK wraps the repetitive
orchestration steps—building artifacts, staging files, monitoring execution—so you can focus on
writing task code.

## Quick links

<div class="grid cards" markdown>

- :material-clock-fast:{ .lg .middle } [Getting Started](guides/getting_started.md): install the SDK and launch your first task.
- :material-book-multiple:{ .lg .middle }  [Guides](guides/README.md): deep dives into configuration, callbacks, rich UIs, and more.
- :material-api:{ .lg .middle } [Reference](reference/README.md): API docs and architectural notes.

</div>

## Why use this SDK?

- **Unified packaging** – build wheels or containers with consistent CLI ergonomics.
- **Declarative tasks** – describe resources with `@task`, override SBATCH options at submit time.
- **Extensible callbacks** – hook into packaging, submission, and runtime phases for custom UX or telemetry.
- **Local-first development** – iterate on macOS or Linux with the same tooling used in production clusters.

If you are coming from an existing SLURM deployment, start with the
[configuration guide](guides/configure_cluster.md) to migrate Slurmfile settings.
For advanced UI customization, explore the
[callback authoring guide](guides/authoring_callbacks.md) and the
`launch_with_preflight.py` example bundled with the SDK.

## Inspiration

This SDK draws inspiration from several workflow orchestration tools:

- **[Flyte](https://flyte.org/)** – a Kubernetes-native workflow orchestration platform with strong typing and versioning. 
- **[Prefect](https://www.prefect.io/)** – a modern workflow orchestration framework with dynamic task graphs and extensive monitoring capabilities.
- **[Ray](https://www.ray.io/)** – a distributed computing framework that provides high-level APIs for parallel and distributed Python. Ray Jobs enables running containerized workloads on clusters.

While these tools target general-purpose orchestration across various compute backends, this SDK focuses specifically on **Slurm environments** common in HPC and ML research computing. The design prioritizes:

- **Minimal abstraction** – thin wrappers around SLURM primitives rather than a new workflow DSL
- **HPC-first features** – native support for distributed training, GPU scheduling, and container runtimes (Enroot/Pyxis)
- **Local development** – seamless iteration on macOS/Linux before deploying to clusters
- **Extensibility** – callbacks and packaging strategies as first-class extension points

If you're already invested in Slurm infrastructure, this SDK lets you leverage that investment without migrating to a new orchestration layer.
