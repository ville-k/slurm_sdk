# Add sidecars to a training job

## Problem

You run a long training job on an expensive GPU node and you want supporting
services — metrics collection, a TensorBoard server, a data prefetcher — to
live on the same allocation, share the node, and shut down cleanly when the
training loop finishes or crashes.

You do **not** want the sidecars to abort the job if they fail. You do want
them to exit promptly when training exits.

## Solution

Declare the training task as a leader and every helper as a
`Peer(on_failure="continue")` in a single `parallel(...)` call. The training
task drives the allocation; sidecars ride along and die with the leader.

```python
from slurm import Cluster, JobContext, Peer, parallel, task


@task(gpus_per_node=4, time="04:00:00", mem="32G")
def train(config: dict, ctx: JobContext) -> dict:
    # ... your training loop ...
    return {"accuracy": 0.94}


@task(cpus_per_task=1, mem="2G")
def metrics(ctx: JobContext) -> None:
    import time
    while not ctx.shutdown_requested:
        log_gpu_stats(ctx.output_dir)
        time.sleep(30)


@task(cpus_per_task=2, mem="4G")
def tensorboard(ctx: JobContext) -> None:
    import subprocess
    # subprocess-backed sidecars inherit SIGTERM via process group — no
    # explicit polling needed.
    subprocess.run(
        ["tensorboard", "--logdir", str(ctx.output_dir), "--port", "6006"],
        check=False,
    )


with Cluster.from_env("prod") as cluster:
    job = parallel(
        Peer(train.partial(config={"lr": 1e-3}), leader=True),
        Peer(metrics, on_failure="continue"),
        Peer(tensorboard, on_failure="continue"),
    )
    accuracy = job.leader_result
```

### Why this works

- One `parallel(...)` call submits a single allocation with `train` as the
  leader and each sidecar tolerated via `on_failure="continue"`.
- When `train` returns, the supervisor flags shutdown, sends `SIGTERM` to
  every sidecar's step, waits `grace_period_seconds` (default 10s), then
  hard-cancels.
- A sidecar crash never aborts training. Check `job.peer_outcomes()` if
  you care about whether the metrics collector died mid-run.
- `job.leader_result` returns the training task's return value directly —
  no dict lookup.

## Choose pure-Python or subprocess sidecars

| Sidecar shape         | Shutdown mechanism                                        | Example                                            |
| --------------------- | --------------------------------------------------------- | -------------------------------------------------- |
| Pure-Python loop      | Poll `ctx.shutdown_requested` each iteration.             | Metrics scraper, custom log aggregator.            |
| Subprocess wrapper    | `subprocess.run` / `Popen` inherits the SIGTERM.          | TensorBoard, `node-exporter`, `nvidia-smi daemon`. |
| Long sleep / blocking | Wrap in a small `while not ctx.shutdown_requested:` loop. | Periodic checkpoint uploader.                      |

The pattern: do short work, check the flag, sleep, repeat. Do not sleep for
minutes between checks — the supervisor's grace window is ~10 seconds by
default.

## Customise the grace period

Override the SIGTERM→SIGKILL window when sidecars need longer to drain:

```python
job = parallel(
    Peer(train.partial(config={"lr": 1e-3}), leader=True),
    Peer(metrics, on_failure="continue"),
    Peer(tensorboard, on_failure="continue"),
    grace_period_seconds=30,
)
```

## Make a sidecar's failure fatal

Sidecars default to nothing — you choose each peer's policy explicitly. If a
helper's crash *should* abort the job, give it `on_failure="kill"` (the
default) instead of `"continue"`:

```python
job = parallel(
    Peer(train.partial(config={"lr": 1e-3}), leader=True),
    Peer(metrics, on_failure="kill"),        # a metrics crash aborts the job
    Peer(tensorboard, on_failure="continue"),
)
```

## Verification

- `job.peer_outcomes()` reports `success` / `continue_on_failure` /
  `shutdown_by_leader` per peer — good for logging which sidecars drained
  cleanly.
- Check `ls $JOB_DIR/slurm_*.out` for per-step stdout; the supervisor
  writes one file per peer step even when they share a node.

## See also

- [Your first multi-peer job](../tutorials/parallel_tasks.md#4-add-a-leader-and-a-sidecar)
  — tutorial-style walkthrough.
- [Parallel reference](../reference/parallel.md) — the `Peer` / `parallel`
  docstrings.
- [How parallel allocations work](../explanation/slurm_allocations.md) —
  why `SIGTERM` propagates through `scancel` rather than a direct `kill`.
