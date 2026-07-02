# Authoring Callbacks

Callbacks let you follow the full lifecycle of a submitted task—covering packaging, submission, execution, scheduler polling, and completion—without having to wire the plumbing yourself. The SDK now provides typed contexts, execution loci, and an optional polling service so you can focus on the UI or integration logic you care about.

## Lifecycle contexts

Each hook receives a dataclass carrying the data that is stable for that phase:

- `PackagingBeginContext` / `PackagingEndContext`: task object, packaging config/result, timestamps, cluster reference.
- `SubmitBeginContext` / `SubmitEndContext`: effective SBATCH options, job directory, pre-submission id, packaging strategy.
- `RunBeginContext`: module/function identifiers, args/kwargs pickle locations, job id/dir, host metadata, environment snapshot, start time.
- `RunEndContext`: success/failure status, error detail, stdout/stderr locations, timing metadata.
- Both run contexts surface a `job_context` field exposing the resolved `JobContext`, so you can inspect host lists or distributed ranks without shelling out from the container.
- `JobStatusUpdatedContext`: emitted by the SDK poller with scheduler snapshots, previous state, and terminal flag.
- `CompletedContext`: emitted on both runner and client sides with job state, exit code, stdout/stderr paths, timing information, and (on failure) traceback details.

Use the dataclasses to drive logging, Rich consoles, metrics, or anything else that needs structured data instead of parsing stdout.

## Execution loci

Every hook now has an execution locus hint that controls where it is invoked:

| Hook | Default locus |
| --- | --- |
| Packaging / Submission hooks | `client` |
| `on_begin_run_job_ctx`, `on_end_run_job_ctx` | `runner` |
| `on_completed_ctx` | `both` |
| `on_job_status_update_ctx` | `client` |

Override the defaults on a callback with the `execution_loci` mapping:

```python
class RunnerOnlyLogger(BaseCallback):
    execution_loci = {
        "on_completed_ctx": ExecutionLocus.RUNNER,
    }

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        print(f"[runner] state={ctx.job_state} exit={ctx.exit_code}")
```

Callbacks are pickled and shipped to the runner only when they declare work that needs the runner locus. Set `requires_pickling = False` on callbacks that must never leave the client.

## SDK-managed polling

Set `poll_interval_secs` on a callback to opt into the shared job-status poller. The SDK creates one background thread per job (only if at least one callback asks for it) and fans out `JobStatusUpdatedContext` instances at the requested cadence. Each callback is rate-limited individually, but receives an immediate update on state transitions and when the job becomes terminal.

```python
class StatusPrinter(BaseCallback):
    poll_interval_secs = 2.0

    def on_job_status_update_ctx(self, ctx: JobStatusUpdatedContext) -> None:
        state = ctx.status.get("JobState", "UNKNOWN")
        print(f"[{ctx.job_id}] {state}")

    def on_completed_ctx(self, ctx: CompletedContext) -> None:
        print(f"[{ctx.job_id}] final={ctx.job_state} exit={ctx.exit_code}")
```

## Runner payloads

Runner-side callbacks now receive richer payloads:

- `RunBeginContext` includes hostname, interpreter details, current working directory, and a light environment snapshot (`SLURM_JOB_ID`, `JOB_DIR`, etc.).
- `RunEndContext` and `CompletedContext` carry stdout/stderr paths, duration metrics, and full exception metadata when the task fails.
- `JobContext` is available via `ctx.job_context` in run/completed hooks or directly through `slurm.runtime.current_job_context()` in user code. It contains resolved hostnames, world size, ranks, and the raw `SLURM_*` environment captured inside the job step.

That makes it straightforward to build failure dashboards or write the diagnostic bundle to an artifact store.

## Opting into the completed event

`on_completed_ctx` fires on the runner first (while the job script is still running) and again on the client once the scheduler reports a terminal state. Use the `emitted_by` flag to distinguish the two sides:

```python
def on_completed_ctx(self, ctx: CompletedContext) -> None:
    if ctx.emitted_by is ExecutionLocus.RUNNER:
        return  # runner already logged rich diagnostics
    log.info("job %s finished state=%s", ctx.job_id, ctx.job_state)
```

Because the client emission happens even without the poller, callbacks that only need a final summary can just implement `on_completed_ctx`.

## Next steps

- Set `poll_interval_secs` on existing callbacks to remove custom polling threads.
- Review default loci to ensure only the required callbacks run on the runner.
- Use the new dataclass fields when rendering UIs or archiving metrics; the fields are stable and covered by tests.
- For Python tasks that need direct access to the allocation metadata, add a `job: JobContext` parameter to your function signature (or call `slurm.runtime.current_job_context()`). The [Job Context guide](job_context.md) walks through the API in more detail.
