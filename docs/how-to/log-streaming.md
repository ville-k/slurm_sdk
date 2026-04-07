# How to Stream Live Job Output

## Problem

After submitting a job, you want to see its output in real-time without SSH-ing to the compute node manually. You may also want to capture output to a file or string buffer for programmatic use.

## Prerequisites

- SLURM SDK installed
- A cluster configured via Slurmfile or environment

## Steps

### 1. Stream output while waiting for a job

Call `job.tail()` after submission to stream stdout in real-time. It blocks until the job completes, replacing the need for `job.wait()`:

```python
from slurm import Cluster, task

@task(time="00:10:00")
def train(epochs: int):
    for i in range(epochs):
        print(f"Epoch {i+1}/{epochs} complete")

with Cluster.from_env("prod") as cluster:
    job = cluster.submit(train)(epochs=10)
    job.tail()  # streams output, blocks until done
```

If the job is still pending, `tail()` waits for it to start before streaming.

### 2. Stream stderr instead of stdout

Pass `stderr=True` to follow the stderr file:

```python
job.tail(stderr=True)
```

### 3. Print current output without following

To print the last N lines and return immediately (like `tail` without `-f`):

```python
job.tail(follow=False, lines=50)
```

### 4. Capture output to a string or file

Pass any writable IO object via the `output` parameter:

```python
import io

# Capture to a string buffer
buf = io.StringIO()
job.tail(output=buf, follow=False)
print(buf.getvalue())

# Write to a log file
with open("job.log", "w") as f:
    job.tail(output=f)
```

### 5. Use the CLI

Stream output from a running job by ID:

```bash
slurm jobs tail <job-id>
```

Common options:

```bash
slurm jobs tail <job-id> --stderr       # stream stderr
slurm jobs tail <job-id> --no-follow    # print and exit
slurm jobs tail <job-id> -n 50          # show last 50 lines
```

Press `Ctrl+C` to stop streaming without cancelling the job.

### 6. Get a structured status snapshot

For programmatic use, `job.snapshot()` combines job state, exit code, and the last lines of stdout/stderr into a single object:

```python
snap = job.snapshot(tail_lines=20)
print(snap.state)        # e.g. "RUNNING", "COMPLETED"
print(snap.is_terminal)  # True if the job has finished
print(snap.stdout_tail)  # last 20 lines of stdout
print(snap.stderr_tail)  # last 20 lines of stderr
```

## Verification

After calling `job.tail()`, you should see the job's print statements appear in your terminal as the job runs. When the job completes, `tail()` returns automatically.

## Troubleshooting

- **"Output file path not available"**: The job metadata doesn't include stdout/stderr paths. This can happen with jobs retrieved by ID if the backend doesn't store path metadata. Submit via the SDK to ensure paths are tracked.
- **No output appears**: The job may be buffering stdout. Add `PYTHONUNBUFFERED=1` to your task environment or use `print(..., flush=True)` in your task function.
- **Ctrl+C doesn't stop streaming**: `tail()` handles `KeyboardInterrupt` and stops cleanly. If it hangs, the underlying SSH connection may be slow to close.

## See also

- [API Reference: Job](../reference/api/jobs_arrays.md) for full `tail()` and `snapshot()` signatures
- [Using the CLI](cli.md) for all available `slurm jobs` subcommands
