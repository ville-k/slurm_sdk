# JobContext & distributed launch helpers

Distributed workloads often need quick access to resolved host lists and rank
metadata to bootstrap launchers such as `torchrun`. The SDK now infers that
metadata directly from the job environment and exposes it through a
`JobContext` object.

## Opt-in by signature

Any `@task` whose function signature includes a parameter named `job` or
annotated with `JobContext` automatically receives the context:

```python
import os

from slurm.decorators import task
from slurm.runtime import JobContext

@task(time="00:10:00", ntasks=8)
def train(config: dict, *, job: JobContext) -> None:
    env = job.torch_distributed_env()
    os.environ.update(env)
    # Bootstrap torchrun or any other launcher
    run_training_loop(config)
```

No decorator flags are required and local invocations can omit the parameter—
the SDK only injects the context when running inside SLURM. If you prefer a
manual lookup, call `slurm.runtime.current_job_context()` from within your code.

## What the context provides

`JobContext` collects the common fields needed for PyTorch elastic launches:

- `hostnames`: resolved node list derived from `SLURM_NODELIST`.
- `world_size`, `rank`, `local_rank`, `node_rank`: numeric identifiers.
- `master_addr`, `master_port`: rendezvous defaults (`MASTER_ADDR` is the first
  hostname unless already defined).
- `output_dir`: Path to the job's output directory (from `JOB_DIR` environment variable).
- `torch_distributed_env()`: helper that returns the minimum environment
  dictionary (`MASTER_*`, `WORLD_SIZE`, `RANK`, `LOCAL_RANK`, `NODE_RANK`, ...).
- `environment`: the raw `SLURM_*` environment snapshot captured inside the job
  step.

Because the context is reconstructed from environment variables, it works inside
containers even when SLURM CLI tooling is unavailable.

## Callbacks and observability

Runner-side callbacks receive the same object via the new `job_context` field on
`RunBeginContext`, `RunEndContext`, and `CompletedContext`. This lets UI-focused
callbacks (like the preflight example) render allocation insight without parsing
environment variables themselves.

## Example

`slurm.examples.distributed_context` demonstrates the pattern end-to-end. Submit
it with a Slurmfile that requests multiple tasks and observe the returned
summary:

```bash
uv run python -m slurm.examples.distributed_context --slurmfile Slurmfile.toml
```

The result includes node names, ranks, and the derived torch environment map that
can be fed directly into `os.environ.update()` before launching your training
loop.
