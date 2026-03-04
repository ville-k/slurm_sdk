# High-Availability Training + Evaluation (Example)

This example implements a production-oriented train/eval workflow with a stable
`run_dir` and strong fault-tolerance semantics:

- **Outer supervisor** (Slurm workflow) performs reconciliation and resubmission.
- **Inner supervisor** (optional, NVRx-style) provides in-job restarts within a
  single allocation; the workflow handles failures that require rescheduling.

The implementation is intentionally lightweight (mocked train/eval logic) so it
can be used as a template for real training code.

High-availability mechanics are encapsulated in example-scoped helpers under
`slurm.examples.high_availability_training.ha` (workflow supervisor + task-attempt
runner), leaving this example focused on training/evaluation business logic.

## Run

```bash
uv run python -m slurm.examples.high_availability_training.workflow \
  --hostname your-slurm-host \
  --username $USER \
  --run-dir ~/slurm_jobs/runs/demo_ha_train_eval \
  --epochs 3 \
  --epoch-steps 10 \
  --max-steps-per-chunk 4
```

## Outputs

All artifacts are written under `run_dir/`:

- `state/state.json`: canonical workflow state (single-writer, atomic updates)
- `train/epoch_*/chunk_*/attempt_*/`: per-attempt training artifacts
- `eval/epoch_*/attempt_*/`: per-attempt evaluation artifacts
- `exports/latest_checkpoint.json`: stable pointer updated by the supervisor

Each task writes `result.json` as its commit record; the supervisor relies on
these records for reconciliation after restarts.

## In-job resiliency (NVRx)

This example includes wiring points for an inner, in-job supervisor:

- Set `--resiliency-enabled --resiliency-implementation mock` to use the built-in
  dependency-free restart loop for retryable exceptions (demo only).
- Set `--resiliency-enabled --resiliency-implementation nvrx` and provide a
  project-specific adapter via `resiliency_config.adapter="module:function"` to
  integrate `nvidia-resiliency-ext` in your runtime/container.

The workflow remains the scheduler-level supervisor that retries/resubmits when
the allocation is lost (preemption, timeout, node failure) or the in-job restart
budget is exhausted.
