# Parallel Train + Eval Example

This example demonstrates a decorator-based workflow that runs training in capped
job segments, launches evaluation after each epoch, and keeps training moving
without waiting for evaluation to finish.

## Run
```bash
uv run python -m slurm.examples.parallel_train_eval.workflow \
  --hostname your-slurm-host \
  --username $USER \
  --partition debug \
  --epochs 3 \
  --epoch-steps 10 \
  --steps-per-job-cap 4
```

## Outputs
- `state.json` tracks epoch progress, checkpoints, and submitted job IDs.
- `checkpoints/epoch_XXX.json` records per-epoch training progress.
- `metrics/epoch_XXX.json` records evaluation results.

Artifacts are written under the workflow's shared directory (`WorkflowContext.shared_dir`),
and the state path is printed in the workflow output.
