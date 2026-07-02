## Task: Add a Slurm SDK example + matching Diátaxis tutorial

### Goal

Improve Slurm SDK docs by adding a **decorator-based (Flyte-like)** example + **tutorial** that teaches a realistic orchestration pattern: **parallel Train + Eval**, where training progress survives fairshare/timeouts and **may require multiple Slurm jobs per epoch**, while eval runs in parallel and does not block training.

---

## Deliverables

### 1) Example (self-contained + simple)

* Path: `src/slurm/examples/parallel_train_eval/`
* Files (suggested):

  * `workflow.py` – orchestrator job (CPU partition)
  * `train_task.py` – train task entrypoint
  * `eval_task.py` – eval task entrypoint
  * `README.md` – minimal usage
* Must use:

  * SDK’s **decorator-based workflow/task** API (Flyte-ish style)
  * `argparse` for CLI, `logging` for logs
  * outputs under a user-provided `--workdir` on shared FS
* Must be runnable in integration-test Slurm environment using **default partitions**, but allow overrides via CLI/config (follow conventions from other examples).

### 2) Tutorial (Diátaxis tutorial)

* Path: `docs/tutorials/parallel-train-eval-workflow.md`
* MkDocs Material, no frontmatter.
* Step-by-step “do this, then that”, and ends with user running the example and inspecting produced artifacts.

### 3) Tests

* Path: `tests/test_parallel_train_eval_example.py` (or match existing naming)
* Use the SDK’s existing **local/dry-run executor** to test without requiring Slurm.
* Formatting/linting must pass (`uv format` / ruff-based).

---

## Example requirements: Parallel Train + Eval workflow

### Functional behavior

* Workflow runs on CPU partition and orchestrates `N` epochs.
* Each epoch has `EPOCH_STEPS` total steps.
* Training for an epoch may take **multiple train jobs** because each job has a “time budget” cap:

  * `STEPS_PER_JOB_CAP` (deterministic stand-in for timeouts/preemption/fairshare variability)
  * each train job performs `min(remaining_steps, cap)` and exits cleanly
* At end of each epoch:

  * submit **one eval job** dependent on that epoch’s completion/checkpoint
  * **do not block** next epoch’s training on eval finishing (fire-and-forget).

### Artifacts/state (must be explicit and inspectable)

All stored in `--workdir`:

* `state.json` (or similar) with:

  * current epoch, steps completed in epoch
  * checkpoint path for epoch
  * job ids submitted (for debug)
* per-epoch checkpoint file(s): simple JSON is fine
* per-epoch metrics file(s): simple JSON is fine, must show it used the checkpoint/epoch args

### CLI (minimum)

* `--epochs`
* `--epoch-steps`
* `--steps-per-job-cap`
* `--workdir`
* partition overrides (default to standard integration partitions):

  * `--partition-workflow`, `--partition-train`, `--partition-eval`
    (Use the same config/CLI handling style as other SDK examples.)

### Observability

* Use `logging` to print:

  * job submissions (type + job id)
  * epoch progress updates
  * artifact paths written

---

## Testing acceptance criteria (local executor)

Using the SDK local/dry-run mode, tests should assert:

* workflow completes `epochs` epochs
* each epoch reaches `epoch_steps` total steps (via artifacts/state)
* when `steps_per_job_cap < epoch_steps`, multiple train jobs were required for an epoch
* exactly one eval job per epoch was launched
* eval launching does not gate progression to next epoch (can be checked via ordering in state/log records, or via the workflow’s control flow markers)

---

## “Done” definition

* Example runs in integration test context with default partitions; partitions can be overridden.
* Tutorial exists and matches the example.
* Tests pass using local/dry-run executor.
* Code is minimal, idiomatic for your decorator-based SDK, and passes `uv format`.

---

If you want, paste one of the existing examples that shows the “config / cli options handling” pattern you want mirrored, and I’ll tighten the agent instructions to match it exactly (names, flags, config objects, etc.).
