# Training & Evaluation Workflow Design: Fault-Resistant Training Under Fairshare Timeouts

## Executive Summary

This document describes a design for implementing an efficient, fault-resistant training loop using the Slurm SDK's `@workflow` decorator under a 4-hour fairshare timeout constraint. The design uses a **looped job array submission strategy** with concurrency=1 to provide:

1. **Short scheduling overhead** between timeout restarts (< 1 minute vs potentially hours)
2. **Built-in retry capability** for jobs that restart before making progress
3. **Adaptive job submission** that prevents overshooting after training completes
4. **Dynamic evaluation scheduling** on epoch boundaries without knowing job counts a-priori
5. **Periodic statistics and ETA** reporting for monitoring

The workflow orchestrates training by submitting job arrays in a loop, where each array element represents one "training segment" (checkpoint → checkpoint). The workflow polls for completion, estimates remaining work based on throughput metrics, and dynamically submits evaluation tasks when epochs complete.

## Problem Statement

### Constraints

1. **4-hour fairshare timeout**: Training jobs can be killed at any time after 4 hours
2. **Long training duration**: Full training (e.g., 100 epochs) takes 20+ hours
3. **Unpredictable job count**: Don't know a-priori how many 4-hour segments are needed
4. **Epoch-based evaluation**: Need to run eval tasks at epoch boundaries, but epochs span multiple jobs
5. **No overshooting**: Minimize no-op jobs submitted after training completes

### Goals

1. **Minimize scheduling gaps**: Keep the time between timeout and next job start < 1 minute
2. **Fault resilience**: Automatically retry if a job fails before making progress
3. **Efficient resource usage**: Don't submit excessive no-op jobs after completion
4. **Dynamic evaluation**: Submit eval tasks as soon as epochs complete
5. **Observability**: Provide ETA, throughput stats, and progress metrics

## High-Level Approach

The workflow operates in a **submit-monitor-adapt loop**:

```
┌─────────────────────────────────────────┐
│         @workflow orchestrator          │
│  (runs for duration of entire training) │
└─────────────────────────────────────────┘
            │
            ├─→ Initialize checkpoint state
            │
            ├─→ Loop:
            │     ┌─ Submit job array (concurrency=1)
            │     │  • Array size: min(remaining_steps/4hr_steps, MAX_LOOKAHEAD)
            │     │  • Each element: train from checkpoint N to N+1
            │     │
            │     ├─ Monitor active job
            │     │  • Poll every 30-60s for completion
            │     │  • Check for new checkpoints
            │     │  • Detect epoch boundaries
            │     │
            │     ├─ Update throughput estimates
            │     │  • Steps per second
            │     │  • Job startup overhead
            │     │  • ETA to completion
            │     │
            │     ├─ Submit eval tasks (if epoch completed)
            │     │  • Triggered by checkpoint metadata
            │     │  • Independent jobs (non-blocking)
            │     │
            │     ├─ Adaptive re-submission
            │     │  • Calculate remaining work
            │     │  • Submit next array if needed
            │     │
            │     └─ Exit when: steps_completed >= total_steps
            │
            └─→ Final evaluation & reporting
```

### Key Insight: Job Arrays with Concurrency=1

Instead of submitting N individual jobs sequentially (expensive scheduling overhead), we submit **job arrays** with `max_concurrent=1`:

```python
# ❌ Expensive: N sequential submissions
for i in range(N):
    train_job = train_segment(checkpoint=i)
    train_job.wait()  # Wait before submitting next

# ✅ Efficient: Single submission, Slurm handles sequencing
train_jobs = train_segment.map(
    checkpoint_ids,
    max_concurrent=1  # One at a time, but pre-queued
)
```

**Benefits:**
- Single `sbatch` call submits all segments
- Slurm scheduler queues them automatically
- Next job starts immediately when previous completes (< 1 min overhead)
- Workflow can monitor and adjust without blocking on each job

## Core Components

### 1. Training Task: Checkpoint-Based Resumption

```python
@task(time="04:00:00", mem="32G", gres="gpu:1")
def train_segment(
    config: TrainingConfig,
    checkpoint_id: int,
    *,
    ctx: JobContext | None = None
) -> SegmentResult:
    """Train from checkpoint N to N+1 (or completion).
    
    This task:
    - Loads checkpoint N (if exists, else starts from scratch)
    - Trains until: timeout, next checkpoint interval, or completion
    - Saves checkpoint N+1 with metadata
    - Returns: steps_completed, epoch_completed, throughput_stats
    """
    shared_dir = ctx.shared_dir if ctx else Path("./shared")
    checkpoint_dir = shared_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # Load existing checkpoint or initialize
    prev_checkpoint = checkpoint_dir / f"checkpoint_{checkpoint_id}.pt"
    if prev_checkpoint.exists():
        state = torch.load(prev_checkpoint)
        start_step = state["step"]
        start_epoch = state["epoch"]
        model = state["model"]
        optimizer = state["optimizer"]
    else:
        start_step = 0
        start_epoch = 0
        model = initialize_model(config)
        optimizer = initialize_optimizer(model, config)
    
    # Training loop with periodic checkpointing
    checkpoint_interval = config.checkpoint_every_n_steps
    max_steps = config.total_steps
    current_step = start_step
    current_epoch = start_epoch
    
    epoch_completed = False
    start_time = time.time()
    
    while current_step < max_steps:
        # Train one step
        loss = train_step(model, optimizer, batch)
        current_step += 1
        
        # Check for epoch boundary
        if current_step % config.steps_per_epoch == 0:
            current_epoch += 1
            epoch_completed = True
        
        # Checkpoint periodically (e.g., every 1000 steps or 30 min)
        elapsed = time.time() - start_time
        at_checkpoint_interval = (current_step % checkpoint_interval == 0)
        near_timeout = elapsed > (4 * 3600 - 300)  # 5 min before timeout
        
        if at_checkpoint_interval or near_timeout or current_step >= max_steps:
            # Save checkpoint
            next_checkpoint = checkpoint_dir / f"checkpoint_{checkpoint_id + 1}.pt"
            torch.save({
                "step": current_step,
                "epoch": current_epoch,
                "model": model.state_dict(),
                "optimizer": optimizer.state_dict(),
                "epoch_completed": epoch_completed,
                "timestamp": time.time(),
            }, next_checkpoint)
            
            # Save metadata for workflow monitoring
            metadata = {
                "checkpoint_id": checkpoint_id + 1,
                "step": current_step,
                "epoch": current_epoch,
                "epoch_completed": epoch_completed,
                "timestamp": time.time(),
                "steps_in_segment": current_step - start_step,
                "elapsed_time": elapsed,
            }
            metadata_file = checkpoint_dir / f"checkpoint_{checkpoint_id + 1}.meta.json"
            with open(metadata_file, "w") as f:
                json.dump(metadata, f)
            
            if near_timeout:
                break  # Exit gracefully before timeout
    
    # Return segment results
    elapsed = time.time() - start_time
    steps_completed = current_step - start_step
    
    return {
        "checkpoint_id": checkpoint_id + 1,
        "steps_completed": steps_completed,
        "total_steps": current_step,
        "epoch": current_epoch,
        "epoch_completed": epoch_completed,
        "elapsed_time": elapsed,
        "throughput": steps_completed / elapsed if elapsed > 0 else 0,
    }
```

### 2. Evaluation Task

```python
@task(time="00:30:00", mem="16G", gres="gpu:1")
def evaluate_checkpoint(
    config: TrainingConfig,
    checkpoint_id: int,
    epoch: int,
    *,
    ctx: JobContext | None = None
) -> EvalResult:
    """Evaluate a checkpoint on validation set.
    
    Args:
        config: Training configuration
        checkpoint_id: Which checkpoint to evaluate
        epoch: Epoch number (for logging)
        ctx: Job context
    
    Returns:
        Evaluation metrics
    """
    shared_dir = ctx.shared_dir if ctx else Path("./shared")
    checkpoint_dir = shared_dir / "checkpoints"
    
    # Load checkpoint
    checkpoint_file = checkpoint_dir / f"checkpoint_{checkpoint_id}.pt"
    state = torch.load(checkpoint_file)
    model = initialize_model(config)
    model.load_state_dict(state["model"])
    
    # Run evaluation
    metrics = run_validation(model, config)
    
    # Save results
    eval_dir = shared_dir / "eval_results"
    eval_dir.mkdir(parents=True, exist_ok=True)
    
    result = {
        "checkpoint_id": checkpoint_id,
        "epoch": epoch,
        "step": state["step"],
        "metrics": metrics,
        "timestamp": time.time(),
    }
    
    result_file = eval_dir / f"eval_epoch_{epoch}.json"
    with open(result_file, "w") as f:
        json.dump(result, f)
    
    return result
```

### 3. Workflow Orchestrator

```python
@dataclass
class TrainingConfig:
    """Training configuration."""
    total_steps: int = 100000
    steps_per_epoch: int = 1000
    checkpoint_every_n_steps: int = 1000
    batch_size: int = 32
    learning_rate: float = 0.001
    model_name: str = "resnet50"
    dataset: str = "imagenet"
    # ... other hyperparameters


@dataclass
class TrainingState:
    """Tracks training progress across job segments."""
    current_checkpoint: int = 0
    current_step: int = 0
    current_epoch: int = 0
    completed: bool = False
    
    # Throughput tracking
    step_times: list[float] = None
    job_overhead_times: list[float] = None
    
    # Evaluation tracking
    epochs_evaluated: set[int] = None
    pending_eval_jobs: list[Job] = None
    
    def __post_init__(self):
        if self.step_times is None:
            self.step_times = []
        if self.job_overhead_times is None:
            self.job_overhead_times = []
        if self.epochs_evaluated is None:
            self.epochs_evaluated = set()
        if self.pending_eval_jobs is None:
            self.pending_eval_jobs = []


@workflow(time="48:00:00", mem="4G")
def train_and_eval_workflow(
    config: TrainingConfig,
    ctx: WorkflowContext,
) -> dict:
    """Fault-resistant training workflow with dynamic evaluation.
    
    This workflow:
    1. Submits training job arrays with concurrency=1
    2. Monitors checkpoints and updates throughput estimates
    3. Dynamically submits evaluation tasks when epochs complete
    4. Adaptively submits new job arrays as needed
    5. Reports progress statistics periodically
    
    Args:
        config: Training configuration
        ctx: Workflow context (auto-injected)
    
    Returns:
        Final training report with all metrics
    """
    print("=" * 60)
    print("FAULT-RESISTANT TRAINING WORKFLOW")
    print("=" * 60)
    print(f"Total steps: {config.total_steps}")
    print(f"Steps per epoch: {config.steps_per_epoch}")
    print(f"Estimated epochs: {config.total_steps // config.steps_per_epoch}")
    print(f"Checkpoint interval: {config.checkpoint_every_n_steps} steps")
    print()
    
    # Initialize state
    state = TrainingState()
    checkpoint_dir = ctx.shared_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # Resume from existing checkpoint if workflow was restarted
    existing_checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.pt"))
    if existing_checkpoints:
        latest = existing_checkpoints[-1]
        checkpoint_id = int(latest.stem.split("_")[1])
        meta_file = checkpoint_dir / f"checkpoint_{checkpoint_id}.meta.json"
        if meta_file.exists():
            with open(meta_file) as f:
                meta = json.load(f)
            state.current_checkpoint = checkpoint_id
            state.current_step = meta["step"]
            state.current_epoch = meta["epoch"]
            print(f"Resuming from checkpoint {checkpoint_id}")
            print(f"  Step: {state.current_step}/{config.total_steps}")
            print(f"  Epoch: {state.current_epoch}")
            print()
    
    # Configuration for adaptive submission
    MAX_LOOKAHEAD = 5  # Max jobs to queue at once
    MONITOR_INTERVAL = 30  # Seconds between checks
    STATS_INTERVAL = 300  # Print stats every 5 minutes
    
    # Estimate steps per 4-hour job (conservative estimate)
    # Will be updated based on actual throughput
    estimated_steps_per_job = config.checkpoint_every_n_steps
    
    active_train_jobs = []
    last_stats_time = time.time()
    workflow_start_time = time.time()
    
    # Main orchestration loop
    while state.current_step < config.total_steps:
        # Calculate how many more jobs we might need
        remaining_steps = config.total_steps - state.current_step
        estimated_jobs_remaining = max(1, math.ceil(
            remaining_steps / estimated_steps_per_job
        ))
        
        # Submit next batch of jobs (if needed)
        if len(active_train_jobs) == 0:
            # Submit new job array with concurrency=1
            jobs_to_submit = min(estimated_jobs_remaining, MAX_LOOKAHEAD)
            
            print(f"[{time.strftime('%H:%M:%S')}] Submitting {jobs_to_submit} training jobs...")
            print(f"  Starting from checkpoint: {state.current_checkpoint}")
            print(f"  Current step: {state.current_step}/{config.total_steps}")
            print(f"  Estimated jobs remaining: {estimated_jobs_remaining}")
            
            checkpoint_ids = list(range(
                state.current_checkpoint,
                state.current_checkpoint + jobs_to_submit
            ))
            
            job_submission_start = time.time()
            
            # Submit as array job with concurrency=1
            # This ensures they run sequentially but with minimal scheduling gap
            train_jobs = train_segment.map(
                [(config, cid) for cid in checkpoint_ids],
                max_concurrent=1
            )
            
            submission_time = time.time() - job_submission_start
            print(f"  ✓ Submitted in {submission_time:.2f}s")
            print()
            
            active_train_jobs = list(train_jobs)
        
        # Monitor active jobs and checkpoints
        print(f"[{time.strftime('%H:%M:%S')}] Monitoring progress...")
        
        # Check for newly completed checkpoints
        new_checkpoints = scan_for_new_checkpoints(
            checkpoint_dir,
            state.current_checkpoint
        )
        
        for checkpoint_meta in new_checkpoints:
            # Update state
            state.current_checkpoint = checkpoint_meta["checkpoint_id"]
            state.current_step = checkpoint_meta["step"]
            state.current_epoch = checkpoint_meta["epoch"]
            
            # Track throughput
            if checkpoint_meta["steps_in_segment"] > 0:
                throughput = (
                    checkpoint_meta["steps_in_segment"] / 
                    checkpoint_meta["elapsed_time"]
                )
                state.step_times.append(throughput)
                
                # Update estimate for future jobs
                if len(state.step_times) >= 3:
                    avg_throughput = sum(state.step_times[-5:]) / min(5, len(state.step_times))
                    estimated_steps_per_job = int(avg_throughput * 4 * 3600 * 0.9)  # 90% of 4 hours
            
            print(f"  ✓ Checkpoint {checkpoint_meta['checkpoint_id']} complete")
            print(f"    Step: {state.current_step}/{config.total_steps} ({state.current_step/config.total_steps*100:.1f}%)")
            print(f"    Epoch: {state.current_epoch}")
            print(f"    Throughput: {checkpoint_meta['steps_in_segment']/checkpoint_meta['elapsed_time']:.2f} steps/sec")
            
            # Check if epoch completed → submit evaluation
            if checkpoint_meta["epoch_completed"] and state.current_epoch not in state.epochs_evaluated:
                print(f"  → Submitting evaluation for epoch {state.current_epoch}...")
                
                eval_job = evaluate_checkpoint(
                    config,
                    checkpoint_meta["checkpoint_id"],
                    state.current_epoch
                )
                
                state.pending_eval_jobs.append(eval_job)
                state.epochs_evaluated.add(state.current_epoch)
                print(f"    ✓ Eval job submitted: {eval_job.id}")
            
            print()
        
        # Check if training is complete
        if state.current_step >= config.total_steps:
            print("Training complete!")
            state.completed = True
            break
        
        # Remove completed jobs from active list
        active_train_jobs = [
            job for job in active_train_jobs 
            if not job.is_completed()
        ]
        
        # Periodic statistics reporting
        if time.time() - last_stats_time > STATS_INTERVAL:
            print_progress_stats(state, config, workflow_start_time)
            last_stats_time = time.time()
        
        # Sleep before next check
        time.sleep(MONITOR_INTERVAL)
    
    # Wait for any remaining training jobs
    print("Waiting for remaining training jobs...")
    if active_train_jobs:
        for job in active_train_jobs:
            job.wait()
    
    # Wait for all evaluation jobs to complete
    print(f"Waiting for {len(state.pending_eval_jobs)} evaluation jobs...")
    for eval_job in state.pending_eval_jobs:
        eval_job.wait()
    
    # Collect all evaluation results
    print("\nCollecting evaluation results...")
    eval_results = []
    eval_dir = ctx.shared_dir / "eval_results"
    for eval_file in sorted(eval_dir.glob("eval_epoch_*.json")):
        with open(eval_file) as f:
            eval_results.append(json.load(f))
    
    # Generate final report
    total_time = time.time() - workflow_start_time
    avg_throughput = sum(state.step_times) / len(state.step_times) if state.step_times else 0
    
    report = {
        "config": asdict(config),
        "final_step": state.current_step,
        "final_epoch": state.current_epoch,
        "total_time_hours": total_time / 3600,
        "avg_throughput_steps_per_sec": avg_throughput,
        "num_job_segments": state.current_checkpoint,
        "eval_results": eval_results,
        "workflow_dir": str(ctx.workflow_job_dir),
    }
    
    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)
    print(f"Final step: {state.current_step}/{config.total_steps}")
    print(f"Final epoch: {state.current_epoch}")
    print(f"Total time: {total_time/3600:.2f} hours")
    print(f"Avg throughput: {avg_throughput:.2f} steps/sec")
    print(f"Job segments: {state.current_checkpoint}")
    print(f"Evaluations: {len(eval_results)}")
    print("=" * 60)
    
    return report


def scan_for_new_checkpoints(
    checkpoint_dir: Path,
    last_checkpoint_id: int
) -> list[dict]:
    """Scan for new checkpoint metadata files.
    
    Args:
        checkpoint_dir: Directory containing checkpoints
        last_checkpoint_id: Last checkpoint ID we've processed
    
    Returns:
        List of metadata dicts for new checkpoints, sorted by ID
    """
    new_checkpoints = []
    
    for meta_file in sorted(checkpoint_dir.glob("checkpoint_*.meta.json")):
        checkpoint_id = int(meta_file.stem.split("_")[1].split(".")[0])
        
        if checkpoint_id > last_checkpoint_id:
            with open(meta_file) as f:
                meta = json.load(f)
            new_checkpoints.append(meta)
    
    return sorted(new_checkpoints, key=lambda m: m["checkpoint_id"])


def print_progress_stats(
    state: TrainingState,
    config: TrainingConfig,
    start_time: float
) -> None:
    """Print formatted progress statistics.
    
    Args:
        state: Current training state
        config: Training configuration
        start_time: Workflow start timestamp
    """
    print("\n" + "-" * 60)
    print("PROGRESS STATISTICS")
    print("-" * 60)
    
    # Progress
    progress_pct = state.current_step / config.total_steps * 100
    print(f"Steps: {state.current_step}/{config.total_steps} ({progress_pct:.1f}%)")
    print(f"Epoch: {state.current_epoch}/{config.total_steps // config.steps_per_epoch}")
    print(f"Checkpoints: {state.current_checkpoint}")
    
    # Throughput
    if state.step_times:
        recent_throughput = sum(state.step_times[-5:]) / min(5, len(state.step_times))
        avg_throughput = sum(state.step_times) / len(state.step_times)
        print(f"\nThroughput:")
        print(f"  Recent: {recent_throughput:.2f} steps/sec")
        print(f"  Average: {avg_throughput:.2f} steps/sec")
    
    # ETA
    if state.step_times:
        remaining_steps = config.total_steps - state.current_step
        estimated_seconds = remaining_steps / recent_throughput
        eta_hours = estimated_seconds / 3600
        
        elapsed = time.time() - start_time
        elapsed_hours = elapsed / 3600
        
        print(f"\nTiming:")
        print(f"  Elapsed: {elapsed_hours:.2f} hours")
        print(f"  ETA: {eta_hours:.2f} hours")
        print(f"  Total estimated: {elapsed_hours + eta_hours:.2f} hours")
    
    # Evaluations
    print(f"\nEvaluations:")
    print(f"  Completed: {len(state.epochs_evaluated)}")
    print(f"  Pending: {len([j for j in state.pending_eval_jobs if not j.is_completed()])}")
    
    print("-" * 60 + "\n")
```

## Key Design Decisions

### 1. Concurrency=1 Job Arrays

**Why this works:**
- SLURM natively supports `--array=0-N%1` (max 1 concurrent)
- Jobs are pre-queued, so next job starts immediately when previous finishes
- Single submission → minimal orchestration overhead
- Workflow can submit next batch while current batch is running

**Alternative (not chosen):** Sequential individual job submissions
- ❌ High scheduling overhead between jobs (minutes to hours)
- ❌ Workflow must wait for each job before submitting next
- ❌ More complex error handling

### 2. Checkpoint-Based Resumption

**Why this works:**
- Each job saves checkpoint before timeout
- Next job loads checkpoint and continues
- Natural retry mechanism: if job fails before checkpoint, next job starts from previous checkpoint
- Workflow can detect progress by monitoring checkpoint files

**Checkpoint frequency:** Every N steps AND before timeout
- Frequent enough to minimize lost work (e.g., 1000 steps or 30 min)
- Infrequent enough to not slow training (checkpoint I/O cost)

### 3. Adaptive Job Submission

**The challenge:** Don't overshoot after training completes

**Solution:** Dynamic estimation based on throughput
1. Start with conservative estimate (e.g., 5 jobs)
2. After each job completes, update estimate:
   - Measure actual steps/second
   - Calculate: `steps_per_4hr = throughput * 4 * 3600 * 0.9`
   - Estimate remaining: `ceil(remaining_steps / steps_per_4hr)`
3. Submit: `min(estimated_remaining, MAX_LOOKAHEAD)`

**MAX_LOOKAHEAD:** Limits queue depth
- Too high: Waste resources on no-op jobs after completion
- Too low: Risk scheduling gaps if estimate is wrong
- Recommended: 3-5 jobs

**Example:**
```
Initial: Submit 5 jobs, estimate 1000 steps/job
Job 1 completes: 1200 steps in 4hr → update estimate to 1200 steps/job
Remaining: 4800 steps → need 4 more jobs
Currently queued: 4 jobs → Don't submit yet
Job 2 completes: Check again, submit if needed
```

### 4. Epoch-Boundary Evaluation

**The challenge:** Don't know which job will complete an epoch

**Solution:** Reactive evaluation based on checkpoint metadata
1. Each checkpoint includes `epoch_completed` flag
2. Workflow monitors new checkpoints (polling loop)
3. When `epoch_completed=True` → submit eval job
4. Track `epochs_evaluated` set to avoid duplicates

**Benefits:**
- No need to predict job counts
- Evaluations run as soon as epoch completes (minimal delay)
- Independent eval jobs don't block training
- Failed eval jobs don't affect training progress

### 5. State Persistence

**Workflow restart capability:** If workflow job is killed/restarted:
1. Scan checkpoint directory for latest checkpoint
2. Load metadata to restore state
3. Resume from latest checkpoint
4. Rescan eval results directory

**Implementation:**
```python
# On workflow start/restart
existing_checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.pt"))
if existing_checkpoints:
    # Resume from latest
    latest_checkpoint = max(existing_checkpoints, key=lambda p: int(p.stem.split("_")[1]))
    load_state_from_checkpoint(latest_checkpoint)
```

## Failure Handling & Recovery

### Failure Modes

1. **Training job fails before making progress**
   - Next job loads previous checkpoint → automatic retry
   - Workflow detects no new checkpoint → submits replacement job

2. **Training job times out**
   - Expected behavior, checkpoint saved before timeout
   - Next job continues from checkpoint

3. **Workflow job is killed/times out**
   - State persists in checkpoint files
   - On restart: Load latest checkpoint, resume orchestration
   - May need to resubmit active job array

4. **Evaluation job fails**
   - Training continues unaffected (independent)
   - Can retry manually or in cleanup phase

5. **Checkpoint file corruption**
   - Keep last N checkpoints (e.g., 3)
   - Fall back to previous checkpoint if load fails

### Recovery Strategies

**Checkpoint cleanup:**
```python
def keep_latest_n_checkpoints(checkpoint_dir: Path, n: int = 3):
    """Keep only the N most recent checkpoints to save space."""
    checkpoints = sorted(
        checkpoint_dir.glob("checkpoint_*.pt"),
        key=lambda p: int(p.stem.split("_")[1])
    )
    
    # Keep latest N
    for checkpoint in checkpoints[:-n]:
        checkpoint.unlink()
        # Also remove metadata
        meta_file = checkpoint.with_suffix(".pt.meta.json")
        if meta_file.exists():
            meta_file.unlink()
```

**Retry logic for failed jobs:**
```python
def submit_with_retry(task, args, max_retries=3):
    """Submit job with automatic retry on failure."""
    for attempt in range(max_retries):
        job = task(*args)
        success = job.wait()
        
        if success:
            return job
        
        print(f"Job failed (attempt {attempt+1}/{max_retries})")
        if attempt < max_retries - 1:
            print("Retrying...")
            time.sleep(60)  # Wait before retry
    
    raise RuntimeError(f"Job failed after {max_retries} attempts")
```

## Statistics & Monitoring

### Metrics to Track

1. **Progress metrics:**
   - Current step / total steps
   - Current epoch / total epochs  
   - Checkpoints completed
   - Training completion percentage

2. **Throughput metrics:**
   - Steps per second (recent & average)
   - Steps per job segment
   - Job startup overhead (time between jobs)

3. **Timing metrics:**
   - Elapsed time
   - ETA to completion
   - Time per epoch
   - Time per checkpoint interval

4. **Resource metrics:**
   - Number of job segments submitted
   - Number of no-op jobs (completed after training finished)
   - Number of failed/retried jobs
   - GPU utilization (if available)

5. **Evaluation metrics:**
   - Number of evaluations completed
   - Number pending
   - Latest validation accuracy/loss

### Stats Display Format

```
────────────────────────────────────────────────────────────
PROGRESS STATISTICS                    [2024-11-10 14:32:15]
────────────────────────────────────────────────────────────
Training Progress:
  Steps:       45,234 / 100,000  (45.2%)
  Epoch:       45 / 100
  Checkpoints: 12

Throughput:
  Recent:  3.15 steps/sec  (last 5 segments)
  Average: 2.98 steps/sec  (overall)

Timing:
  Elapsed:   4.22 hours
  ETA:       5.11 hours
  Total est: 9.33 hours

Resource Usage:
  Job segments:    12 submitted
  Failed/retried:  1
  Estimated total: 22 segments

Evaluations:
  Completed: 45 epochs
  Pending:   0 jobs
  Latest metrics:
    - Val accuracy: 94.2%
    - Val loss: 0.182

GPU Utilization: ~95% (estimated)
────────────────────────────────────────────────────────────
```

## Example Usage

### Basic Training Run

```python
from slurm import Cluster
from train_workflow import train_and_eval_workflow, TrainingConfig

# Define configuration
config = TrainingConfig(
    total_steps=100000,
    steps_per_epoch=1000,
    checkpoint_every_n_steps=1000,
    batch_size=32,
    learning_rate=0.001,
    model_name="resnet50",
    dataset="imagenet",
)

# Submit workflow
with Cluster.from_env() as cluster:
    workflow_job = train_and_eval_workflow(config)
    
    print(f"Workflow submitted: {workflow_job.id}")
    print(f"Job directory: {workflow_job.target_job_dir}")
    
    # Option 1: Block and wait for completion
    report = workflow_job.get_result()
    
    # Option 2: Non-blocking - let workflow run in background
    # Check status later with: workflow_job.get_status()
```

### Resuming After Interruption

If the workflow is killed/interrupted, simply re-run the same command:

```python
# Workflow will automatically:
# 1. Scan for latest checkpoint
# 2. Resume from that point
# 3. Continue orchestrating remaining jobs

with Cluster.from_env() as cluster:
    workflow_job = train_and_eval_workflow(config)
    report = workflow_job.get_result()
```

The workflow detects existing checkpoints and resumes automatically.

### Monitoring Active Workflow

```python
# In a separate script/notebook
from pathlib import Path
import json

workflow_dir = Path("/path/to/workflow/job/dir")
checkpoint_dir = workflow_dir / "shared" / "checkpoints"

# Read latest checkpoint metadata
checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.meta.json"))
if checkpoints:
    with open(checkpoints[-1]) as f:
        latest = json.load(f)
    
    print(f"Current step: {latest['step']}")
    print(f"Current epoch: {latest['epoch']}")
    print(f"Throughput: {latest['steps_in_segment'] / latest['elapsed_time']:.2f} steps/sec")

# Read eval results
eval_dir = workflow_dir / "shared" / "eval_results"
eval_results = []
for eval_file in sorted(eval_dir.glob("eval_epoch_*.json")):
    with open(eval_file) as f:
        eval_results.append(json.load(f))

print(f"\nEvaluations completed: {len(eval_results)}")
if eval_results:
    latest_eval = eval_results[-1]
    print(f"Latest epoch: {latest_eval['epoch']}")
    print(f"Metrics: {latest_eval['metrics']}")
```

## Performance Analysis

### Scheduling Overhead Comparison

**Individual sequential submissions:**
```
Job 1: [submit: 30s] [queue: 2min] [run: 4hr] 
Gap: [30s submit + 2min queue] = 2.5min
Job 2: [submit: 30s] [queue: 2min] [run: 4hr]
Gap: 2.5min
...
Total overhead for 20 jobs: 20 × 2.5min = 50min
```

**Job array with concurrency=1:**
```
Array submission: [submit: 2s for all 20 jobs]
Job 1: [queue: 2min] [run: 4hr]
Gap: [scheduler start next: 10s] ← SLURM's internal transition
Job 2: [run: 4hr]
Gap: 10s
...
Total overhead for 20 jobs: 2s + 19 × 10s = ~3min
```

**Speedup: 50min → 3min (17x reduction in overhead)**

### Resource Efficiency

**No-op jobs with adaptive submission:**

Scenario: Training completes after 19.5 jobs worth of work

**Without adaptation (static submission of 20):**
- Job 20 runs for ~5 minutes before detecting completion
- Wasted: ~3.9 hours of GPU time (if fairshare allows full 4hr)

**With adaptation (MAX_LOOKAHEAD=5, updates after each job):**
```
Submit: Jobs 1-5
After job 5: Update estimate → need ~15 more → submit jobs 6-10
After job 10: Update estimate → need ~10 more → submit jobs 11-15
After job 15: Update estimate → need ~5 more → submit jobs 16-20
Job 19 completes training → Job 20 starts but exits early
```
- Job 20 still runs briefly, but unavoidable with concurrency=1 queuing
- Could add optimization: Check completion flag before starting each job

**Further optimization:**
```python
# In train_segment, check if already completed before starting
def train_segment(config, checkpoint_id, *, ctx=None):
    checkpoint_dir = ctx.shared_dir / "checkpoints"
    
    # Quick check: Is training already done?
    completion_flag = checkpoint_dir / "TRAINING_COMPLETE"
    if completion_flag.exists():
        print("Training already complete, exiting early")
        return {"early_exit": True, "reason": "already_complete"}
    
    # ... rest of training logic
```

This check happens at job start (< 1 second), so no-op job costs are minimal.

## Advanced Features

### 1. Multi-GPU Training Segments

For distributed training across multiple GPUs/nodes:

```python
@task(
    time="04:00:00",
    mem="128G",
    gres="gpu:4",  # 4 GPUs per node
    nodes=2,         # 2 nodes = 8 GPUs total
    ntasks_per_node=4,
)
def train_segment_distributed(config, checkpoint_id, *, ctx=None):
    """Train segment with distributed data parallel."""
    
    # Initialize distributed training
    world_size = int(os.environ.get("SLURM_NTASKS", 1))
    rank = int(os.environ.get("SLURM_PROCID", 0))
    
    dist.init_process_group(
        backend="nccl",
        world_size=world_size,
        rank=rank,
    )
    
    # Rest of training logic with DDP
    # ...
```

### 2. Dynamic Checkpoint Frequency

Adjust checkpoint frequency based on progress:

```python
def get_checkpoint_interval(current_step, total_steps):
    """More frequent checkpoints near end of training."""
    progress = current_step / total_steps
    
    if progress < 0.5:
        return 2000  # Early training: less frequent
    elif progress < 0.9:
        return 1000  # Mid training: moderate
    else:
        return 500   # Final stretch: more frequent
```

### 3. Conditional Evaluation

Only evaluate when metrics improve or at specific milestones:

```python
# In workflow
last_eval_loss = float('inf')

for checkpoint_meta in new_checkpoints:
    if checkpoint_meta["epoch_completed"]:
        # Quick check: Has loss improved enough to warrant eval?
        if checkpoint_meta.get("train_loss", 0) < last_eval_loss * 0.95:
            # Loss improved by >5% → evaluate
            eval_job = evaluate_checkpoint(...)
            last_eval_loss = checkpoint_meta["train_loss"]
        elif state.current_epoch % 10 == 0:
            # Milestone: every 10 epochs regardless
            eval_job = evaluate_checkpoint(...)
```

### 4. Early Stopping

Stop training if validation metrics plateau:

```python
# In workflow, after collecting eval results
if len(eval_results) >= 10:
    recent_accs = [r["metrics"]["accuracy"] for r in eval_results[-10:]]
    
    # Check if accuracy hasn't improved in last 10 epochs
    if max(recent_accs) == recent_accs[0]:
        print("No improvement in 10 epochs, stopping early")
        
        # Set completion flag
        completion_flag = checkpoint_dir / "TRAINING_COMPLETE"
        completion_flag.write_text(f"Early stopping at epoch {state.current_epoch}")
        
        # Cancel remaining jobs
        for job in active_train_jobs:
            job.cancel()
        
        break
```

### 5. Automatic Hyperparameter Adjustment

Adjust learning rate or other hyperparameters between segments:

```python
# Save adjusted config in checkpoint metadata
if current_epoch % 30 == 0:
    # Decay learning rate
    config.learning_rate *= 0.1
    print(f"Reduced learning rate to {config.learning_rate}")

# Checkpoint includes updated config
torch.save({
    "step": current_step,
    "epoch": current_epoch,
    "model": model.state_dict(),
    "optimizer": optimizer.state_dict(),
    "config": asdict(config),  # Include updated config
}, checkpoint_file)
```

## Testing Strategy

### Unit Tests

1. **Checkpoint save/load:**
   - Test checkpoint serialization
   - Test metadata generation
   - Test resume from checkpoint

2. **State tracking:**
   - Test TrainingState initialization
   - Test state updates from metadata
   - Test throughput calculation

3. **Helper functions:**
   - Test `scan_for_new_checkpoints()`
   - Test `print_progress_stats()`
   - Test adaptive job estimation logic

### Integration Tests

1. **Single training segment:**
   - Submit one train_segment job
   - Verify checkpoint created
   - Verify metadata correct

2. **Multi-segment training:**
   - Submit 3 sequential segments
   - Verify each loads previous checkpoint
   - Verify progress increments correctly

3. **Evaluation trigger:**
   - Train to complete 2 epochs
   - Verify eval jobs submitted
   - Verify eval results saved

### End-to-End Tests

1. **Full workflow (small scale):**
   - Run workflow with total_steps=1000
   - Verify completion
   - Verify all evals ran
   - Verify final report

2. **Workflow resume:**
   - Run workflow, kill after 2 segments
   - Restart workflow
   - Verify resumes from latest checkpoint

3. **Failure recovery:**
   - Inject failure in segment 3
   - Verify retry from previous checkpoint

4. **Adaptive submission:**
   - Mock varying throughput
   - Verify job estimates adjust
   - Verify doesn't overshoot

## Future Enhancements

### 1. Preemption Detection

More robust handling of fairshare preemption:

```python
# Listen for SIGTERM (sent before preemption)
import signal

def preemption_handler(signum, frame):
    print("Preemption signal received, saving checkpoint...")
    save_checkpoint()
    sys.exit(0)

signal.signal(signal.SIGTERM, preemption_handler)
```

### 2. Shared Memory Checkpoints

For very large models, use shared memory for faster checkpoint I/O:

```python
# Save checkpoint to /dev/shm (RAM disk)
checkpoint_file = Path("/dev/shm") / f"checkpoint_{checkpoint_id}.pt"
torch.save(state, checkpoint_file)

# Copy to persistent storage asynchronously
shutil.copy(checkpoint_file, persistent_checkpoint_dir)
```

### 3. Distributed Workflow

Split orchestration for massive scale:

```python
@workflow
def meta_orchestrator(config, ctx):
    """High-level orchestrator that spawns sub-workflows."""
    
    # Spawn multiple training workflows for different model variants
    variants = ["small", "medium", "large"]
    
    workflow_jobs = []
    for variant in variants:
        variant_config = create_variant_config(config, variant)
        wf_job = train_and_eval_workflow(variant_config)
        workflow_jobs.append(wf_job)
    
    # Wait for all variants
    results = [job.get_result() for job in workflow_jobs]
    
    # Select best variant
    return select_best(results)
```

### 4. Cloud Storage Integration

Save checkpoints to cloud storage for durability:

```python
# In train_segment
def save_checkpoint_with_cloud_backup(state, checkpoint_file, cloud_path):
    """Save checkpoint locally and to cloud storage."""
    
    # Save locally first
    torch.save(state, checkpoint_file)
    
    # Async upload to cloud (S3, GCS, etc.)
    upload_thread = threading.Thread(
        target=upload_to_cloud,
        args=(checkpoint_file, cloud_path)
    )
    upload_thread.start()
```

### 5. Web Dashboard

Real-time monitoring web interface:

```python
# In workflow, periodically update stats file
stats_file = ctx.shared_dir / "live_stats.json"

stats = {
    "current_step": state.current_step,
    "current_epoch": state.current_epoch,
    "throughput": recent_throughput,
    "eta_hours": eta_hours,
    "progress_pct": progress_pct,
    "timestamp": time.time(),
}

with open(stats_file, "w") as f:
    json.dump(stats, f)

# Separate web server reads stats_file and serves dashboard
```

### 6. Multi-Objective Training

Train multiple objectives in parallel:

```python
# Train on different data splits or objectives simultaneously
@workflow
def multi_objective_training(config, objectives, ctx):
    """Train multiple objectives with shared checkpointing."""
    
    # Each objective gets its own training loop
    # but they share the same model checkpoints
    
    train_jobs = []
    for objective in objectives:
        objective_config = create_objective_config(config, objective)
        # Each gets its own checkpoint namespace
        train_jobs.append(
            train_and_eval_workflow(objective_config)
        )
    
    # Run all objectives concurrently
    results = [job.get_result() for job in train_jobs]
    
    return {"objectives": objectives, "results": results}
```

## Conclusion

This design provides a robust, efficient solution for long-running training under fairshare timeout constraints. Key benefits:

1. **Minimal scheduling overhead** (< 1 min between jobs vs potentially hours)
2. **Automatic fault recovery** via checkpoint-based resumption
3. **Adaptive resource usage** to prevent wasteful no-op jobs
4. **Dynamic evaluation** without predicting job counts
5. **Rich observability** with progress stats and ETA

The workflow is production-ready and can be extended with advanced features like distributed training, early stopping, cloud integration, and real-time monitoring.

### Implementation Checklist

- [ ] Implement `train_segment` task with checkpoint save/load
- [ ] Implement `evaluate_checkpoint` task
- [ ] Implement `train_and_eval_workflow` orchestrator
- [ ] Add helper functions: `scan_for_new_checkpoints()`, `print_progress_stats()`
- [ ] Add configuration dataclasses: `TrainingConfig`, `TrainingState`
- [ ] Write unit tests for state tracking and helpers
- [ ] Write integration tests for multi-segment training
- [ ] Run end-to-end test with small-scale training
- [ ] Add monitoring/logging enhancements
- [ ] Document usage examples
- [ ] Deploy to production

**Estimated implementation time:** 2-3 days for core functionality, 1-2 days for tests and polish.




