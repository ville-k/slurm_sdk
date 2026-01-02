# New Examples Summary

## Created Examples

### 1. `dependent_jobs.py` - Submitless Execution with Dependencies

**Purpose**: Demonstrates the modern submitless execution API where tasks return Jobs when called within a cluster context.

**Key Features Demonstrated**:
- ✅ Context manager usage (`with Cluster.from_env() as cluster:`)
- ✅ Automatic dependency detection (passing Job as argument)
- ✅ Sequential pipeline with automatic chaining
- ✅ Container packaging
- ✅ Result retrieval via `.get_result()`
- ✅ JobContext injection

**Pipeline Flow**:
```
generate_data → preprocess_data → train_model → evaluate_model → generate_report
                                                             ↘_______________↗
```

**Usage**:
```bash
uv run python -m slurm.examples.dependent_jobs --env local --dataset-size 1000 --learning-rate 0.001
```

**Code Highlights**:
```python
with Cluster.from_env() as cluster:
    # Calling task returns Job (Future)
    data_job = generate_data(1000)
    
    # Pass Job as argument - automatic dependency!
    preprocess_job = preprocess_data(data_job)
    
    # Chain continues
    model_job = train_model(preprocess_job, 0.001)
    eval_job = evaluate_model(model_job, test_size=200)
    
    # Multiple dependencies
    report_job = generate_report(model_job, eval_job)
    
    # Get result triggers submission and waits
    result = report_job.get_result()
```

---

### 2. `ml_workflow.py` - Workflow Orchestration with Arrays

**Purpose**: Demonstrates advanced workflow features including array jobs, parallel execution, and conditional logic.

**Key Features Demonstrated**:
- ✅ `@workflow` decorator
- ✅ `WorkflowContext` parameter injection
- ✅ Array jobs via `.map()` for parallel execution
- ✅ Automatic dependency tracking
- ✅ Conditional logic based on results
- ✅ Shared directory concepts (`ctx.shared_dir`)
- ✅ Multi-stage pipeline with mixed parallel/sequential execution

**Pipeline Flow**:
```
prepare_dataset
      ↓
train_model.map([config1, config2, config3, ...])  ← Parallel array job
      ↓           ↓           ↓           ↓
eval_model.map([job1, job2, job3, ...])           ← Parallel array job
      ↓
select_best_model (conditional logic)
      ↓
final_validation (conditional - only if threshold met)
```

**Usage**:
```bash
uv run python -m slurm.examples.ml_workflow --env local --dataset mnist --num-configs 4
```

**Code Highlights**:
```python
@workflow(time="01:00:00", job_name="ml_hyperparameter_search")
def hyperparameter_search_workflow(
    dataset_name: str,
    configs: list[dict],
    ctx: WorkflowContext,  # Auto-injected!
) -> dict:
    # Stage 1: Prepare shared dataset
    dataset_job = prepare_dataset(dataset_name)
    dataset = dataset_job.get_result()
    
    # Stage 2: Train multiple models in parallel (array job)
    training_inputs = [(config, dataset) for config in configs]
    train_jobs = train_model.map(training_inputs)
    
    # Stage 3: Evaluate all models in parallel (automatic dependencies)
    eval_inputs = [(train_job, "test") for train_job in train_jobs]
    eval_jobs = evaluate_model.map(eval_inputs)
    
    # Stage 4: Collect results and apply logic
    eval_results = eval_jobs.get_results()
    best_idx = max(range(len(eval_results)), 
                   key=lambda i: eval_results[i]["test_accuracy"])
    
    # Stage 5: Conditional execution
    if eval_results[best_idx]["test_accuracy"] > 0.85:
        final_results = final_validation(configs[best_idx], "comprehensive").get_result()
    
    return {"best_model": best_idx, "results": eval_results}

# Execute workflow
with Cluster.from_env() as cluster:
    workflow_job = hyperparameter_search_workflow("mnist", configs)
    report = workflow_job.get_result()
```

---

## Implementation Details

### Container Packaging

Both examples use container packaging (like `hello_container.py`):
- Each example expects a `Slurmfile.container_example.toml` configuration
- Uses Podman/Docker for reproducible execution environments
- Tasks run in isolated containers on the cluster

### Self-Contained Design

Both examples are fully self-contained:
- ✅ Fake/simulated processing (no external dependencies)
- ✅ Complete pipelines from start to finish
- ✅ Rich console output with progress indicators
- ✅ Comprehensive help text and CLI arguments
- ✅ Clear documentation and comments

### Design Patterns Demonstrated

1. **Submitless Execution**: Tasks called directly within context manager
2. **Automatic Dependencies**: Jobs passed as arguments create dependencies
3. **Array Jobs**: `.map()` for parallel execution over lists
4. **Context Injection**: `JobContext` and `WorkflowContext` auto-injected
5. **Conditional Logic**: Workflow decisions based on intermediate results
6. **Result Collection**: `.get_results()` for array jobs, `.get_result()` for single jobs

---

## API Coverage

### From submitless_execution_design.md

| Feature | dependent_jobs.py | ml_workflow.py |
|---------|-------------------|----------------|
| Context manager (`with Cluster.from_env()`) | ✅ | ✅ |
| Submitless task calling | ✅ | ✅ |
| Automatic dependencies (Job args) | ✅ | ✅ |
| `.after()` explicit dependencies | - | - |
| Array jobs with `.map()` | - | ✅ |
| `@workflow` decorator | - | ✅ |
| `WorkflowContext` injection | - | ✅ |
| Conditional logic | - | ✅ |
| `.unwrapped` for testing | ✅ | ✅ |
| Container packaging | ✅ | ✅ |

---

## Testing

Both examples have been tested for:
- ✅ Import successfully
- ✅ CLI argument parsing
- ✅ Help text generation
- ✅ Code style (no linter errors)

To verify:
```bash
# Test imports
uv run python -c "from slurm.examples import dependent_jobs, ml_workflow"

# Test CLI
uv run python -m slurm.examples.dependent_jobs --help
uv run python -m slurm.examples.ml_workflow --help
```

---

## Integration with Existing Examples

These examples complement the existing example suite:

| Example | Focus | Level |
|---------|-------|-------|
| `hello_world.py` | Basic submission | Beginner |
| `hello_container.py` | Container packaging | Beginner |
| **`dependent_jobs.py`** | **Submitless + dependencies** | **Intermediate** |
| **`ml_workflow.py`** | **Workflow orchestration** | **Advanced** |
| `distributed_context.py` | Multi-node distributed | Advanced |
| `chain_jobs.py` | Job chaining patterns | Intermediate |

---

## Next Steps

Users learning the SDK progression:
1. Start with `hello_world.py` or `hello_container.py`
2. Move to `dependent_jobs.py` for submitless execution
3. Study `ml_workflow.py` for workflow orchestration
4. Explore `distributed_context.py` for multi-node scenarios

These examples serve as templates for real-world ML pipelines, data processing workflows, and parallel computing tasks.

