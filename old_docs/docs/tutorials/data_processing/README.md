# Data Processing Tutorial

This tutorial demonstrates how to use the SLURM SDK for scalable data processing workflows. You'll learn how to:
- Process large datasets in parallel on SLURM clusters
- Chain multiple processing steps
- Handle intermediate results and checkpointing
- Use the `output_dir` feature for storing processed data

## What you'll learn

- Writing data processing tasks with `@task` decorator
- Using `JobContext.output_dir` for storing results
- Chaining jobs to create multi-stage pipelines
- Efficient data transfer between local and cluster storage
- Handling large files and datasets

## Files in this tutorial

```
data_processing/
├── README.md              # This file
├── process.py             # Main data processing script
├── pyproject.toml         # Project dependencies
├── Slurmfile.toml         # Cluster configuration
└── sample_data/           # Sample input data
    └── books.txt          # Sample text corpus
```

## Prerequisites

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create project and install dependencies
uv sync
```

## Quick start

### 1. Process data locally

```bash
# Run single-stage processing
uv run python process.py --input sample_data/books.txt --output ./output

# Run full pipeline locally
uv run python process.py --pipeline --input sample_data/books.txt --output ./output
```

### 2. Submit to SLURM cluster

```bash
# Submit single processing job
uv run python process.py --submit --env production --input /cluster/data/large_corpus.txt

# Submit full pipeline (multiple chained jobs)
uv run python process.py --submit-pipeline --env production --input /cluster/data/large_corpus.txt
```

## Tutorial stages

### Stage 1: Text tokenization and cleaning

Process raw text files into cleaned, tokenized data:

```python
@task(time="00:30:00", mem="4G", cpus_per_task=4)
def tokenize_text(
    input_path: str,
    *,
    job: JobContext | None = None
) -> str:
    \"\"\"Tokenize and clean input text file.\"\"\"

    # Read input
    with open(input_path) as f:
        text = f.read()

    # Process text
    tokens = simple_tokenize(text)

    # Save to output_dir if running on cluster
    if job and job.output_dir:
        output_path = job.output_dir / "tokens.json"
        save_tokens(tokens, output_path)
        return str(output_path)
    else:
        # Local execution - save to current dir
        output_path = Path("./output/tokens.json")
        output_path.parent.mkdir(exist_ok=True)
        save_tokens(tokens, output_path)
        return str(output_path)
```

### Stage 2: Feature extraction

Extract features from tokenized data:

```python
@task(time="01:00:00", mem="8G", cpus_per_task=8)
def extract_features(
    tokens_path: str,
    *,
    job: JobContext | None = None
) -> str:
    \"\"\"Extract features from tokenized data.\"\"\"

    # Load tokens
    tokens = load_tokens(tokens_path)

    # Extract features (TF-IDF, word embeddings, etc.)
    features = compute_features(tokens)

    # Save features
    if job and job.output_dir:
        output_path = job.output_dir / "features.npz"
    else:
        output_path = Path("./output/features.npz")
        output_path.parent.mkdir(exist_ok=True)

    save_features(features, output_path)
    return str(output_path)
```

### Stage 3: Data aggregation and statistics

Compute statistics and aggregate processed data:

```python
@task(time="00:15:00", mem="2G")
def compute_statistics(
    features_path: str,
    *,
    job: JobContext | None = None
) -> dict:
    \"\"\"Compute statistics from extracted features.\"\"\"

    # Load features
    features = load_features(features_path)

    # Compute statistics
    stats = {
        "num_samples": len(features),
        "feature_dim": features.shape[1],
        "mean": float(features.mean()),
        "std": float(features.std()),
    }

    # Save statistics
    if job and job.output_dir:
        stats_path = job.output_dir / "stats.json"
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)

    return stats
```

## Chaining jobs

The tutorial shows how to chain jobs manually using `job.wait()`:

```python
def run_pipeline(input_path: str, cluster: Cluster):
    \"\"\"Run full data processing pipeline.\"\"\"

    print("Stage 1: Tokenization")
    job1 = cluster.submit(tokenize_text)(input_path)
    job1.wait()
    tokens_path = job1.get_result()

    print(f"Stage 2: Feature extraction (input: {tokens_path})")
    job2 = cluster.submit(extract_features)(tokens_path)
    job2.wait()
    features_path = job2.get_result()

    print(f"Stage 3: Statistics (input: {features_path})")
    job3 = cluster.submit(compute_statistics)(features_path)
    job3.wait()
    stats = job3.get_result()

    print("Pipeline complete!")
    print(f"Statistics: {stats}")

    return stats
```

## Working with large files

### Efficient data transfer

When working with large datasets, consider:

1. **Use cluster-local paths** - Process data directly on cluster storage:
   ```python
   # Don't download entire dataset
   input_path = "/cluster/scratch/data/large_file.parquet"

   job = cluster.submit(process_data)(input_path)
   ```

2. **Stream processing** - Process data in chunks:
   ```python
   def process_large_file(input_path: str, chunk_size: int = 10000):
       for chunk in pd.read_csv(input_path, chunksize=chunk_size):
           process_chunk(chunk)
   ```

3. **Use appropriate packaging** - For large dependencies, use container packaging:
   ```toml
   [production.packaging]
   type = "container"
   dockerfile = "Dockerfile"
   ```

### Checkpointing

Save intermediate results to resume processing:

```python
@task(time="04:00:00", mem="16G")
def process_with_checkpoints(
    input_path: str,
    *,
    job: JobContext | None = None
) -> str:
    \"\"\"Process data with checkpointing.\"\"\"

    checkpoint_dir = job.output_dir / "checkpoints" if job else Path("./checkpoints")
    checkpoint_dir.mkdir(exist_ok=True)

    # Check for existing checkpoint
    checkpoint_file = checkpoint_dir / "progress.json"
    if checkpoint_file.exists():
        with open(checkpoint_file) as f:
            progress = json.load(f)
        start_idx = progress["last_processed"]
    else:
        start_idx = 0

    # Process data
    for i in range(start_idx, total_items):
        process_item(i)

        # Save checkpoint every 1000 items
        if i % 1000 == 0:
            with open(checkpoint_file, "w") as f:
                json.dump({"last_processed": i}, f)

    return "processing complete"
```

## Advanced patterns

### Parallel processing with multiple jobs

Submit multiple independent jobs for embarrassingly parallel tasks:

```python
def process_multiple_files(file_list: list[str], cluster: Cluster):
    \"\"\"Process multiple files in parallel.\"\"\"

    jobs = []
    for file_path in file_list:
        job = cluster.submit(process_single_file)(file_path)
        jobs.append(job)

    # Wait for all jobs
    for job in jobs:
        job.wait()

    # Collect results
    results = [job.get_result() for job in jobs]

    return results
```

### Using scratch space

Leverage fast local scratch storage on compute nodes:

```python
@task(time="02:00:00", mem="32G")
def process_with_scratch(
    input_path: str,
    *,
    job: JobContext | None = None
) -> str:
    \"\"\"Use scratch space for intermediate files.\"\"\"

    # Use cluster scratch if available
    scratch = Path(os.environ.get("SCRATCH", "/tmp"))

    # Copy input to scratch
    local_input = scratch / "input_data.parquet"
    shutil.copy(input_path, local_input)

    # Process using fast local storage
    result = process_data(local_input)

    # Save final result to job output directory
    output_path = job.output_dir / "result.parquet" if job else Path("./result.parquet")
    result.to_parquet(output_path)

    return str(output_path)
```

## Configuration tips

### Slurmfile settings for data processing

```toml
[production.cluster]
backend = "ssh"
job_base_dir = "/cluster/scratch/$USER/slurm_jobs"  # Use scratch for temp files

[production.packaging]
type = "wheel"  # Fast packaging for pure Python code
python_executable = "/usr/bin/python3"

[production.submit]
partition = "cpu"  # CPU partition for data processing
account = "data-science"
```

### Resource allocation

Choose resources based on your data size:

- **Small datasets (< 1GB)**: `mem="4G"`, `cpus_per_task=2`, `time="00:30:00"`
- **Medium datasets (1-10GB)**: `mem="16G"`, `cpus_per_task=8`, `time="02:00:00"`
- **Large datasets (> 10GB)**: `mem="64G"`, `cpus_per_task=16`, `time="08:00:00"`

## Troubleshooting

### Out of memory errors
- Increase `mem` parameter in `@task` decorator
- Process data in chunks instead of loading entire dataset
- Use more efficient data formats (Parquet instead of CSV)

### Slow data transfer
- Use cluster-local paths instead of copying from login nodes
- Consider using `rsync` or parallel transfer tools for large files
- Package only code, not data, in containers

### Job timeouts
- Increase `time` parameter
- Add checkpointing to resume from interruptions
- Split large jobs into smaller chunks

## Next steps

- See [guides/job_context.md](../../docs/guides/job_context.md) for more on `output_dir`
- See [guides/local_cluster_testing.md](../../docs/guides/local_cluster_testing.md) for testing patterns
- Check [examples/chain_jobs.py](../../src/slurm/examples/chain_jobs.py) for job chaining examples
