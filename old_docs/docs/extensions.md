# Extensions

The Slurm SDK provides several optional extensions that enhance functionality with profiling, debugging, and distributed training capabilities.

## Built-in Extensions

### CProfilerCallback

CPU profiling using Python's built-in `cProfile` module. No additional dependencies required.

```python
from slurm import task
from slurm.callbacks import CProfilerCallback

@task(callbacks=[CProfilerCallback(
    print_stats=True,
    sort_by='cumulative',
    top_n=20
)])
def cpu_intensive_task():
    # Your code here
    pass
```

**Options:**
- `output_dir`: Directory to save profiling data
- `output_filename`: Name of the profile data file (default: `profile.prof`)
- `print_stats`: Print statistics after execution
- `sort_by`: Sort criterion for stats (`cumulative`, `time`, `calls`)
- `top_n`: Number of top entries to show

## Optional Extensions

These require additional dependencies. Install them using:

```bash
# For PyTorch support
pip install slurm-sdk[torch]
# or
uv add --group torch slurm-sdk

# For memory profiling
pip install slurm-sdk[memray]
# or
uv add --group memray slurm-sdk

# For debugger support
pip install slurm-sdk[debugger]
# or
uv add --group debugger slurm-sdk

# Install all extras
pip install slurm-sdk[torch,memray,debugger]
```

### PyTorch Extensions

#### TorchProfilerCallback

Profile PyTorch operations including CUDA and memory usage.

```python
from slurm import task
from slurm.extras.torch import TorchProfilerCallback

@task(callbacks=[TorchProfilerCallback(
    export_tensorboard=True,
    profile_memory=True,
    schedule_active=10
)])
def train_model():
    # Your PyTorch training code
    pass
```

**Options:**
- `output_dir`: Output directory for profiling results
- `activities`: List of activities to profile (`['cpu', 'cuda']`)
- `schedule_wait`, `schedule_warmup`, `schedule_active`, `schedule_repeat`: Profiling schedule
- `record_shapes`: Record tensor shapes
- `profile_memory`: Profile memory usage
- `with_stack`: Record stack traces
- `with_flops`: Estimate FLOPs
- `export_tensorboard`: Export for TensorBoard
- `export_chrome_trace`: Export Chrome trace JSON
- `export_stacks`: Export stack traces

#### torchrun_task Decorator

Run tasks with PyTorch's distributed training using `torchrun`.

```python
from slurm import task
from slurm.extras.torch import torchrun_task

@task
@torchrun_task(nproc_per_node=4)
def distributed_training():
    import torch.distributed as dist
    dist.init_process_group(backend="nccl")

    # Your distributed training code
    rank = dist.get_rank()
    world_size = dist.get_world_size()

    # ... training logic ...

    dist.destroy_process_group()
```

**Options:**
- `nproc_per_node`: Number of processes per node
- `nnodes`: Number of nodes
- `node_rank`: Rank of the current node
- `master_addr`: Address of the master node
- `master_port`: Port for communication
- `rdzv_backend`: Rendezvous backend (default: `c10d`)
- `rdzv_endpoint`: Rendezvous endpoint
- `standalone`: Use standalone mode (default: `True`)
- `extra_args`: Additional torchrun arguments

### MemrayCallback

Memory profiling using Bloomberg's memray profiler.

```python
from slurm import task
from slurm.extras.memray import MemrayCallback

@task(callbacks=[MemrayCallback(
    native_traces=True,
    generate_flamegraph=True
)])
def memory_intensive_task():
    # Your memory-intensive code
    pass
```

**Options:**
- `output_dir`: Directory for profiling data
- `output_filename`: Output file name (default: `memray.bin`)
- `native_traces`: Capture native (C/C++) stack traces
- `follow_fork`: Track memory in child processes
- `trace_python_allocators`: Trace Python allocators
- `generate_flamegraph`: Auto-generate flamegraph HTML
- `generate_table`: Auto-generate table report
- `generate_tree`: Auto-generate tree view

**Analyzing Results:**

After execution, use memray CLI tools:

```bash
# Generate flamegraph
memray flamegraph memray.bin -o flamegraph.html

# Show table view
memray table memray.bin

# Show tree view
memray tree memray.bin

# Show statistics
memray stats memray.bin
```

### DebugOnFailureCallback

Attach a remote debugger (VSCode) when a task fails.

```python
from slurm import task
from slurm.extras.debugger import DebugOnFailureCallback

@task(callbacks=[DebugOnFailureCallback(
    port=5678,
    wait_for_client=True,
    timeout=300  # Wait up to 5 minutes
)])
def buggy_task():
    # Your code that might fail
    raise ValueError("Something went wrong!")
```

**Options:**
- `host`: Host address to listen on (default: `0.0.0.0`)
- `port`: Port to listen on (auto-selected if not specified)
- `wait_for_client`: Wait for debugger to attach
- `timeout`: Maximum wait time in seconds
- `output_connection_info`: Save connection info to file
- `connection_info_file`: Connection info filename

**VSCode Configuration:**

When a task fails, the callback will output connection information. Add this to your `.vscode/launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Attach to Slurm Task",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "your-slurm-node.example.com",
                "port": 5678
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "/path/to/remote/workspace"
                }
            ]
        }
    ]
}
```

Then:
1. The callback will print connection information when a failure occurs
2. Open VSCode's Run and Debug panel (Ctrl+Shift+D)
3. Select "Attach to Slurm Task" and press F5
4. You can now inspect variables, step through code, etc.

## Combining Extensions

You can use multiple callbacks together:

```python
from slurm import task
from slurm.callbacks import CProfilerCallback
from slurm.extras.torch import TorchProfilerCallback
from slurm.extras.memray import MemrayCallback
from slurm.extras.debugger import DebugOnFailureCallback

@task(callbacks=[
    CProfilerCallback(print_stats=True),
    TorchProfilerCallback(export_tensorboard=True),
    MemrayCallback(generate_flamegraph=True),
    DebugOnFailureCallback(port=5678),
])
def complex_task():
    # Your code here
    pass
```

## Implementation Notes

- **CProfilerCallback** runs on the RUNNER (where the job executes)
- **TorchProfilerCallback** runs on the RUNNER
- **MemrayCallback** runs on the RUNNER
- **DebugOnFailureCallback** runs on the RUNNER and activates only on failure
- All profiling output is saved to the job directory by default
- Extensions gracefully handle missing dependencies with clear error messages
