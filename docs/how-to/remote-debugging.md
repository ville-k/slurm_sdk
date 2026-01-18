# How to Debug SLURM Jobs Remotely

## Problem

You need to debug Python code running inside a SLURM job on a remote compute node, potentially inside a container.

## Prerequisites

- SLURM SDK installed locally
- SSH access to the cluster configured in your Slurmfile
- VS Code with the Python extension (for the debugger UI)

## Steps

### 1. Add debugpy to your container image

If using container packaging, ensure `debugpy` is installed in your container image:

```dockerfile
FROM python:3.11-slim

WORKDIR /workspace
COPY . .

# Install your package AND debugpy
RUN pip install . debugpy

CMD ["python3", "-m", "slurm", "--help"]
```

For wheel packaging, add `debugpy` to your project dependencies or install it in the cluster's Python environment.

### 2. Add DebugCallback to your task submission

Import and add the `DebugCallback` to your cluster's callbacks:

```python
from slurm.callbacks.callbacks import LoggerCallback
from slurm.callbacks.debug import DebugCallback
from slurm.cluster import Cluster
from slurm.decorators import task

@task(time="01:00:00", mem="4G")
def my_task():
    # Your code here - debugger will pause before this runs
    result = complex_computation()
    return result

def main():
    cluster = Cluster.from_file(
        "Slurmfile.toml",
        env="my-cluster",
        callbacks=[
            LoggerCallback(),
            DebugCallback(wait_for_client=True),
        ],
    )

    job = cluster.submit(my_task)()
    job.wait()
```

**DebugCallback options:**

| Option                | Default   | Description                                |
| --------------------- | --------- | ------------------------------------------ |
| `port`                | 5678      | Port for debugpy to listen on              |
| `wait_for_client`     | False     | Block execution until debugger attaches    |
| `host`                | "0.0.0.0" | Interface to bind to                       |
| `log_connection_info` | True      | Log connection details when debugpy starts |

Setting `wait_for_client=True` is recommended for debugging - it pauses execution at the start of your task until you attach the debugger.

### 3. Submit the job

Submit your job as usual:

```bash
python my_script.py
# or
uv run python my_script.py
```

Note the job ID from the output.

### 4. Start the debug port forwarding

Once the job is running, use the CLI to set up SSH port forwarding:

```bash
slurm jobs debug <job_id> --env my-cluster
```

You'll see output like:

```
Setting up debug session for job 12345
Compute node: gpu-0001
Remote debugpy port: 5678
Local port: 5678

Starting SSH port forwarding...
Forwarding localhost:5678 → gpu-0001:5678

VS Code launch.json configuration:
{
    "name": "Attach to SLURM Job",
    "type": "debugpy",
    "request": "attach",
    "connect": {
        "host": "localhost",
        "port": 5678
    },
    "pathMappings": [
        {
            "localRoot": "${workspaceFolder}",
            "remoteRoot": "/path/in/container"
        }
    ]
}

Press Ctrl+C to stop port forwarding

Port forwarding active on localhost:5678
```

Keep this terminal running while debugging.

### 5. Configure VS Code

Create or update `.vscode/launch.json` in your project:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Attach to SLURM Job",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 5678
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "/workspace"
                }
            ]
        }
    ]
}
```

**Important:** Update `remoteRoot` to match where your code is located in the container or on the compute node:

- For containers: typically `/workspace` or wherever your Dockerfile copies the code
- For wheel packaging: the path where the wheel is installed (check with `pip show your-package`)

### 6. Attach the debugger

1. Open VS Code
1. Go to **Run and Debug** (Ctrl+Shift+D / Cmd+Shift+D)
1. Select "Attach to SLURM Job" from the dropdown
1. Click the green play button or press F5

If `wait_for_client=True`, your task will now start executing and stop at breakpoints.

## Verification

To verify debugpy is running inside your job, connect to the container:

```bash
slurm jobs connect <job_id> --env my-cluster
```

Then check for debugpy:

```python
python3 -c "
import os
for pid in os.listdir('/proc'):
    if pid.isdigit():
        try:
            with open(f'/proc/{pid}/cmdline', 'rb') as f:
                cmdline = f.read().decode()
                if 'debugpy' in cmdline:
                    print(f'{pid}: {cmdline.replace(chr(0), \" \")}')
        except:
            pass
"
```

## Troubleshooting

### Connection refused

If you see "Connection refused" when the debugger tries to connect:

1. **debugpy not installed**: Check that debugpy is in your container/environment
1. **Job not running yet**: Wait for the job to start (check with `slurm jobs watch`)
1. **Wrong port**: Ensure the port matches between DebugCallback and your VS Code config

### Debugger connects but no breakpoints hit

1. **Path mappings incorrect**: Verify `remoteRoot` matches the actual code location
1. **Code differs**: Ensure the local code matches what's in the container
1. **Breakpoints in wrong file**: Check you're setting breakpoints in the task function

### Port already in use

If port 5678 is busy locally, use a different port:

```bash
slurm jobs debug <job_id> --port 5679 --env my-cluster
```

And update your VS Code config accordingly.

## See also

- [CLI Reference: slurm jobs debug](../reference/cli.md#slurm-jobs-debug)
- [CLI Reference: slurm jobs connect](../reference/cli.md#slurm-jobs-connect)
- [DebugCallback API Reference](../reference/api/callbacks.md)
