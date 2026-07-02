# Attaching Tools to Running SLURM Jobs

## Comprehensive Guide for Shells, Debuggers, and Profilers with Enroot/Pyxis

This document covers strategies for attaching various development and debugging tools to running SLURM jobs using enroot/pyxis containers. Each tool is covered with both **planned** (instrumented ahead of time) and **emergency** (attach to already-running process) approaches.

______________________________________________________________________

# Part 1: Interactive Access

## 1.1 Shells and Terminals

### Emergency Attach: ✅ FULLY SUPPORTED

SLURM provides native support for attaching to running jobs without requiring SSH on compute nodes.

**Option A: srun --jobid (Recommended)**

```bash
# Attach a new bash shell to an existing job allocation
srun --jobid=<jobid> --overlap --pty bash

# With pyxis, attach to the SAME container namespace
srun --jobid=<jobid> --overlap --pty --container-name=<container_name> bash

# Target a specific node in multi-node jobs
srun --jobid=<jobid> --overlap -w <nodename> --pty bash
```

The `--overlap` flag allows the new step to share resources with running steps. The `--container-name` flag (requires naming your container at launch) lets you enter the same container environment.

**Option B: sattach (Limited)**

```bash
# Attach to stdio of a running step
sattach <jobid>.<stepid>
```

This only connects to existing stdin/stdout/stderr—you don't get a new shell, just the ability to watch output and send input to the existing process.

**Option C: Direct enroot exec (Requires node access)**

```bash
# If you have SSH or other access to the compute node
enroot list                        # Find your container
enroot exec <container_pid> bash   # Enter the container
```

### Planned Setup

**Name your containers at launch for easier attachment:**

```bash
srun --container-image=$IMAGE \
     --container-name=training_run \
     python train.py
```

Then attach with:

```bash
srun --jobid=$SLURM_JOB_ID --overlap --pty --container-name=training_run bash
```

______________________________________________________________________

## 1.2 tmux/screen for Persistent Sessions

### Planned Setup (Recommended)

Launch your job inside tmux for easy reattachment:

```bash
#!/bin/bash
#SBATCH --job-name=training

# Start tmux session, run training, keep session alive
srun --container-image=$IMAGE --pty \
    tmux new-session -d -s training "python train.py; exec bash"

# The job stays running with tmux, you can attach later
```

**Attach to tmux from srun shell:**

```bash
# First get a shell in the job
srun --jobid=$JOBID --overlap --pty --container-name=training_run bash

# Then attach to tmux
tmux attach -t training
```

**Alternative: tmux inside container**

```bash
# In your sbatch script
srun --container-image=$IMAGE --pty bash -c '
    tmux new-session -d -s main
    tmux send-keys -t main "python train.py" Enter
    tmux attach -t main
'
```

### Container Requirements

```dockerfile
RUN apt-get update && apt-get install -y tmux screen
```

______________________________________________________________________

## 1.3 VSCode Remote Attachment

### Option A: VSCode Tunnels (Cleanest for Containers) ✅

VSCode tunnels work entirely over outbound connections—no SSH server needed.

**Emergency/Planned Setup:**

```bash
# From srun attach session or in your job script
code tunnel --accept-server-license-terms --name slurm-$SLURM_JOB_ID
```

This creates a tunnel you can connect to from:

- Local VSCode via "Remote - Tunnels" extension
- vscode.dev in a browser

**Container requirements:**

```dockerfile
# Download and install code CLI
RUN curl -fsSL "https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64" \
    -o /tmp/vscode-cli.tar.gz && \
    tar -xzf /tmp/vscode-cli.tar.gz -C /usr/local/bin && \
    rm /tmp/vscode-cli.tar.gz
```

**Planned: Auto-start tunnel in job**

```bash
#!/bin/bash
#SBATCH --job-name=training

srun --container-image=$IMAGE bash -c '
    # Start tunnel in background
    code tunnel --accept-server-license-terms --name slurm-$SLURM_JOB_ID &
    TUNNEL_PID=$!
    
    # Run training
    python train.py
    
    # Cleanup
    kill $TUNNEL_PID
'
```

### Option B: SSH + VSCode Remote-SSH

If your cluster allows SSH to compute nodes:

```bash
# In job script, start sshd
srun --container-image=$IMAGE bash -c '
    /usr/sbin/sshd -D &
    python train.py
'
```

Then connect via VSCode Remote-SSH extension to `<node>:<port>`.

______________________________________________________________________

# Part 2: Debuggers

## 2.1 Python Debugger (debugpy)

### Planned Debugging (Recommended)

**Instrumented approach with environment variable trigger:**

```python
import os

if port := os.environ.get("DEBUGPY_PORT"):
    import debugpy
    debugpy.listen(("0.0.0.0", int(port)))
    print(f"Debugger listening on port {port}", flush=True)
    
    if os.environ.get("DEBUGPY_WAIT"):
        print("Waiting for debugger to attach...", flush=True)
        debugpy.wait_for_client()

# ... rest of your training code ...
```

**Launch with debugging enabled:**

```bash
srun --container-image=$IMAGE \
     --export=ALL,DEBUGPY_PORT=5678,DEBUGPY_WAIT=1 \
     python train.py
```

**VSCode launch.json for remote attach:**

```json
{
    "name": "Attach to SLURM Job",
    "type": "python",
    "request": "attach",
    "connect": {
        "host": "<compute_node>",
        "port": 5678
    },
    "pathMappings": [
        {
            "localRoot": "${workspaceFolder}",
            "remoteRoot": "/path/in/container"
        }
    ]
}
```

### Emergency Debugging

**Option A: Signal-triggered debugpy**

```python
import signal
import os

def enable_debugger(signum, frame):
    import debugpy
    port = int(os.environ.get("DEBUGPY_PORT", 5678))
    debugpy.listen(("0.0.0.0", port))
    print(f"Debugger listening on port {port}", flush=True)

signal.signal(signal.SIGUSR1, enable_debugger)
```

Trigger via srun attach: `kill -USR1 <pid>`

**Option B: File-trigger debugpy**

```python
import os
import time

DEBUGGER_TRIGGER = "/tmp/enable_debugger"

def check_debugger_trigger():
    if os.path.exists(DEBUGGER_TRIGGER):
        import debugpy
        if not debugpy.is_client_connected():
            debugpy.listen(("0.0.0.0", 5678))
            print("Debugger enabled, waiting for client...", flush=True)
        os.remove(DEBUGGER_TRIGGER)

# Call periodically in training loop
for step, batch in enumerate(dataloader):
    if step % 100 == 0:
        check_debugger_trigger()
    train_step(batch)
```

Enable via srun attach: `touch /tmp/enable_debugger`

**Option C: Signal-triggered interactive REPL (No VSCode needed)**

```python
import signal
import code

def drop_to_repl(signum, frame):
    """Drop into interactive Python console with access to current state"""
    banner = f"Interactive console at frame: {frame.f_code.co_filename}:{frame.f_lineno}"
    code.interact(banner=banner, local={**globals(), **frame.f_locals})

signal.signal(signal.SIGUSR2, drop_to_repl)
```

Trigger: `kill -USR2 <pid>` — gives you a Python REPL in the srun terminal.

### Read-Only Inspection: py-spy

For non-invasive inspection without any pre-instrumentation:

```bash
# From srun attach session
py-spy dump --pid <pid>              # One-time stack trace
py-spy top --pid <pid>               # Live sampling view  
py-spy record -o profile.svg --pid <pid>  # Record flame graph
```

**Advantages:**

- Zero instrumentation required
- Minimal overhead
- Won't perturb NCCL timing in distributed training
- Works on any Python process

**Container requirements:**

```dockerfile
RUN pip install py-spy
```

______________________________________________________________________

## 2.2 GDB for Native Code

### Emergency Attach

```bash
# From srun attach session
gdb -p <pid>

# Or attach to Python with symbols
gdb python <pid>
```

**Useful GDB commands for Python:**

```gdb
# Get Python stack trace
py-bt

# Print Python locals
py-locals

# Continue execution
continue
```

### Planned: Core Dumps

```bash
# Enable core dumps in job
ulimit -c unlimited
echo "/tmp/core.%p" > /proc/sys/kernel/core_pattern

# After crash, analyze with gdb
gdb python /tmp/core.<pid>
```

______________________________________________________________________

# Part 3: Memory Profilers

## 3.1 Memray (Python Memory Profiler)

### Emergency Attach: ✅ FULLY SUPPORTED

Memray has **native attach capability** for running processes:

```bash
# From your srun attach session
memray attach <pid>                    # Live TUI view
memray attach <pid> -o capture.bin     # Write to file for later analysis
memray attach <pid> --native           # Include C/C++/Rust allocations
memray attach <pid> --duration 60      # Profile for 60 seconds then detach
```

**Requirements:**

- `memray` must be installed in the container's Python environment
- gdb or lldb must be available in the container
- ptrace permissions required (`--cap-add=SYS_PTRACE` in Docker/enroot)
- You can attach to your own processes, or need `CAP_SYS_PTRACE` for others

**Detaching:**

```bash
memray detach <pid>  # Stop profiling, process continues running
```

**Caveats:**

- Only captures allocations *after* attach (no historical data)
- May crash/deadlock edge cases (use in dev only)
- Slight overhead during profiling

### Planned Profiling

**Option A: Launch with profiling**

```bash
# In your sbatch script
srun --container-image=... memray run -o profile.bin --native python train.py
```

**Option B: Programmatic control (recommended for training loops)**

```python
import memray
import os

# Check for profiling trigger
if os.environ.get("MEMRAY_PROFILE"):
    tracker = memray.Tracker("memory_profile.bin", native_traces=True)
    tracker.__enter__()

# ... training code ...

if os.environ.get("MEMRAY_PROFILE"):
    tracker.__exit__(None, None, None)
```

**Option C: Signal-triggered profiling**

```python
import signal
import memray

tracker = None

def toggle_memray(signum, frame):
    global tracker
    if tracker is None:
        tracker = memray.Tracker("/tmp/memray_emergency.bin")
        tracker.__enter__()
        print("Memray profiling started")
    else:
        tracker.__exit__(None, None, None)
        tracker = None
        print("Memray profiling stopped")

signal.signal(signal.SIGUSR2, toggle_memray)
```

Then trigger via: `kill -USR2 <pid>`

______________________________________________________________________

# Part 4: GPU and Performance Profilers

## 4.1 Nsight Systems (nsys)

Memray has **native attach capability** for running processes:

```bash
# From your srun attach session
memray attach <pid>                    # Live TUI view
memray attach <pid> -o capture.bin     # Write to file for later analysis
memray attach <pid> --native           # Include C/C++/Rust allocations
memray attach <pid> --duration 60      # Profile for 60 seconds then detach
```

**Requirements:**

- `memray` must be installed in the container's Python environment
- gdb or lldb must be available in the container
- ptrace permissions required (`--cap-add=SYS_PTRACE` in Docker/enroot)
- You can attach to your own processes, or need `CAP_SYS_PTRACE` for others

**Detaching:**

```bash
memray detach <pid>  # Stop profiling, process continues running
```

**Caveats:**

- Only captures allocations *after* attach (no historical data)
- May crash/deadlock edge cases (use in dev only)
- Slight overhead during profiling

### Planned Profiling

**Option A: Launch with profiling**

```bash
# In your sbatch script
srun --container-image=... memray run -o profile.bin --native python train.py
```

**Option B: Programmatic control (recommended for training loops)**

```python
import memray
import os

# Check for profiling trigger
if os.environ.get("MEMRAY_PROFILE"):
    tracker = memray.Tracker("memory_profile.bin", native_traces=True)
    tracker.__enter__()

# ... training code ...

if os.environ.get("MEMRAY_PROFILE"):
    tracker.__exit__(None, None, None)
```

**Option C: Signal-triggered profiling**

```python
import signal
import memray

tracker = None

def toggle_profiling(signum, frame):
    global tracker
    if tracker is None:
        tracker = memray.Tracker("/tmp/memray_emergency.bin")
        tracker.__enter__()
        print("Memray profiling started")
    else:
        tracker.__exit__(None, None, None)
        tracker = None
        print("Memray profiling stopped")

signal.signal(signal.SIGUSR2, toggle_profiling)
```

Then trigger via: `kill -USR2 <pid>`

______________________________________________________________________

## 4.1 Nsight Systems (nsys)

### Emergency Attach: ❌ NOT SUPPORTED

**Nsight Systems cannot attach to running processes.** This is a fundamental limitation—nsys requires launching the target application to inject its tracing hooks into CUDA/NVTX APIs at startup.

### Planned Profiling

**Option A: Wrap entire job with nsys**

```bash
# sbatch script
srun --container-image=... \
    nsys profile \
    --trace=cuda,nvtx,osrt,cudnn,cublas \
    --cuda-memory-usage=true \
    --output=profile_%q{SLURM_PROCID}_%q{SLURM_JOBID} \
    python train.py
```

**Option B: Delay + Duration (most practical for training)**

```bash
nsys profile \
    --delay=300 \           # Wait 5 minutes before profiling
    --duration=60 \         # Profile for 60 seconds
    --trace=cuda,nvtx \
    --output=training_profile \
    python train.py
```

**Option C: cudaProfilerApi control (RECOMMENDED)**

```python
import torch

# Your training loop
for epoch in range(num_epochs):
    for step, batch in enumerate(dataloader):
        
        # Profile specific steps
        if epoch == 5 and step == 100:
            torch.cuda.cudart().cudaProfilerStart()
        
        # ... training step ...
        
        if epoch == 5 and step == 110:
            torch.cuda.cudart().cudaProfilerStop()
```

Launch with:

```bash
nsys profile --capture-range=cudaProfilerApi \
    --capture-range-end=stop \
    python train.py
```

**Option D: NVTX Range Triggers**

```python
import torch
import nvtx

# Mark regions of interest
with nvtx.annotate("critical_section", color="red"):
    model(batch)
```

Launch with:

```bash
nsys profile --capture-range=nvtx \
    --nvtx-capture="critical_section" \
    python train.py
```

### Emergency Workaround: Checkpoint and Restart

For "emergency" nsys profiling, the practical approach is:

1. Save checkpoint
1. Kill job
1. Restart with nsys wrapper and `--capture-range=cudaProfilerApi`
1. Fast-forward to problem area and trigger profiling

______________________________________________________________________

## 4.2 PyTorch Profiler

### Emergency Attach: ⚠️ REQUIRES PRE-INSTRUMENTATION

PyTorch Profiler must be instantiated within the Python process—there's no external attach mechanism. However, you can build dynamic enablement into your code.

### Planned Profiling

**Option A: Schedule-based (best for training loops)**

```python
from torch.profiler import profile, schedule, tensorboard_trace_handler, ProfilerActivity

with profile(
    activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA],
    schedule=schedule(
        skip_first=10,   # Skip first 10 steps
        wait=5,          # Then wait 5 steps
        warmup=1,        # Warmup for 1 step
        active=3,        # Actively profile 3 steps
        repeat=2         # Repeat cycle twice
    ),
    on_trace_ready=tensorboard_trace_handler('./logs'),
    record_shapes=True,
    profile_memory=True,
    with_stack=True
) as prof:
    for step, batch in enumerate(dataloader):
        train_step(batch)
        prof.step()
```

**Option B: Dynamic toggle (for emergency scenarios)**

```python
import torch
from torch.profiler import profile, ProfilerActivity
import signal
import os

class DynamicProfiler:
    def __init__(self):
        self.profiler = None
        self.output_dir = "/tmp/pytorch_profiles"
        os.makedirs(self.output_dir, exist_ok=True)
        
    def start(self):
        if self.profiler is None:
            self.profiler = profile(
                activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA],
                record_shapes=True,
                profile_memory=True,
                with_stack=True
            )
            self.profiler.__enter__()
            print("PyTorch Profiler started")
    
    def stop(self):
        if self.profiler is not None:
            self.profiler.__exit__(None, None, None)
            trace_path = f"{self.output_dir}/trace_{os.getpid()}.json"
            self.profiler.export_chrome_trace(trace_path)
            print(f"Profiler stopped, trace saved to {trace_path}")
            self.profiler = None

dynamic_profiler = DynamicProfiler()

def handle_profile_signal(signum, frame):
    if dynamic_profiler.profiler is None:
        dynamic_profiler.start()
    else:
        dynamic_profiler.stop()

signal.signal(signal.SIGUSR1, handle_profile_signal)
```

Trigger from srun attach: `kill -USR1 <pid>`

**Option C: File-trigger based (no signal handling needed)**

```python
import os
from torch.profiler import profile, ProfilerActivity

TRIGGER_FILE = "/tmp/enable_pytorch_profile"
profiler = None

def check_profile_trigger():
    global profiler
    if os.path.exists(TRIGGER_FILE) and profiler is None:
        profiler = profile(
            activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA],
            profile_memory=True
        )
        profiler.__enter__()
        print("Profiling enabled")
    elif not os.path.exists(TRIGGER_FILE) and profiler is not None:
        profiler.__exit__(None, None, None)
        profiler.export_chrome_trace("/tmp/emergency_trace.json")
        profiler = None
        print("Profiling disabled, trace saved")

# In training loop
for step, batch in enumerate(dataloader):
    if step % 10 == 0:  # Check every 10 steps
        check_profile_trigger()
    train_step(batch)
```

Enable via srun attach: `touch /tmp/enable_pytorch_profile`
Disable: `rm /tmp/enable_pytorch_profile`

______________________________________________________________________

# Part 5: Infrastructure Setup

## 5.1 Container Image Requirements

Include these in your training container for full debugging/profiling capability:

```dockerfile
FROM nvcr.io/nvidia/pytorch:24.01-py3

# ============= DEBUGGING TOOLS =============
# GDB for native debugging
RUN apt-get update && apt-get install -y \
    gdb \
    tmux \
    screen \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python debugging and profiling
RUN pip install --no-cache-dir \
    debugpy \
    py-spy \
    memray \
    nvtx

# VSCode CLI for tunnels
RUN curl -fsSL "https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64" \
    -o /tmp/vscode-cli.tar.gz && \
    tar -xzf /tmp/vscode-cli.tar.gz -C /usr/local/bin && \
    rm /tmp/vscode-cli.tar.gz

# Enable ptrace for memray attach (also needs runtime flag)
# Note: actual capability must be granted at container runtime
```

______________________________________________________________________

## 5.2 Unified Entrypoint with All Hooks

```python
#!/usr/bin/env python3
"""
training_entrypoint.py - Universal wrapper with debugging/profiling hooks

Signals:
  SIGUSR1 - Toggle PyTorch Profiler
  SIGUSR2 - Drop to interactive REPL
  
Environment Variables:
  DEBUGPY_PORT     - Enable debugpy on this port at startup
  DEBUGPY_WAIT     - Wait for debugger to attach before continuing
  NSYS_START_STEP  - Step number to start nsys profiling (with cudaProfilerApi)
  NSYS_DURATION    - Number of steps to profile with nsys
  MEMRAY_PROFILE   - Enable memray from startup
"""

import os
import sys
import signal

# ==================== DEBUGPY SETUP ====================
if port := os.environ.get("DEBUGPY_PORT"):
    import debugpy
    debugpy.listen(("0.0.0.0", int(port)))
    print(f"[DEBUG] debugpy listening on port {port}", flush=True)
    if os.environ.get("DEBUGPY_WAIT"):
        print("[DEBUG] Waiting for debugger to attach...", flush=True)
        debugpy.wait_for_client()

# ==================== PYTORCH PROFILER ====================
_pytorch_profiler = None

def _toggle_pytorch_profiler(signum, frame):
    global _pytorch_profiler
    from torch.profiler import profile, ProfilerActivity
    
    if _pytorch_profiler is None:
        _pytorch_profiler = profile(
            activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA],
            profile_memory=True,
            with_stack=True
        )
        _pytorch_profiler.__enter__()
        print(f"[PROFILE] PyTorch profiler STARTED (pid={os.getpid()})", flush=True)
    else:
        _pytorch_profiler.__exit__(None, None, None)
        out = f"/tmp/pytorch_trace_{os.getpid()}_{id(_pytorch_profiler)}.json"
        _pytorch_profiler.export_chrome_trace(out)
        _pytorch_profiler = None
        print(f"[PROFILE] PyTorch profiler STOPPED -> {out}", flush=True)

signal.signal(signal.SIGUSR1, _toggle_pytorch_profiler)

# ==================== INTERACTIVE REPL ====================
def _drop_to_repl(signum, frame):
    import code
    banner = f"\n[REPL] Interactive console at {frame.f_code.co_filename}:{frame.f_lineno}\n"
    banner += "Local variables available. Type 'exit()' or Ctrl-D to continue.\n"
    code.interact(banner=banner, local={**globals(), **frame.f_locals})

signal.signal(signal.SIGUSR2, _drop_to_repl)

# ==================== NSIGHT SYSTEMS HOOKS ====================
_NSYS_START = int(os.environ.get("NSYS_START_STEP", -1))
_NSYS_DURATION = int(os.environ.get("NSYS_DURATION", 10))

def nsys_step_hook(step: int):
    """Call this in your training loop for nsys cudaProfilerApi control"""
    import torch
    if step == _NSYS_START:
        torch.cuda.cudart().cudaProfilerStart()
        print(f"[NSYS] Profiling STARTED at step {step}", flush=True)
    elif step == _NSYS_START + _NSYS_DURATION:
        torch.cuda.cudart().cudaProfilerStop()
        print(f"[NSYS] Profiling STOPPED at step {step}", flush=True)

# ==================== MEMRAY HOOKS ====================
_memray_tracker = None

if os.environ.get("MEMRAY_PROFILE"):
    import memray
    _memray_tracker = memray.Tracker(
        f"/tmp/memray_{os.getpid()}.bin",
        native_traces=True
    )
    _memray_tracker.__enter__()
    print(f"[MEMRAY] Profiling enabled from startup", flush=True)

# ==================== EXPORTS ====================
__all__ = ['nsys_step_hook']

# ==================== MAIN ====================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m training_entrypoint <script.py> [args...]")
        sys.exit(1)
    
    # Run the target script
    script = sys.argv[1]
    sys.argv = sys.argv[1:]  # Shift argv so script sees correct args
    
    with open(script) as f:
        code = compile(f.read(), script, 'exec')
        exec(code, {'__name__': '__main__', '__file__': script})
```

______________________________________________________________________

## 5.3 SLURM Job Template

```bash
#!/bin/bash
#SBATCH --job-name=training
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --gpus-per-node=8
#SBATCH --time=24:00:00

# ============= CONTAINER CONFIGURATION =============
IMAGE="nvcr.io/nvidia/pytorch:24.01-py3"
CONTAINER_NAME="training_${SLURM_JOB_ID}"

# Enable ptrace for memray attach
CONTAINER_FLAGS="--cap-add=SYS_PTRACE"

# Mount paths
MOUNTS="/scratch:/scratch,/tmp:/tmp"

# ============= LAUNCH MODES =============

# MODE 1: Normal training with debugging hooks available
srun --container-image=$IMAGE \
     --container-name=$CONTAINER_NAME \
     --container-mounts=$MOUNTS \
     $CONTAINER_FLAGS \
     python -m training_entrypoint train.py --config config.yaml

# MODE 2: With nsys profiling (uncomment to use)
# srun --container-image=$IMAGE \
#      --container-name=$CONTAINER_NAME \
#      --container-mounts=$MOUNTS \
#      nsys profile \
#          --capture-range=cudaProfilerApi \
#          --capture-range-end=stop \
#          --trace=cuda,nvtx,osrt \
#          --cuda-memory-usage=true \
#          --output=profile_%q{SLURM_PROCID}_%q{SLURM_JOBID} \
#      python -m training_entrypoint train.py --config config.yaml

# MODE 3: With debugger waiting (uncomment to use)
# srun --container-image=$IMAGE \
#      --container-name=$CONTAINER_NAME \
#      --container-mounts=$MOUNTS \
#      --export=ALL,DEBUGPY_PORT=5678,DEBUGPY_WAIT=1 \
#      python -m training_entrypoint train.py --config config.yaml
```

______________________________________________________________________

# Part 6: Quick Reference

## 6.1 Emergency Commands Cheat Sheet

| Tool                 | Emergency Command                                        | Pre-instrumentation Required? |
| -------------------- | -------------------------------------------------------- | ----------------------------- |
| **Shell**            | `srun --jobid=X --overlap --pty --container-name=Y bash` | No                            |
| **tmux attach**      | `tmux attach -t session` (from srun shell)               | Yes (tmux must be running)    |
| **VSCode**           | `code tunnel` (from srun shell)                          | No                            |
| **debugpy**          | `kill -USR1 <pid>`                                       | Yes (signal handler)          |
| **REPL**             | `kill -USR2 <pid>`                                       | Yes (signal handler)          |
| **py-spy**           | `py-spy dump --pid <pid>`                                | No                            |
| **memray**           | `memray attach <pid>`                                    | No                            |
| **PyTorch Profiler** | `kill -USR1 <pid>`                                       | Yes (signal handler)          |
| **nsys**             | ❌ Not possible                                          | Must restart with nsys        |
| **gdb**              | `gdb -p <pid>`                                           | No                            |

## 6.2 Capability Summary

| Tool                 | Emergency Attach        | Planned Profiling | Best For                             |
| -------------------- | ----------------------- | ----------------- | ------------------------------------ |
| **srun --jobid**     | ✅ Native               | ✅                | Shell access, running commands       |
| **tmux**             | ✅ (if pre-started)     | ✅                | Persistent sessions, detach/reattach |
| **VSCode Tunnels**   | ✅                      | ✅                | Full IDE, editing, terminal          |
| **debugpy**          | ⚠️ Signal-triggered     | ✅                | Breakpoints, step debugging          |
| **py-spy**           | ✅ Zero instrumentation | ✅                | CPU profiling, stack traces          |
| **gdb**              | ✅ Native               | ✅                | Native code, crashes, core dumps     |
| **memray**           | ✅ Native               | ✅                | Memory leaks, allocation patterns    |
| **PyTorch Profiler** | ⚠️ Signal-triggered     | ✅                | Operator timing, memory, CUDA        |
| **nsys**             | ❌                      | ✅ Excellent      | GPU kernels, system-wide, CUDA API   |

## 6.3 Signal Assignment Convention

For the unified entrypoint, use these signals consistently:

| Signal    | Action                     | Use Case                         |
| --------- | -------------------------- | -------------------------------- |
| `SIGUSR1` | Toggle PyTorch Profiler    | Performance investigation        |
| `SIGUSR2` | Drop to REPL               | Emergency state inspection       |
| `SIGQUIT` | (Available for custom use) | Could be: dump stats, checkpoint |

## 6.4 File Triggers (Alternative to Signals)

For systems where signals are problematic:

| Trigger File                  | Action                      |
| ----------------------------- | --------------------------- |
| `/tmp/enable_pytorch_profile` | Start/stop PyTorch profiler |
| `/tmp/enable_debugger`        | Start debugpy listener      |
| `/tmp/dump_state`             | Write debug info to file    |

______________________________________________________________________

# Part 7: Troubleshooting

## Common Issues

### "Cannot attach - permission denied"

```bash
# Check ptrace permissions
cat /proc/sys/kernel/yama/ptrace_scope
# 0 = anyone can attach to their own processes
# 1 = only parent can attach (default)
# Fix: Run container with --cap-add=SYS_PTRACE
```

### "Container not found" when using --container-name

```bash
# List running containers
enroot list

# Make sure you named the container at launch
srun --container-name=myname ...
```

### "debugpy already listening"

```python
# Check if already connected before listening
import debugpy
if not debugpy.is_client_connected():
    debugpy.listen(("0.0.0.0", 5678))
```

### "Signal not delivered to correct process"

```bash
# In multi-process training, find the correct rank
ps aux | grep python
# Or use SLURM's process tracking
scontrol listpids $SLURM_JOB_ID
```

### nsys produces empty trace

```bash
# Make sure cudaProfilerStart() was actually called
# Check that --capture-range=cudaProfilerApi was set
# Verify CUDA operations happen between start/stop
```

______________________________________________________________________

*Document for ML training infrastructure. Covers SLURM + Enroot/Pyxis containerized workloads.*
