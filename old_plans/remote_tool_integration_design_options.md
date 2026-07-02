# Remote Tool Integration Design Options

## Overview

This document explores design options for integrating remote debugging, profiling, and interactive tools into the SLURM SDK. The goal is to provide a unified, ergonomic interface for attaching various development tools to running SLURM jobs.

### Current State

The SDK currently supports:

- `DebugCallback` - Enables debugpy for remote Python debugging
- `slurm jobs connect` - Attach shell to running jobs (with container support)
- `slurm jobs debug` - SSH port forwarding for debugpy

### Tools to Integrate

Based on the background research, the following tools are candidates for SDK integration:

| Tool             | Category         | Emergency Attach? | SDK Integration Potential   |
| ---------------- | ---------------- | ----------------- | --------------------------- |
| debugpy          | Debugger         | Signal-triggered  | ✅ Already implemented      |
| py-spy           | Profiler         | ✅ Native         | High - zero instrumentation |
| memray           | Memory Profiler  | ✅ Native         | High - native attach        |
| PyTorch Profiler | GPU/CPU Profiler | Signal-triggered  | Medium - requires hooks     |
| Nsight Systems   | GPU Profiler     | ❌ No             | Low - must wrap at launch   |
| GDB              | Native Debugger  | ✅ Native         | Medium - specialized use    |
| VSCode Tunnels   | IDE              | ✅ Native         | Medium - useful but complex |
| tmux             | Terminal         | Pre-started only  | Low - user preference       |

______________________________________________________________________

## Design Option 1: Unified Callback System

### Concept

Extend the callback architecture to support multiple tools through a family of specialized callbacks that share common patterns.

### Proposed Callbacks

```python
from slurm.callbacks import (
    DebugCallback,      # Existing - debugpy
    ProfileCallback,    # New - PyTorch Profiler with signal control
    MemrayCallback,     # New - memray with file/signal triggers
    NsysCallback,       # New - wraps command with nsys
    InstrumentationCallback,  # New - unified entrypoint wrapper
)
```

### Implementation

**ProfileCallback (PyTorch Profiler)**

```python
class ProfileCallback(Callback):
    """Enable PyTorch Profiler with signal-based control."""

    def __init__(
        self,
        *,
        signal: int = signal.SIGUSR1,
        output_dir: str = "/tmp/pytorch_profiles",
        activities: List[str] = ["cpu", "cuda"],
        record_shapes: bool = True,
        profile_memory: bool = True,
        with_stack: bool = True,
    ):
        self.signal = signal
        self.output_dir = output_dir
        # ...

    def on_begin_run(self, context):
        # Install signal handler that toggles profiler
        # Inject profiler control code into runner
        pass
```

**MemrayCallback**

```python
class MemrayCallback(Callback):
    """Enable memray memory profiling."""

    def __init__(
        self,
        *,
        mode: Literal["startup", "signal", "attach"] = "attach",
        output_path: str = "/tmp/memray_{job_id}.bin",
        native_traces: bool = True,
    ):
        # mode="attach" means no instrumentation, user runs `memray attach` manually
        # mode="startup" runs entire job under memray
        # mode="signal" installs toggle handler
        pass
```

**NsysCallback**

```python
class NsysCallback(Callback):
    """Wrap job execution with Nsight Systems profiler."""

    def __init__(
        self,
        *,
        trace: List[str] = ["cuda", "nvtx", "osrt"],
        capture_range: Literal["full", "cudaProfilerApi", "nvtx"] = "cudaProfilerApi",
        delay: Optional[int] = None,  # seconds
        duration: Optional[int] = None,  # seconds
        cuda_memory_usage: bool = True,
    ):
        pass

    def wrap_command(self, command: str) -> str:
        # Prepend nsys profile ... to the command
        return f"nsys profile {self._build_flags()} {command}"
```

### CLI Extensions

```bash
# Attach profiler to running job
slurm jobs profile <job_id> --tool pytorch   # Toggle PyTorch profiler via signal
slurm jobs profile <job_id> --tool memray    # Attach memray to process
slurm jobs profile <job_id> --tool py-spy    # Run py-spy on process

# View/download profiles
slurm jobs profiles <job_id>                 # List available profile files
slurm jobs profiles <job_id> --download      # Download to local machine
```

### Pros

- Consistent with existing callback architecture
- Each tool is opt-in and composable
- Clear separation of concerns
- Easy to test individual callbacks

### Cons

- Many callbacks to configure for full debugging setup
- Some tools (nsys) fundamentally change how the job runs
- Signal conflicts possible if multiple callbacks use same signals

______________________________________________________________________

## Design Option 2: Unified Instrumentation Entrypoint

### Concept

Provide a single "super callback" that installs a comprehensive instrumentation entrypoint, enabling all debugging/profiling tools through environment variables and signals.

### Implementation

```python
class InstrumentationCallback(Callback):
    """Universal instrumentation with all debugging/profiling hooks."""

    def __init__(
        self,
        *,
        # Debugpy
        debugpy_port: Optional[int] = None,
        debugpy_wait: bool = False,

        # PyTorch Profiler
        pytorch_profiler_signal: int = signal.SIGUSR1,

        # Interactive REPL
        repl_signal: int = signal.SIGUSR2,

        # Memray
        memray_startup: bool = False,

        # Nsight Systems (changes execution model)
        nsys_enabled: bool = False,
        nsys_start_step: Optional[int] = None,
        nsys_duration_steps: int = 10,
    ):
        pass
```

**Usage**

```python
cluster = Cluster.from_file(
    "Slurmfile.toml",
    callbacks=[
        LoggerCallback(),
        InstrumentationCallback(
            debugpy_port=5678,
            debugpy_wait=True,
            pytorch_profiler_signal=signal.SIGUSR1,
            repl_signal=signal.SIGUSR2,
        ),
    ],
)
```

**Generated Entrypoint**

The callback generates a wrapper script that's prepended to the job:

```python
# Auto-generated instrumentation entrypoint
import os, signal

# Debugpy setup
if port := os.environ.get("SLURM_SDK_DEBUGPY_PORT"):
    import debugpy
    debugpy.listen(("0.0.0.0", int(port)))
    if os.environ.get("SLURM_SDK_DEBUGPY_WAIT"):
        debugpy.wait_for_client()

# PyTorch Profiler toggle
_profiler = None
def _toggle_profiler(sig, frame):
    global _profiler
    from torch.profiler import profile, ProfilerActivity
    if _profiler is None:
        _profiler = profile(activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA])
        _profiler.__enter__()
    else:
        _profiler.__exit__(None, None, None)
        _profiler.export_chrome_trace(f"/tmp/trace_{os.getpid()}.json")
        _profiler = None

signal.signal(signal.SIGUSR1, _toggle_profiler)

# REPL drop
def _repl(sig, frame):
    import code
    code.interact(local={**globals(), **frame.f_locals})

signal.signal(signal.SIGUSR2, _repl)
```

### CLI Integration

```bash
# Show what instrumentation is available for a job
slurm jobs instrumentation <job_id>

# Output:
# Job 12345 instrumentation:
#   debugpy: listening on port 5678
#   pytorch_profiler: SIGUSR1 to toggle
#   repl: SIGUSR2 to drop into REPL
#   memray: not enabled (can attach manually)

# Trigger specific tools
slurm jobs signal <job_id> --sigusr1          # Toggle PyTorch profiler
slurm jobs signal <job_id> --sigusr2          # Drop to REPL
slurm jobs repl <job_id>                       # Alias for SIGUSR2 + connect
```

### Pros

- Single callback for all instrumentation
- Consistent signal conventions across all jobs
- Environment variables document what's enabled
- Simpler user experience

### Cons

- Monolithic - harder to extend with new tools
- All-or-nothing for some features
- May include unnecessary overhead

______________________________________________________________________

## Design Option 3: Tool-Specific CLI Commands

### Concept

Instead of callbacks, provide CLI commands that interact with running jobs directly. Tools that support native attach (py-spy, memray, gdb) don't need pre-instrumentation.

### Proposed CLI Structure

```bash
slurm jobs <command> <job_id> [options]

# Existing
slurm jobs connect <job_id>     # Shell access
slurm jobs debug <job_id>       # debugpy port forwarding

# New - Native attach tools (no pre-instrumentation needed)
slurm jobs py-spy <job_id>      # Run py-spy on job process
slurm jobs memray <job_id>      # Attach memray to job process
slurm jobs gdb <job_id>         # Attach gdb to job process

# New - Signal-based tools (require callback)
slurm jobs profile <job_id>     # Toggle PyTorch profiler
slurm jobs repl <job_id>        # Drop to interactive REPL

# New - Utility
slurm jobs pids <job_id>        # List PIDs in job
slurm jobs signal <job_id> SIG  # Send signal to job process
```

### Implementation: `slurm jobs py-spy`

```python
@jobs_app.command(name="py-spy")
def py_spy_job(
    job_id: str,
    mode: Literal["top", "dump", "record"] = "top",
    duration: int = 60,
    output: Optional[str] = None,
    native: bool = False,
    env: Optional[str] = None,
    slurmfile: Optional[str] = None,
) -> None:
    """Profile a running job with py-spy.

    py-spy attaches to running Python processes without any pre-instrumentation.
    Requires py-spy to be installed in the container.
    """
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    job = cluster.get_job(job_id)
    status = job.get_status()

    # Get the Python process PID
    pid = _find_python_pid(cluster, job_id)

    # Build py-spy command
    if mode == "top":
        cmd = f"py-spy top --pid {pid}"
    elif mode == "dump":
        cmd = f"py-spy dump --pid {pid}"
    elif mode == "record":
        out = output or f"/tmp/py-spy-{job_id}.svg"
        cmd = f"py-spy record --pid {pid} --duration {duration} -o {out}"

    if native:
        cmd += " --native"

    # Execute via srun --overlap
    _execute_in_job(cluster, job_id, cmd, pty=True)
```

### Implementation: `slurm jobs memray`

```python
@jobs_app.command(name="memray")
def memray_job(
    job_id: str,
    mode: Literal["attach", "detach", "flamegraph"] = "attach",
    output: Optional[str] = None,
    duration: Optional[int] = None,
    native: bool = True,
    env: Optional[str] = None,
    slurmfile: Optional[str] = None,
) -> None:
    """Profile memory usage with memray.

    memray can attach to running Python processes to track allocations.
    Requires memray and gdb/lldb in the container.
    """
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    pid = _find_python_pid(cluster, job_id)

    if mode == "attach":
        out = output or f"/tmp/memray-{job_id}.bin"
        cmd = f"memray attach {pid} -o {out}"
        if native:
            cmd += " --native"
        if duration:
            cmd += f" --duration {duration}"
    elif mode == "detach":
        cmd = f"memray detach {pid}"
    elif mode == "flamegraph":
        # Convert existing capture to flamegraph
        cmd = f"memray flamegraph {output}"

    _execute_in_job(cluster, job_id, cmd, pty=True)
```

### Helper: Find Python PID

```python
def _find_python_pid(cluster, job_id: str) -> int:
    """Find the main Python process PID in a job."""
    # Run ps inside the job to find Python processes
    cmd = "ps -eo pid,cmd --no-headers | grep -E 'python.*slurm.runner' | head -1 | awk '{print $1}'"
    stdout, _, _ = _execute_in_job(cluster, job_id, cmd, capture=True)

    pid = stdout.strip()
    if not pid:
        raise RuntimeError(f"Could not find Python process in job {job_id}")

    return int(pid)
```

### Pros

- Tools that support native attach need no pre-instrumentation
- Intuitive CLI - one command per tool
- Users can discover tools via `slurm jobs --help`
- Easy to add new tools

### Cons

- Some tools (PyTorch Profiler, nsys) still need callbacks
- More CLI commands to maintain
- Requires tools installed in container

______________________________________________________________________

## Design Option 4: Hybrid Approach (Recommended)

### Concept

Combine the best of all options:

1. **Callbacks** for tools that need pre-instrumentation (debugpy, PyTorch Profiler, nsys)
1. **CLI commands** for tools with native attach (py-spy, memray, gdb)
1. **Unified signal convention** documented and enforced

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SLURM SDK                                 │
├─────────────────────────────────────────────────────────────────┤
│  Callbacks (pre-instrumentation)     │  CLI (runtime attach)    │
│  ────────────────────────────────    │  ─────────────────────   │
│  • DebugCallback (debugpy)           │  • slurm jobs connect    │
│  • ProfileCallback (PyTorch)         │  • slurm jobs debug      │
│  • NsysCallback (Nsight Systems)     │  • slurm jobs py-spy     │
│  • ReplCallback (interactive REPL)   │  • slurm jobs memray     │
│                                      │  • slurm jobs gdb        │
│                                      │  • slurm jobs signal     │
├─────────────────────────────────────────────────────────────────┤
│  Container Requirements Documentation                            │
│  ─────────────────────────────────────────────────────────────  │
│  Dockerfile snippets for each tool                               │
└─────────────────────────────────────────────────────────────────┘
```

### Callback Implementations

**ProfileCallback**

```python
class ProfileCallback(Callback):
    """Enable PyTorch Profiler with signal-based toggle.

    Usage:
        cluster = Cluster.from_file(
            "Slurmfile.toml",
            callbacks=[ProfileCallback(signal=signal.SIGUSR1)],
        )

    To toggle profiling on a running job:
        slurm jobs signal <job_id> --sigusr1
        # or
        slurm jobs profile <job_id> --toggle
    """

    EXECUTION_LOCUS = CallbackExecutionLocus.RUNNER

    def __init__(
        self,
        *,
        signal: int = signal.SIGUSR1,
        output_dir: str = "/tmp/pytorch_profiles",
        activities: List[str] = ["cpu", "cuda"],
        profile_memory: bool = True,
        with_stack: bool = True,
    ):
        self.signal = signal
        self.output_dir = output_dir
        self.activities = activities
        self.profile_memory = profile_memory
        self.with_stack = with_stack

    def on_begin_run(self, context: RunContext) -> None:
        import os
        import signal as sig
        from torch.profiler import profile, ProfilerActivity

        os.makedirs(self.output_dir, exist_ok=True)

        _profiler = None

        def toggle_profiler(signum, frame):
            nonlocal _profiler
            activities = []
            if "cpu" in self.activities:
                activities.append(ProfilerActivity.CPU)
            if "cuda" in self.activities:
                activities.append(ProfilerActivity.CUDA)

            if _profiler is None:
                _profiler = profile(
                    activities=activities,
                    profile_memory=self.profile_memory,
                    with_stack=self.with_stack,
                )
                _profiler.__enter__()
                print(f"[ProfileCallback] Started profiling (pid={os.getpid()})")
            else:
                _profiler.__exit__(None, None, None)
                trace_path = f"{self.output_dir}/trace_{os.getpid()}_{id(_profiler)}.json"
                _profiler.export_chrome_trace(trace_path)
                print(f"[ProfileCallback] Stopped, trace: {trace_path}")
                _profiler = None

        sig.signal(self.signal, toggle_profiler)
        print(f"[ProfileCallback] Send signal {self.signal} to toggle profiling")
```

**ReplCallback**

```python
class ReplCallback(Callback):
    """Enable interactive REPL via signal.

    When triggered, drops into a Python REPL with access to the current
    execution frame's local and global variables.

    Usage:
        cluster = Cluster.from_file(
            "Slurmfile.toml",
            callbacks=[ReplCallback(signal=signal.SIGUSR2)],
        )

    To trigger:
        slurm jobs signal <job_id> --sigusr2
        # Then connect to see the REPL:
        slurm jobs connect <job_id>
    """

    EXECUTION_LOCUS = CallbackExecutionLocus.RUNNER

    def __init__(self, *, signal: int = signal.SIGUSR2):
        self.signal = signal

    def on_begin_run(self, context: RunContext) -> None:
        import signal as sig
        import code

        def drop_to_repl(signum, frame):
            banner = f"\n[ReplCallback] Interactive console at {frame.f_code.co_filename}:{frame.f_lineno}\n"
            banner += "Local variables available. Type 'exit()' or Ctrl-D to continue.\n"
            code.interact(banner=banner, local={**globals(), **frame.f_locals})

        sig.signal(self.signal, drop_to_repl)
        print(f"[ReplCallback] Send signal {self.signal} for interactive REPL")
```

### CLI Implementations

**slurm jobs signal**

```python
@jobs_app.command(name="signal")
def signal_job(
    job_id: str,
    signal_name: Annotated[str, cyclopts.Parameter(help="Signal to send (e.g., SIGUSR1, USR1, 10)")],
    env: Optional[str] = None,
    slurmfile: Optional[str] = None,
) -> None:
    """Send a signal to the main process in a job.

    Useful for triggering signal-based callbacks like ProfileCallback or ReplCallback.
    """
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    pid = _find_runner_pid(cluster, job_id)

    # Parse signal name
    sig_num = _parse_signal(signal_name)

    # Send signal via srun
    cmd = f"kill -{sig_num} {pid}"
    _execute_in_job(cluster, job_id, cmd)

    console.print(f"[green]Sent signal {signal_name} to PID {pid}[/green]")
```

**slurm jobs py-spy**

```python
@jobs_app.command(name="py-spy")
def py_spy_job(
    job_id: str,
    mode: Annotated[str, cyclopts.Parameter(help="Mode: top, dump, or record")] = "top",
    duration: Annotated[int, cyclopts.Parameter(help="Recording duration in seconds")] = 30,
    native: Annotated[bool, cyclopts.Parameter(help="Include native stack frames")] = False,
    env: Optional[str] = None,
    slurmfile: Optional[str] = None,
) -> None:
    """Profile a running job with py-spy.

    py-spy attaches to running Python processes without pre-instrumentation.
    Requires py-spy installed in the container.

    Modes:
      top     - Live view of where time is being spent (default)
      dump    - One-time stack trace dump
      record  - Record to flamegraph SVG
    """
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    pid = _find_runner_pid(cluster, job_id)

    if mode == "top":
        cmd = f"py-spy top --pid {pid}"
    elif mode == "dump":
        cmd = f"py-spy dump --pid {pid}"
    elif mode == "record":
        cmd = f"py-spy record --pid {pid} --duration {duration} -o /tmp/profile-{job_id}.svg"
    else:
        raise ValueError(f"Unknown mode: {mode}")

    if native:
        cmd += " --native"

    console.print(f"[cyan]Running: {cmd}[/cyan]")
    _execute_in_job_interactive(cluster, job_id, cmd)
```

### Signal Convention

Document and enforce a standard signal mapping:

| Signal  | Number | Default Use              | Callback        |
| ------- | ------ | ------------------------ | --------------- |
| SIGUSR1 | 10     | Toggle PyTorch Profiler  | ProfileCallback |
| SIGUSR2 | 12     | Drop to interactive REPL | ReplCallback    |
| SIGQUIT | 3      | (Reserved for user)      | -               |

### Container Requirements Documentation

Add to docs explaining what each tool needs:

````markdown
## Container Requirements by Tool

### py-spy (CPU profiling)
```dockerfile
RUN pip install py-spy
````

### memray (memory profiling)

```dockerfile
RUN pip install memray
RUN apt-get update && apt-get install -y gdb
```

Note: Container must run with `--cap-add=SYS_PTRACE`

### PyTorch Profiler

```dockerfile
# Included with PyTorch, no additional install needed
```

### debugpy (remote debugging)

```dockerfile
RUN pip install debugpy
```

### Full debugging/profiling image

```dockerfile
RUN pip install debugpy py-spy memray
RUN apt-get update && apt-get install -y gdb tmux
```

```

---

## Recommendation

**Option 4 (Hybrid Approach)** provides the best balance:

1. **Immediate value**: py-spy and memray commands require no code changes
2. **Progressive enhancement**: Users can add callbacks as needed
3. **Clear mental model**: "native attach" vs "pre-instrumented" tools
4. **Extensible**: New tools can be added as either CLI or callback

### Implementation Priority

**Phase 1** (Low effort, high value):
- [ ] `slurm jobs signal` - Foundation for signal-based tools
- [ ] `slurm jobs py-spy` - Zero-instrumentation profiling
- [ ] Document container requirements

**Phase 2** (Medium effort):
- [ ] `ProfileCallback` - PyTorch Profiler with signal toggle
- [ ] `ReplCallback` - Interactive REPL access
- [ ] `slurm jobs profile` - CLI wrapper for ProfileCallback

**Phase 3** (Higher effort):
- [ ] `slurm jobs memray` - Memory profiling
- [ ] `NsysCallback` - Nsight Systems wrapper
- [ ] `slurm jobs gdb` - GDB attach

**Phase 4** (Nice to have):
- [ ] VSCode tunnel integration
- [ ] Profile file download/viewing
- [ ] Unified `slurm jobs instrumentation` status command

---

## Open Questions

1. **Signal conflicts**: What if user code already uses SIGUSR1/SIGUSR2?
   - Option A: Make signals configurable per-callback
   - Option B: Use file-based triggers as alternative
   - Option C: Document and let users resolve conflicts

2. **Multi-process jobs**: How to handle distributed training with multiple ranks?
   - Option A: Target rank 0 by default, `--rank` flag for others
   - Option B: Broadcast signals to all ranks
   - Option C: Let user specify PID explicitly

3. **Container detection**: How to know if tools are installed?
   - Option A: Try and fail with helpful error message
   - Option B: Probe container at job start
   - Option C: Trust user to read docs

4. **Profile file retrieval**: How to get profiles off the cluster?
   - Option A: `slurm jobs download` command
   - Option B: Write to shared filesystem user provides
   - Option C: Integrate with job result retrieval

---

## Appendix: Tool Comparison Matrix

| Feature | debugpy | py-spy | memray | PyTorch Prof | nsys |
|---------|---------|--------|--------|--------------|------|
| Pre-instrumentation | Required | No | No | Required | Required |
| Attach to running | No | Yes | Yes | No | No |
| Signal toggle | Yes | N/A | N/A | Yes | N/A |
| Container deps | debugpy | py-spy | memray, gdb | (builtin) | nsys |
| ptrace needed | No | Yes | Yes | No | No |
| GPU profiling | No | No | No | Yes | Yes |
| Memory profiling | No | No | Yes | Yes | Yes |
| Flame graphs | No | Yes | Yes | Yes | Yes |
| Live view | No | Yes | Yes | No | No |
```
