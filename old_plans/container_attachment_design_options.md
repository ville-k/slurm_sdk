# Container Attachment Design Options

This document explores design options for connecting to containers running inside SLURM jobs via the `slurm jobs connect` command.

## Current Behavior

Currently, `slurm jobs connect <job_id>` runs:

```bash
srun --jobid=<job_id> --overlap --pty bash
```

This attaches to the **job's resource allocation** on the compute node, not to any container the job might be running inside. Users get a shell on the bare metal node within the job's cgroup.

## Background: How Containers Work in the SDK

### Container Runtime Stack

```
┌─────────────────────────────────────────────┐
│              SLURM Cluster                  │
│  ┌───────────────────────────────────────┐  │
│  │           Pyxis Plugin                │  │  ← SLURM plugin providing --container-* flags
│  │  ┌─────────────────────────────────┐  │  │
│  │  │         enroot Runtime          │  │  │  ← Actual container runtime
│  │  │  ┌───────────────────────────┐  │  │  │
│  │  │  │    Container Instance     │  │  │  │  ← User's job runs here
│  │  │  │    (from Docker image)    │  │  │  │
│  │  │  └───────────────────────────┘  │  │  │
│  │  └─────────────────────────────────┘  │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

### Current Job Submission

The SDK submits container jobs with these flags:

```bash
srun --container-image='registry.example.com/image:tag' \
     --container-mounts='/path:/path:rw' \
     --container-workdir='$JOB_DIR' \
     python -m slurm.runner ...
```

**Notable absence**: No `--container-name` flag is used, meaning containers are anonymous.

### Available Metadata

| Source                       | Information Available                            |
| ---------------------------- | ------------------------------------------------ |
| `CONTAINER_IMAGE` env var    | Image reference used to launch the container     |
| `SLURM_SDK_PACKAGING_CONFIG` | Base64-encoded packaging config (includes image) |
| Job's Slurmfile config       | Packaging type, image name, registry             |
| `.slurm_environment.json`    | Container image, python executable               |

______________________________________________________________________

## Design Options

### Option 1: Named Containers with `--container-name`

**Approach**: Modify job submission to include `--container-name`, then use the same name when connecting.

#### Job Submission Changes

```bash
# Current
srun --container-image='...' python -m slurm.runner

# Proposed
srun --container-image='...' \
     --container-name='slurm-job-{job_id}' \
     python -m slurm.runner
```

#### Connect Command

```bash
srun --jobid=<job_id> --overlap \
     --container-name='slurm-job-{job_id}' \
     --pty bash
```

#### Pros

- Clean, official Pyxis approach
- Deterministic container name from job ID
- Works reliably once configured
- Single command to connect

#### Cons

- **Breaking change**: Requires modifying job submission
- Existing running jobs won't have named containers
- Container name must be coordinated between submission and connect
- May have cluster-specific Pyxis configuration requirements

#### Implementation Complexity: Medium

- Modify `rendering.py` to add `--container-name` flag
- Modify `jobs.py` connect command to use same naming scheme
- Need to handle backward compatibility for jobs submitted without names

______________________________________________________________________

### Option 2: Query and Attach via `enroot exec`

**Approach**: SSH to the compute node, query enroot for running containers, and attach.

#### Connect Flow

```
1. Get job's allocated node from SLURM
2. SSH to that node (via login node hop)
3. List enroot containers: `enroot list -f`
4. Find container matching job (by image name or process)
5. Attach: `enroot exec <pid> bash`
```

#### Example Implementation

```python
def _connect_to_container_via_enroot(backend, job_id, node):
    # Step 1: Find container PID on compute node
    find_cmd = f"ssh {node} 'enroot list -f | grep {image_pattern}'"
    stdout, _, _ = backend._run_command(find_cmd)
    container_pid = parse_enroot_output(stdout)

    # Step 2: Attach to container
    exec_cmd = f"ssh -t {node} 'enroot exec {container_pid} bash'"
    # ... interactive session ...
```

#### Pros

- Works with existing jobs (no submission changes)
- Direct attachment to running container
- Access to container's exact environment

#### Cons

- **Complex**: Requires SSH hop to compute node (may not be allowed)
- Depends on `enroot` being available and accessible
- Container identification is heuristic (by image name matching)
- May require elevated privileges on compute node
- Cluster security policies may block direct node SSH

#### Implementation Complexity: High

- Need to handle SSH to compute nodes
- Parse enroot output formats
- Handle multiple containers on same node
- Error handling for permission issues

______________________________________________________________________

### Option 3: New Container Instance with Same Image

**Approach**: Start a new container instance with the same image, attached to the job allocation.

#### Connect Command

```bash
srun --jobid=<job_id> --overlap \
     --container-image='<same-image>' \
     --container-mounts='<same-mounts>' \
     --pty bash
```

#### Implementation

```python
def connect_job(job_id, container=False):
    if container:
        # Get container config from job metadata or Slurmfile
        image = get_job_container_image(job_id)
        mounts = get_job_container_mounts(job_id)

        srun_cmd = [
            "srun", f"--jobid={job_id}", "--overlap",
            f"--container-image={image}",
            f"--container-mounts={mounts}",
            "--pty", "bash"
        ]
```

#### Pros

- Works with existing jobs
- No cluster configuration changes needed
- Uses official Pyxis mechanism
- Can inspect container filesystem, installed packages

#### Cons

- **Not the same container**: New instance, different PID namespace
- Cannot see running processes from original container
- Cannot interact with job's running code directly
- File changes in new container don't affect original
- Uses additional container overhead

#### Implementation Complexity: Low-Medium

- Need to retrieve container image from job metadata
- Handle mounts configuration
- May need to query Slurmfile for packaging config

______________________________________________________________________

### Option 4: `nsenter` into Container Namespaces

**Approach**: Find the container's init process and use `nsenter` to enter its namespaces.

#### Connect Flow

```
1. SSH to compute node
2. Find container process: ps aux | grep slurm.runner
3. Get container's PID
4. Enter namespaces: nsenter -t <pid> -m -u -i -n -p bash
```

#### Pros

- Enters the actual running container
- Can see and interact with running processes
- Works with any container runtime

#### Cons

- **Requires root or CAP_SYS_ADMIN** on compute node
- Complex to identify correct process
- May not work with all container configurations
- SSH to compute node required
- Security policies likely prohibit this

#### Implementation Complexity: High

- Root access typically not available
- Process identification is fragile
- Namespace flags vary by setup

______________________________________________________________________

### Option 5: Hybrid Approach with Auto-Detection

**Approach**: Detect job configuration and choose the best available method.

#### Decision Tree

```
┌─────────────────────────────────────────┐
│     slurm jobs connect --container      │
└─────────────────┬───────────────────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │ Is job containerized?│
        └─────────┬───────────┘
                  │
         ┌───────┴───────┐
         │ Yes           │ No
         ▼               ▼
┌─────────────────┐  ┌──────────────────┐
│ Has --container-│  │ Regular srun     │
│ name flag?      │  │ --overlap --pty  │
└────────┬────────┘  └──────────────────┘
         │
    ┌────┴────┐
    │ Yes     │ No
    ▼         ▼
┌─────────┐ ┌─────────────────────────┐
│ Attach  │ │ New container instance  │
│ by name │ │ with same image         │
└─────────┘ └─────────────────────────┘
```

#### Implementation

```python
def connect_job(job_id, container=False, env=None, slurmfile=None):
    cluster = get_cluster(env=env, slurmfile=slurmfile)
    job = cluster.get_job(job_id)
    status = job.get_status()

    # Determine if job is containerized
    packaging_config = get_job_packaging_config(cluster, job_id)
    is_container_job = packaging_config.get("type") == "container"

    if container and is_container_job:
        image = packaging_config.get("image")
        # Check if job was submitted with --container-name
        container_name = get_container_name_if_exists(job_id)

        if container_name:
            # Option 1: Attach to named container
            srun_cmd = build_named_container_cmd(job_id, container_name)
        else:
            # Option 3: New instance with same image
            srun_cmd = build_new_container_cmd(job_id, image, packaging_config)
    else:
        # Regular connection to node
        srun_cmd = build_basic_cmd(job_id)

    execute_interactive(srun_cmd)
```

#### Pros

- Flexible: works with various configurations
- Backward compatible
- Can evolve as infrastructure changes
- Best user experience with `--container` flag

#### Cons

- More complex implementation
- Multiple code paths to maintain
- Behavior depends on job configuration

#### Implementation Complexity: Medium

______________________________________________________________________

## Comparison Matrix

| Criterion                | Option 1: Named | Option 2: enroot | Option 3: New Instance | Option 4: nsenter | Option 5: Hybrid |
| ------------------------ | --------------- | ---------------- | ---------------------- | ----------------- | ---------------- |
| Works with existing jobs | No              | Yes              | Yes                    | Yes               | Yes              |
| Enters actual container  | Yes             | Yes              | No (new instance)      | Yes               | Depends          |
| Requires cluster changes | Yes             | No               | No                     | No                | Optional         |
| Security/permissions     | Low             | High             | Low                    | Very High         | Low              |
| Implementation effort    | Medium          | High             | Low                    | High              | Medium           |
| Reliability              | High            | Medium           | High                   | Low               | High             |
| See running processes    | Yes             | Yes              | No                     | Yes               | Depends          |

______________________________________________________________________

## Recommendation

### Short-term: Option 3 (New Container Instance)

For immediate implementation, **Option 3** provides the best balance:

- Works with all existing jobs
- No cluster configuration changes
- Reliable and predictable
- Low implementation complexity

**Limitation**: Users won't see processes from the original container, but they can:

- Inspect the container filesystem
- Run the same Python environment
- Access mounted data
- Debug package installations

### Medium-term: Option 5 (Hybrid) with Option 1

1. Implement Option 3 first for immediate functionality
1. Add `--container-name` to job submission (Option 1)
1. Update connect to prefer named container attachment when available
1. Fall back to new instance for older jobs

### CLI Design

```bash
# Connect to node (current behavior)
slurm jobs connect 12345

# Connect to container (new instance or named if available)
slurm jobs connect 12345 --container

# Force new container instance even if named exists
slurm jobs connect 12345 --container --new-instance
```

______________________________________________________________________

## Implementation Plan

### Phase 1: New Container Instance (Option 3)

1. Add `--container` / `-c` flag to `jobs connect` command
1. Retrieve packaging config from:
   - Job's Slurmfile environment
   - Or query job metadata for `CONTAINER_IMAGE`
1. Build srun command with container flags
1. Execute interactive session

**Estimated effort**: 2-3 hours

### Phase 2: Named Containers (Option 1)

1. Modify `rendering.py` to add `--container-name=slurm-sdk-{pre_submission_id}`
1. Store container name in job metadata
1. Update connect to use named attachment when available
1. Add migration path for existing jobs

**Estimated effort**: 4-6 hours

### Phase 3: Enhanced Detection (Option 5)

1. Auto-detect container configuration
1. Choose optimal connection method
1. Provide clear feedback about connection type
1. Handle edge cases (multi-node, array jobs)

**Estimated effort**: 2-3 hours

______________________________________________________________________

## Open Questions

1. **Multi-node jobs**: How should container connection work when job spans multiple nodes?

   - Connect to first node? Specific node? Let user choose?

1. **Array jobs**: Each array task may have its own container instance

   - Need task ID in addition to job ID?

1. **Container name uniqueness**: If using Option 1, ensure names don't collide

   - Use `slurm-sdk-{job_id}-{step_id}` format?

1. **Mounts for new instance**: Should new container instance have:

   - Same mounts as original?
   - Minimal mounts for debugging?
   - User-configurable?

1. **Working directory**: Where should the shell start?

   - Job directory (`$JOB_DIR`)?
   - Container's default workdir?
   - User's home in container?

______________________________________________________________________

## References

- [Pyxis Documentation](https://github.com/NVIDIA/pyxis)
- [enroot Documentation](https://github.com/NVIDIA/enroot)
- [SLURM srun man page](https://slurm.schedmd.com/srun.html)
- SDK Container Packaging: `src/slurm/packaging/container.py`
- SDK Job Rendering: `src/slurm/rendering.py`
