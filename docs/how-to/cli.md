# How to Use the slurm CLI

This guide shows how to use the `slurm` command-line interface to manage jobs, view cluster information, and use the interactive TUI applications.

## Problem

You want to check job status, view cluster partitions, browse documentation, or monitor your cluster without writing Python code.

## Prerequisites

- The slurm-sdk package installed (`pip install slurm-sdk` or `uv add slurm-sdk`)
- For TUI features: install with `pip install slurm-sdk[tui]`
- A Slurmfile in your project (for most commands)
- SSH access to your cluster (for job and partition queries)

## Steps

### 1. View available commands

```bash
slurm --help
```

This shows the main subcommands:

- `jobs` - Manage SLURM jobs
- `cluster` - Manage cluster configurations

### 2. List configured environments

View all environments defined in your Slurmfile without connecting to the cluster:

```bash
slurm cluster list
```

Output shows environment names, hostnames, and the Slurmfile path:

```
                    Environments
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Name       ┃ Hostname              ┃ Slurmfile      ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ default    │ cluster.example.com   │ ./Slurmfile    │
│ production │ prod.example.com      │ ./Slurmfile    │
│ local      │ (local)               │ ./Slurmfile    │
└────────────┴───────────────────────┴────────────────┘
```

### 3. List jobs in the queue

View all jobs currently in the SLURM queue:

```bash
slurm jobs list
```

Jobs are displayed with color-coded states:

- Green: RUNNING
- Yellow: PENDING
- Blue: COMPLETED
- Red: FAILED, TIMEOUT, NODE_FAIL
- Magenta: CANCELLED

### 4. Show job details

Get detailed information about a specific job:

```bash
slurm jobs show 12345
```

This displays a panel with job state, exit code, work directory, timing information, and resource usage.

### 5. View cluster partitions

See available partitions and their status:

```bash
slurm cluster show
```

This connects to the cluster and displays partition information including availability, node counts, time limits, and resources.

### 6. Use a different environment

Specify which Slurmfile environment to use:

```bash
slurm jobs list --env production
slurm cluster show -e staging
```

### 7. Use a specific Slurmfile

Point to a Slurmfile in a different location:

```bash
slurm cluster list --slurmfile /path/to/Slurmfile
slurm jobs list -f ~/projects/ml/Slurmfile
```

### 8. Launch the interactive dashboard

Monitor jobs and cluster status in a terminal UI:

```bash
slurm dash
```

Navigate with arrow keys, press `Enter` to expand sections, and use `Tab` to switch between panes. Press `r` to refresh data manually, or `a` to toggle auto-refresh.

To cancel a running job, select it in the tree and press `c`.

### 9. Browse documentation offline

Open the interactive documentation viewer:

```bash
slurm docs
```

Press `/` to search, use arrow keys to navigate results, and press `Enter` to open a document. Press `Escape` to clear the search.

You can also start with a search query:

```bash
slurm docs workflow
slurm docs --search "task decorator"
```

## Verification

Check that the CLI is working:

```bash
# Should show version number
slurm --version

# Should show environments (if Slurmfile exists)
slurm cluster list
```

## Command reference

| Command                | Description                 | Requires connection |
| ---------------------- | --------------------------- | ------------------- |
| `slurm cluster list`   | List Slurmfile environments | No                  |
| `slurm cluster show`   | Show partition information  | Yes                 |
| `slurm jobs list`      | List jobs in queue          | Yes                 |
| `slurm jobs show <id>` | Show job details            | Yes                 |
| `slurm dash`           | Interactive dashboard (TUI) | Yes                 |
| `slurm docs`           | Documentation viewer (TUI)  | No                  |

## Common options

All commands that connect to a cluster accept:

| Option        | Short | Description                     |
| ------------- | ----- | ------------------------------- |
| `--env`       | `-e`  | Environment name from Slurmfile |
| `--slurmfile` | `-f`  | Path to Slurmfile               |

## Troubleshooting

### "No Slurmfile found"

Create a Slurmfile in your project directory:

```toml
[default]
hostname = "your-cluster.example.com"
username = "your-username"
```

Or specify a path with `--slurmfile`.

### "Environment 'X' not defined"

List available environments and check for typos:

```bash
slurm cluster list
```

### Connection timeout

Check network connectivity and SSH access:

```bash
ssh your-cluster.example.com
```

### Permission denied

Verify your SSH credentials are configured correctly. See [SSH Security](ssh_security.md) for configuration options.

## See also

- [Getting Started](../tutorials/getting_started_hello_world.md) - First steps with the SDK
- [SSH Security](ssh_security.md) - Configure secure SSH connections
- [CLI Reference](../reference/cli.md) - Full command reference
