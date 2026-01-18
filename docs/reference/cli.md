# CLI Reference

The `slurm` command-line interface provides job and cluster management capabilities.

## Installation

The CLI is installed automatically with the slurm-sdk package:

```bash
pip install slurm-sdk
# or
uv add slurm-sdk
```

For TUI features (interactive dashboard and documentation viewer), install with the `tui` extra:

```bash
pip install slurm-sdk[tui]
# or
uv add slurm-sdk[tui]
```

## Synopsis

```
slurm [OPTIONS] COMMAND [ARGS]
```

## Global Options

| Option         | Description                   |
| -------------- | ----------------------------- |
| `--help`, `-h` | Display help message and exit |
| `--version`    | Display application version   |

## Commands

### slurm jobs

Manage SLURM jobs.

#### slurm jobs list

List jobs in the SLURM queue.

```
slurm jobs list [--env ENV] [--slurmfile PATH]
```

**Options:**

| Option        | Short | Description                     |
| ------------- | ----- | ------------------------------- |
| `--env`       | `-e`  | Environment name from Slurmfile |
| `--slurmfile` | `-f`  | Path to Slurmfile               |

**Output columns:**

| Column    | Description                     |
| --------- | ------------------------------- |
| Job ID    | SLURM job identifier            |
| Name      | Job name                        |
| State     | Current job state (color-coded) |
| User      | Job owner                       |
| Time      | Elapsed run time                |
| Partition | SLURM partition                 |
| Nodes     | Number of nodes                 |

**State colors:**

| State     | Color   |
| --------- | ------- |
| RUNNING   | Green   |
| PENDING   | Yellow  |
| COMPLETED | Blue    |
| FAILED    | Red     |
| CANCELLED | Magenta |
| TIMEOUT   | Red     |

#### slurm jobs show

Show details for a specific job.

```
slurm jobs show JOB_ID [--env ENV] [--slurmfile PATH]
```

**Arguments:**

| Argument | Description                      |
| -------- | -------------------------------- |
| `JOB_ID` | SLURM job ID to show details for |

**Options:**

| Option        | Short | Description                     |
| ------------- | ----- | ------------------------------- |
| `--env`       | `-e`  | Environment name from Slurmfile |
| `--slurmfile` | `-f`  | Path to Slurmfile               |

**Output fields:**

- State - Current job state
- Exit Code - Job exit code (e.g., "0:0" for success)
- Work Dir - Job working directory
- Partition - SLURM partition
- Account - SLURM account
- User - Job owner
- Submitted - Submission timestamp
- Started - Start timestamp
- Ended - End timestamp
- Run Time - Total run time
- Time Limit - Maximum allowed time
- Reason - Pending reason (if applicable)
- Nodes - Number of nodes
- CPUs - Number of CPUs

#### slurm jobs watch

Watch jobs in real-time with a live dashboard.

```
slurm jobs watch [JOB_FILTER] [--account USER] [--poll-interval SECONDS] [--env ENV] [--slurmfile PATH]
```

Shows a continuously updating view of jobs in the SLURM queue. If a job ID or name is provided, shows detailed info for matching jobs.

**Arguments:**

| Argument     | Description                       |
| ------------ | --------------------------------- |
| `JOB_FILTER` | Optional job ID or name to filter |

**Options:**

| Option            | Short | Description                                  |
| ----------------- | ----- | -------------------------------------------- |
| `--account`       | `-a`  | Filter jobs by account/user (default: $USER) |
| `--poll-interval` | `-i`  | Seconds between queue polls (default: 5)     |
| `--env`           | `-e`  | Environment name from Slurmfile              |
| `--slurmfile`     | `-f`  | Path to Slurmfile                            |

Press `Ctrl+C` to exit the dashboard.

#### slurm jobs connect

Connect to a running job with an interactive shell.

```
slurm jobs connect JOB_ID [--node NODE] [--no-container] [--env ENV] [--slurmfile PATH]
```

Uses `srun --jobid --overlap --pty bash` to attach a shell to the job's allocation. For container jobs (submitted via `container` packaging), automatically attaches to the running container using the `--container-name` flag.

**Arguments:**

| Argument | Description                |
| -------- | -------------------------- |
| `JOB_ID` | SLURM job ID to connect to |

**Options:**

| Option           | Short | Description                               |
| ---------------- | ----- | ----------------------------------------- |
| `--node`         | `-n`  | Target specific node in multi-node jobs   |
| `--no-container` |       | Connect to bare node instead of container |
| `--env`          | `-e`  | Environment name from Slurmfile           |
| `--slurmfile`    | `-f`  | Path to Slurmfile                         |

**Container Jobs:**

When connecting to a job that was submitted with `container` packaging, the CLI automatically detects this and attaches to the named container (named `slurm-sdk-{pre_submission_id}`) running inside the job. This provides access to the container's filesystem and environment.

Use `--no-container` to bypass container attachment and connect directly to the bare compute node instead.

**Multi-node Jobs:**

For jobs running on multiple nodes, the CLI prompts you to select which node to connect to. Alternatively, use `--node` to specify the target node directly.

#### slurm jobs cancel

Cancel a running or pending job.

```
slurm jobs cancel JOB_ID [--force] [--env ENV] [--slurmfile PATH]
```

Sends a cancellation signal to the job via `scancel`. The job will be terminated if running, or removed from the queue if pending.

**Arguments:**

| Argument | Description            |
| -------- | ---------------------- |
| `JOB_ID` | SLURM job ID to cancel |

**Options:**

| Option        | Short | Description                     |
| ------------- | ----- | ------------------------------- |
| `--force`     | `-f`  | Skip confirmation prompt        |
| `--env`       | `-e`  | Environment name from Slurmfile |
| `--slurmfile` | `-s`  | Path to Slurmfile               |

**Behavior:**

- Shows job name and state before cancelling
- Prompts for confirmation unless `--force` is used
- Jobs in terminal states (COMPLETED, FAILED, CANCELLED) are not cancelled

#### slurm jobs debug

Attach a debugger to a running job.

```
slurm jobs debug JOB_ID [--port PORT] [--wait] [--env ENV] [--slurmfile PATH]
```

Sets up SSH port forwarding from local port to the job's debugpy port on the compute node. The job must be running with debugpy enabled (via `DebugCallback` or `DEBUGPY_PORT` environment variable).

**Arguments:**

| Argument | Description           |
| -------- | --------------------- |
| `JOB_ID` | SLURM job ID to debug |

**Options:**

| Option        | Short | Description                                        |
| ------------- | ----- | -------------------------------------------------- |
| `--port`      | `-p`  | Local port for debugger forwarding (default: 5678) |
| `--wait`      | `-w`  | Signal job to wait for debugger attachment         |
| `--env`       | `-e`  | Environment name from Slurmfile                    |
| `--slurmfile` | `-f`  | Path to Slurmfile                                  |

After connecting, use VS Code's "Remote Attach" configuration to connect to `localhost:<port>`.

### slurm cluster

Manage cluster configurations.

#### slurm cluster list

List configured environments from Slurmfile.

```
slurm cluster list [--slurmfile PATH]
```

This command works offline without connecting to a cluster.

**Options:**

| Option        | Short | Description       |
| ------------- | ----- | ----------------- |
| `--slurmfile` | `-f`  | Path to Slurmfile |

**Output columns:**

| Column    | Description                                       |
| --------- | ------------------------------------------------- |
| Name      | Environment name                                  |
| Hostname  | Cluster hostname (or "(local)" for local backend) |
| Slurmfile | Path to the Slurmfile                             |

#### slurm cluster show

Show cluster partition information.

```
slurm cluster show [--env ENV] [--slurmfile PATH]
```

This command connects to the cluster to retrieve partition information.

**Options:**

| Option        | Short | Description                     |
| ------------- | ----- | ------------------------------- |
| `--env`       | `-e`  | Environment name from Slurmfile |
| `--slurmfile` | `-f`  | Path to Slurmfile               |

**Output columns:**

| Column      | Description                                          |
| ----------- | ---------------------------------------------------- |
| Partition   | Partition name                                       |
| Avail       | Availability status (up/down)                        |
| Nodes       | Total number of nodes in the partition               |
| Node States | Breakdown of nodes by state (e.g., "10 idle, 5 mix") |
| Timelimit   | Maximum job time                                     |

Note: Partitions are aggregated by name. SLURM returns separate rows for each node state, but the CLI combines them to show one row per partition with the total node count and a summary of node states.

#### slurm cluster watch

Watch cluster partitions in real-time with a live dashboard.

```
slurm cluster watch [--account USER] [--poll-interval SECONDS] [--env ENV] [--slurmfile PATH]
```

Shows a continuously updating view of partition utilization including allocated, idle, and down nodes with utilization percentages.

**Options:**

| Option            | Short | Description                                    |
| ----------------- | ----- | ---------------------------------------------- |
| `--account`       | `-a`  | Highlight resource usage for this account/user |
| `--poll-interval` | `-i`  | Seconds between info polls (default: 5)        |
| `--env`           | `-e`  | Environment name from Slurmfile                |
| `--slurmfile`     | `-f`  | Path to Slurmfile                              |

Press `Ctrl+C` to exit the dashboard.

#### slurm cluster connect

Connect to a cluster's login node via SSH.

```
slurm cluster connect [ENV_NAME] [--slurmfile PATH]
```

Opens an interactive SSH session to the cluster's login node as configured in the Slurmfile.

**Arguments:**

| Argument   | Description                                               |
| ---------- | --------------------------------------------------------- |
| `ENV_NAME` | Environment name to connect to (default: first available) |

**Options:**

| Option        | Short | Description       |
| ------------- | ----- | ----------------- |
| `--slurmfile` | `-f`  | Path to Slurmfile |

### slurm dash

Launch an interactive TUI dashboard for monitoring jobs and cluster status.

!!! note "Requires TUI extra"
This command requires the TUI extra: `pip install slurm-sdk[tui]`

```
slurm dash [--env ENV] [--slurmfile PATH] [--refresh-interval SECONDS]
```

**Options:**

| Option               | Short | Description                      | Default |
| -------------------- | ----- | -------------------------------- | ------- |
| `--env`              | `-e`  | Environment name from Slurmfile  |         |
| `--slurmfile`        | `-f`  | Path to Slurmfile                |         |
| `--refresh-interval` |       | Auto-refresh interval in seconds | 30      |

**Dashboard Layout:**

The dashboard displays a two-pane layout:

- **Left pane**: Navigation tree showing:

  - My Jobs - your currently queued and running jobs
  - Account Jobs - jobs from other users in your account
  - Cluster Status - partition availability and node states

- **Right pane**: Detail panel showing information for the selected item

**Keyboard Shortcuts:**

| Key     | Action                     |
| ------- | -------------------------- |
| `↑`/`↓` | Navigate the tree          |
| `Enter` | Expand/collapse tree node  |
| `Tab`   | Switch focus between panes |
| `r`     | Manual refresh             |
| `a`     | Toggle auto-refresh on/off |
| `c`     | Cancel selected job        |
| `q`     | Quit the dashboard         |

**Refresh Behavior:**

The dashboard uses a hybrid refresh approach:

- Auto-refresh is enabled by default (every 30 seconds)
- Press `a` to toggle auto-refresh on/off
- Press `r` for immediate manual refresh
- Last refresh time is shown in the status bar

### slurm docs

Browse SDK documentation in an interactive TUI viewer.

!!! note "Requires TUI extra"
This command requires the TUI extra: `pip install slurm-sdk[tui]`

```
slurm docs [SEARCH] [--search QUERY]
```

**Arguments:**

| Argument | Description                        |
| -------- | ---------------------------------- |
| `SEARCH` | Optional search query (positional) |

**Options:**

| Option     | Description                           |
| ---------- | ------------------------------------- |
| `--search` | Search query to find in documentation |

**Viewer Layout:**

The documentation viewer displays a two-pane layout:

- **Left pane**: Navigation tree following the mkdocs.yml structure

  - Tutorials
  - How-To Guides
  - Reference
  - Explanation
  - Community (Changelog, Contributing)

- **Right pane**: Markdown content viewer with:

  - Syntax-highlighted code blocks
  - Table of contents
  - Clickable internal links

**Keyboard Shortcuts:**

| Key      | Action                           |
| -------- | -------------------------------- |
| `↑`/`↓`  | Navigate tree or search results  |
| `Enter`  | Open selected document/result    |
| `/`      | Show and focus search input      |
| `Escape` | Clear search and hide search bar |
| `Tab`    | Switch focus between panes       |
| `q`      | Quit the viewer                  |

**Search Features:**

- Full-text search across all documentation
- Results show document title and context snippet
- Search index is built on first use and cached
- Live search as you type (after 2+ characters)

**Examples:**

```bash
# Open documentation browser
slurm docs

# Search for "workflow" on startup
slurm docs workflow

# Search with explicit flag
slurm docs --search "task decorator"
```

### slurm mcp

Manage the MCP (Model Context Protocol) server.

#### slurm mcp run

Run the MCP server exposing SLURM SDK APIs.

```
slurm mcp run [--transport TYPE] [--host HOST] [--port PORT] [--env ENV] [--slurmfile PATH]
```

The MCP server provides tools and resources for AI assistants to interact with SLURM clusters.

**Options:**

| Option        | Short | Description                                   |
| ------------- | ----- | --------------------------------------------- |
| `--transport` | `-t`  | Transport type: "stdio" (default) or "http"   |
| `--host`      | `-H`  | Host to bind to for HTTP (default: 127.0.0.1) |
| `--port`      | `-p`  | Port to bind to for HTTP (default: 8000)      |
| `--env`       | `-e`  | Environment name from Slurmfile               |
| `--slurmfile` | `-f`  | Path to Slurmfile                             |

**Transport modes:**

- `stdio` (default): For Claude Desktop integration. Communicates via stdin/stdout.
- `http`: For network access. Exposes an HTTP endpoint at `http://host:port/mcp`.

**Available MCP Tools:**

| Tool                | Description                            |
| ------------------- | -------------------------------------- |
| `list_jobs`         | List jobs in the SLURM queue           |
| `get_job`           | Get detailed status for a specific job |
| `cancel_job`        | Cancel a running or pending job        |
| `list_partitions`   | List cluster partitions                |
| `list_environments` | List Slurmfile environments            |

**Available MCP Resources:**

| URI                     | Description             |
| ----------------------- | ----------------------- |
| `slurm://queue`         | Current job queue       |
| `slurm://jobs/{job_id}` | Job details             |
| `slurm://partitions`    | Partition info          |
| `slurm://config`        | Slurmfile configuration |

#### slurm mcp status

Show MCP server configuration and status.

```
slurm mcp status [--slurmfile PATH]
```

**Options:**

| Option        | Short | Description       |
| ------------- | ----- | ----------------- |
| `--slurmfile` | `-f`  | Path to Slurmfile |

## Environment Variables

| Variable    | Description               |
| ----------- | ------------------------- |
| `SLURMFILE` | Default path to Slurmfile |
| `SLURM_ENV` | Default environment name  |

## Exit Codes

| Code | Description                    |
| ---- | ------------------------------ |
| 0    | Success                        |
| 1    | Error (with message displayed) |
| 130  | Interrupted (Ctrl+C)           |

## Examples

List all environments:

```bash
slurm cluster list
```

List jobs in the production environment:

```bash
slurm jobs list --env production
```

Show details for job 12345:

```bash
slurm jobs show 12345
```

Use a specific Slurmfile:

```bash
slurm cluster list -f /path/to/Slurmfile
```

Show partitions for the staging environment:

```bash
slurm cluster show -e staging
```

Watch your jobs in real-time:

```bash
slurm jobs watch
```

Watch a specific job:

```bash
slurm jobs watch 12345
```

Connect to a running job:

```bash
slurm jobs connect 12345
```

Connect to a container job's bare node (bypassing container):

```bash
slurm jobs connect 12345 --no-container
```

Cancel a job (with confirmation):

```bash
slurm jobs cancel 12345
```

Cancel a job (skip confirmation):

```bash
slurm jobs cancel 12345 --force
```

Debug a running job (requires debugpy enabled):

```bash
slurm jobs debug 12345
```

Watch cluster utilization:

```bash
slurm cluster watch
```

Connect to a cluster:

```bash
slurm cluster connect production
```

Start MCP server for Claude Desktop:

```bash
slurm mcp run
```

Start MCP server over HTTP:

```bash
slurm mcp run --transport http --port 8000
```

## See Also

- [How to Use the CLI](../how-to/cli.md) - Step-by-step usage guide
- [Cluster API Reference](api/cluster.md) - Python API for cluster operations
