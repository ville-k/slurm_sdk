# CLI Reference

The `slurm` command-line interface provides job and cluster management capabilities.

## Installation

The CLI is installed automatically with the slurm-sdk package:

```bash
pip install slurm-sdk
# or
uv add slurm-sdk
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

## See Also

- [How to Use the CLI](../how-to/cli.md) - Step-by-step usage guide
- [Cluster API Reference](api/cluster.md) - Python API for cluster operations
