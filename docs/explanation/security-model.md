# Security Model

This document explains the security model of the SLURM SDK, including trust
assumptions, potential risks, and best practices for secure usage.

## Overview

The Slurm SDK is designed to run user-defined Python tasks on HPC clusters. It
handles:

- Serializing task arguments and results
- Transferring files to and from clusters via SSH or local filesystem
- Executing commands on remote systems
- Managing job lifecycle

Security is balanced against usability for HPC workflows, where users typically
have authenticated access to cluster resources.

## Trust Model

### What the SDK Trusts

1. **The cluster filesystem**: Files written to the job directory are trusted.
   The SDK assumes that only authorized users can access job directories on the
   cluster.

1. **SSH connections**: Once authenticated, the SSH connection is trusted for
   command execution and file transfer.

1. **Local filesystem**: On local backend, the SDK trusts the local filesystem.

1. **User-provided code**: Tasks are user-defined Python functions. The SDK
   executes whatever code the user provides.

### What the SDK Does NOT Trust

1. **Arbitrary network hosts**: The SDK verifies SSH host keys (by default, logs
   a warning for unknown hosts).

1. **User input in shell commands**: All user-provided strings are escaped with
   `shlex.quote()` before inclusion in shell commands.

## Pickle Serialization

### Why Pickle?

The SDK uses Python's `pickle` module for serializing:

- Task arguments and keyword arguments
- Task results
- Callbacks
- Array job items
- System path configuration

Pickle is used because it can serialize arbitrary Python objects, which is
essential for passing complex data structures to tasks.

### Security Implications

Pickle deserialization can execute arbitrary code if the pickled data is
maliciously crafted. This is a known security concern (CWE-502).

### Mitigation

The SDK's pickle usage is safe when:

1. **Pickle files are created by the SDK itself** - The SDK serializes data in
   `rendering.py` and deserializes it in `runner.py`.

1. **Files are transferred via trusted channels** - SSH with host key
   verification, or local filesystem access.

1. **Job directories are protected** - Standard HPC cluster permissions prevent
   unauthorized access to job directories.

### When to Be Cautious

- Do not deserialize pickle files from untrusted sources
- Ensure cluster filesystem permissions are properly configured
- Use `host_key_policy="reject"` for SSH connections in high-security
  environments

## SSH Security

### Host Key Verification

The SDK supports three host key verification policies:

| Policy   | Description                                     | Use Case                   |
| -------- | ----------------------------------------------- | -------------------------- |
| `auto`   | Automatically accept and save unknown keys      | Development/testing        |
| `warn`   | Log a warning but accept unknown keys (default) | General use                |
| `reject` | Reject connections to unknown hosts             | High-security environments |

Configure via:

```python
from slurm import Cluster

cluster = Cluster(
    hostname="cluster.example.com",
    host_key_policy="reject",  # Strictest setting
)
```

Or via CLI:

```bash
python script.py --host-key-policy=reject
```

### Credential Handling

- **SSH keys are preferred** over passwords for authentication
- Passwords are cleared from memory immediately after successful authentication
- Avoid passing passwords via CLI arguments (visible in process list)
- Use SSH agent or key files instead

### Recommendations

1. Add cluster hosts to `~/.ssh/known_hosts` before first use
1. Use SSH keys with passphrase protection
1. Consider using `host_key_policy="reject"` in production

## Command Execution

### Shell Injection Prevention

The SDK prevents shell injection through:

1. **List-based subprocess calls**: Internal SLURM commands use list arguments
   with `shell=False`, eliminating shell interpretation.

1. **shlex.quote()**: All user-provided strings included in shell commands are
   escaped using `shlex.quote()`.

1. **Input validation**: The `validation` module provides functions to validate
   job names, account names, partition names, and job IDs.

### Public API

The `execute_command()` method accepts arbitrary shell commands and uses
`shell=True`. This is intentional for flexibility but requires caution:

```python
# Safe: no user input
cluster.backend.execute_command("ls -la /path/to/dir")

# UNSAFE: user input not escaped
# cluster.backend.execute_command(f"cat {user_provided_path}")  # DON'T DO THIS

# Safe: escaped user input
import shlex
cluster.backend.execute_command(f"cat {shlex.quote(user_provided_path)}")
```

## Slurmfile Callbacks

### Dynamic Code Loading

Callbacks specified in `Slurmfile` configuration are dynamically imported and
instantiated:

```toml
[[callbacks]]
target = "mypackage.callbacks:MyCallback"
args = ["arg1"]
kwargs = { key = "value" }
```

This loads arbitrary Python modules, similar to `pyproject.toml` scripts.

### Security Implications

- Only use trusted Slurmfiles
- Review callback configurations before running
- Callbacks execute with the same permissions as the SDK

## File Permissions

### Job Scripts

Job scripts are created with permissions `0o750` (owner: rwx, group: r-x) by
default. This can be configured:

```python
from slurm.api.local import LocalBackend

backend = LocalBackend(script_permissions=0o755)  # More permissive if needed
```

Use `0o755` if your SLURM configuration requires world-readable scripts.

### Job Directories

Job directories are created with default `umask` permissions. Ensure your
cluster's `umask` settings provide appropriate protection.

## Recommendations for Production Use

### High-Security Environments

1. Use `host_key_policy="reject"` for SSH
1. Pre-populate `~/.ssh/known_hosts` with cluster host keys
1. Use SSH key authentication (no passwords)
1. Review and audit Slurmfile configurations
1. Restrict job directory permissions

### General Best Practices

1. Keep the SDK updated for security fixes
1. Use SSH keys instead of passwords
1. Validate user input before including in job configurations
1. Monitor job directories for unauthorized modifications
1. Use cluster-provided authentication mechanisms (Kerberos, etc.) when
   available

## Security-Related Configuration

| Setting              | Location            | Default        | Description                  |
| -------------------- | ------------------- | -------------- | ---------------------------- |
| `host_key_policy`    | `SSHCommandBackend` | `"warn"`       | SSH host key verification    |
| `script_permissions` | `LocalBackend`      | `0o750`        | Job script file permissions  |
| `job_base_dir`       | Backend             | `~/slurm_jobs` | Base directory for job files |

## Reporting Security Issues

If you discover a security vulnerability in the Slurm SDK, please report it
responsibly by contacting the maintainers directly rather than opening a public
issue.
