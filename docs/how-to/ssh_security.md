# How to Harden SSH Connections for Production

This guide shows how to configure the SLURM SDK for secure SSH connections in production environments.

## Problem

By default, the SDK uses `host_key_policy="warn"` which logs warnings for unknown SSH hosts but still connects. This is convenient for development but not appropriate for production where you want to prevent man-in-the-middle attacks.

## Prerequisites

- SSH access to your SLURM cluster
- The cluster's SSH host key fingerprint (obtain from your cluster administrator)

## Steps

### 1. Obtain the cluster's host key

Get the host key from your cluster. You can do this by SSHing to the cluster once and verifying the fingerprint with your administrator:

```bash
# Connect once to add to known_hosts (verify fingerprint first!)
ssh your-cluster.example.com

# Or fetch the key without connecting
ssh-keyscan -t ed25519 your-cluster.example.com >> ~/.ssh/known_hosts
```

Verify the fingerprint matches what your administrator provides before accepting.

### 2. Configure the SDK to reject unknown hosts

Set `host_key_policy="reject"` to refuse connections to hosts not in your known_hosts file:

```python
from slurm import Cluster

cluster = Cluster(
    hostname="your-cluster.example.com",
    username="your-username",
    host_key_policy="reject",  # Reject unknown hosts
)
```

Or via CLI:

```bash
python your_script.py \
    --hostname your-cluster.example.com \
    --host-key-policy reject
```

### 3. Use SSH key authentication

Avoid password authentication in production. Configure SSH keys:

```python
cluster = Cluster(
    hostname="your-cluster.example.com",
    username="your-username",
    host_key_policy="reject",
    # SSH keys are used automatically from ~/.ssh/
    # Or specify explicitly:
    # key_filename="/path/to/your/private_key",
)
```

### 4. Configure via Slurmfile (optional)

For project-wide settings, add to your `Slurmfile`:

```toml
[cluster]
hostname = "your-cluster.example.com"
username = "your-username"
host_key_policy = "reject"
```

## Verification

Test that the configuration rejects unknown hosts:

```python
from slurm import Cluster
import paramiko

# This should raise paramiko.ssh_exception.SSHException
# if the host is not in known_hosts
try:
    cluster = Cluster(
        hostname="unknown-host.example.com",
        host_key_policy="reject",
    )
    cluster.backend._connect()
except paramiko.ssh_exception.SSHException as e:
    print(f"Correctly rejected unknown host: {e}")
```

## Host key policy options

| Policy   | Behavior                      | Use case              |
| -------- | ----------------------------- | --------------------- |
| `reject` | Refuse unknown hosts          | Production            |
| `warn`   | Log warning, allow connection | Development (default) |
| `auto`   | Silently accept and save      | Testing only          |

## Troubleshooting

### "Server not found in known_hosts"

The host key is not in your `~/.ssh/known_hosts` file. Add it:

```bash
ssh-keyscan -t ed25519 your-cluster.example.com >> ~/.ssh/known_hosts
```

### "Host key verification failed"

The host key changed since you last connected. This could indicate:

1. **Legitimate change**: The cluster was reinstalled or reconfigured. Verify with your administrator, then update:

   ```bash
   ssh-keygen -R your-cluster.example.com
   ssh-keyscan -t ed25519 your-cluster.example.com >> ~/.ssh/known_hosts
   ```

1. **Potential attack**: If unexpected, do not connect. Contact your administrator.

### Connection works in terminal but not SDK

Ensure the SDK is reading the same known_hosts file. The SDK checks:

1. System host keys (`/etc/ssh/ssh_known_hosts`)
1. User host keys (`~/.ssh/known_hosts`)

## See also

- [Security Model](../explanation/security-model.md) - Full security documentation
- [SSH Configuration](https://man.openbsd.org/ssh_config) - OpenSSH config options
