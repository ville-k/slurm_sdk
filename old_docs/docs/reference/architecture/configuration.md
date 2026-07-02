# Configuration Reference

This page describes how the SDK discovers and interprets project configuration, with a focus on the TOML-based `Slurmfile` consumed by `Cluster.from_env` and related helpers.

## File discovery

- The loader searches for a Slurmfile by respecting (in order) an explicit path, the `SLURMFILE` environment variable, and an upward directory walk from the current working directory looking for `Slurmfile`, `Slurmfile.toml`, `slurmfile`, or `slurmfile.toml`.
- Passing a directory resolves to the first matching filename inside it. Missing files raise `SlurmfileNotFoundError`.

## Environment selection

- Each Slurmfile can contain multiple environments. The active environment is chosen from an explicit parameter, the `SLURM_ENV` environment variable, or defaults to `default`.
- Configuration is merged in three layers: `root.default` (if present), `environments.default`, and the named environment. Non-dictionary entries raise `SlurmfileInvalidError`.

## Schema overview

- `cluster.backend` *(required)*: backend identifier understood by `slurm.api.create_backend`.
- `cluster.job_base_dir`: base directory for job artifacts on the target node.
- `cluster.backend_config`: free-form table passed directly to backend constructors (e.g., SSH hostname, username, port, timeouts).
- `packaging`: optional table forwarded to `slurm.packaging.get_packaging_strategy` when tasks omit an explicit config.
- `submit`: optional table consumed by examples and CLI tooling for common SBATCH arguments (e.g., `account`, `partition`).
- `callbacks`: optional list of dotted-import strings or objects describing callback factories.

## Callback instantiation

- Each callback entry can be a string (`"package.module:Class"`) or a table with `target`, optional `args`, and `kwargs` lists.
- The loader imports the target, instantiates it, and verifies it subclasses `BaseCallback`. Failures raise `SlurmfileInvalidError` with the problematic target.

## Example

```toml
[default.cluster]
backend = "ssh"
job_base_dir = "~/slurm_jobs"

[default.cluster.backend_config]
hostname = "login.example.com"
username = "slurm-user"

[default.packaging]
type = "wheel"
python_version = "3.9"

[local.cluster]
job_base_dir = "/tmp/slurm_jobs"

[local.cluster.backend_config]
hostname = "localhost"
port = 10022
```

## Public API

- `slurm.cluster.Cluster.from_env(path_or_env, env=None, overrides=None, callbacks=None)` constructs a cluster using the resolved configuration.
- `slurm.config.load_environment(slurmfile=None, env=None)` returns a `SlurmfileEnvironment` object with the merged config, resolved path, and instantiated callbacks.
