# Configure a Cluster from a Slurmfile

Learn how to run jobs using `Cluster.from_env` and a project Slurmfile.

## Prerequisites

- Python environment with the SDK installed (e.g., `uv pip install -e .`).
- SSH credentials for the target cluster, captured in your `Slurmfile`.
- Optional: the example configuration at `src/slurm/examples/Slurmfile.example.toml`.

## 1. Create or update a Slurmfile

1. Copy the example template:
   ```bash
   cp src/slurm/examples/Slurmfile.example.toml Slurmfile
   ```
2. Edit the file to match your environment. At minimum provide:
   - `cluster.backend` (currently `ssh`)
   - `cluster.backend_config.hostname` and `username`
   - `submit.account` and `submit.partition`

## 2. Select the environment

- Use the `default` table for primary settings and create additional environments (e.g., `[local]`) as needed.
- Either set `SLURM_ENV=local` or pass `--env local` when invoking scripts.

## 3. Run the example

1. Ensure SSH access works (key or agent configured).
2. Execute the hello-world example using the Slurmfile:
   ```bash
   uv run python -m slurm.examples.hello_slurmfile Slurmfile --env default
   ```
3. The script loads the configuration, submits the task, waits for completion, and prints the result.

## Troubleshooting tips

- Missing files raise `SlurmfileNotFoundError`. Confirm the path exists or set `SLURMFILE`.
- Schema issues raise `SlurmfileInvalidError`. Validate table structure and key names.
- Authentication failures stem from SSH settings; reuse `ssh hostname` manually to verify access.
