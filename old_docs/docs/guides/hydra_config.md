# Manage SLURM settings with Hydra

Hydra excels at composing hierarchical configuration for machine learning
projects. You can use it to keep the values that normally live in `Slurmfile.toml`
in the same configuration tree that drives your experiment logic. This guide
shows a minimal pattern that keeps local development, staging, and production
cluster options side-by-side with the rest of your Hydra config.

## 1. Define a Hydra config schema

Create a config package (for example `conf/cluster.yaml`) that captures the
settings you would normally place inside a Slurmfile:

```yaml title="conf/cluster.yaml"
defaults:
  - cluster: local

cluster:
  local:
    backend_type: ssh
    backend:
      hostname: mac.local
      username: ${oc.env:USER}
    submit:
      partition: debug
      account: research
  gpu:
    backend_type: ssh
    backend:
      hostname: gpu-login.mycluster
      username: ${oc.env:USER}
    submit:
      partition: gpu
      account: research-gpu
```

Each node under `cluster` represents an environment. You can use Hydra's
composition system to override the default on the CLI:

```
$ uv run python train.py cluster=gpu
```

## 2. Bridge Hydra config to the SDK

Inside your task entry point, resolve the Hydra config and map it into the
`Cluster` constructor. The example below uses the same structure as the
Slurmfile but stays entirely within Hydra.

```python title="train.py"
import hydra
from omegaconf import DictConfig
from slurm.callbacks import LoggerCallback
from slurm.cluster import Cluster
from slurm.decorators import task


@task(time="02:00:00", gpus_per_node=1)
def train(cfg: DictConfig) -> None:
    # Your training logic that already consumes the Hydra config
    ...


@hydra.main(version_base="1.3", config_path="conf", config_name="cluster")
def main(cfg: DictConfig) -> None:
    cluster_cfg = cfg.cluster
    backend_cfg = cluster_cfg.backend

    callbacks = [LoggerCallback()]  # optionally inject rich console here

    cluster = Cluster(
        backend_type=cluster_cfg.backend_type,
        callbacks=callbacks,
        **backend_cfg,
    )

    submit_defaults = cluster_cfg.submit

    job = train.submit(
        cluster=cluster,
        account=submit_defaults.account,
        partition=submit_defaults.partition,
    )(cfg)

    job.wait()


if __name__ == "__main__":
    main()
```

The task receives the Hydra config (or a subset of it) which means you do not
have to mirror the same values in both Hydra and Slurmfile.

## 3. Keep the Slurmfile minimal (optional)

You can still provide a tiny Slurmfile that simply points to Hydra for the
cluster configuration. Many teams keep a single environment in the Slurmfile
for legacy tools and call into Hydra in their Python entry points:

```toml title="Slurmfile.toml"
[default]
cluster = { backend = "ssh", backend_config = { hostname = "127.0.0.1" } }
```

From here you can gradually move everything into Hydra and eventually delete the
Slurmfile once your pipeline no longer relies on it.

### Tips

- Use Hydra's `defaults` list to set the cluster environment per experiment.
- Add validation with [pydantic-hydra](https://github.com/adriangb/pydantic-hydra)
  or dataclasses when configs grow large.
- When running on CI, pin the cluster override via `HYDRA_FULL_ERROR=1
  python train.py cluster=ci` to keep logs predictable.
