# Cosmos-RL Qwen3 GRPO Async Showcase Design

## Goal

Build a Slurm SDK example that launches a Cosmos-RL Qwen3 GRPO async training run as one heterogeneous Slurm allocation:

- A controller peer on a small CPU or utility node
- Policy training replicas on H100-class nodes
- Rollout replicas on cheaper A100/L40S-class nodes
- Optional telemetry/profiling peers that do not affect training if they fail

The example should showcase the new heterogeneous job support by making the SDK own the Slurm allocation shape, role placement, service discovery, and failure policy. Cosmos-RL should own the RL runtime internals after each role starts.

This should not call Cosmos-RL's existing `tools/slurm/dispatch_job.py` from inside a Slurm SDK job. That would hide the new SDK features behind a second Slurm launcher.

The public example should also avoid repo-path and `.sqsh`-path arguments. It should assume Cosmos-RL is available in the task runtime through the cluster's base environment or through SDK packaging configured outside the example. Internally, the role wrappers may still invoke Cosmos-RL's launcher or distributed entrypoint as a child process, because real policy and rollout roles need `torchrun`/rank process management. That process boundary should be a private implementation detail, not the shape users configure.

## Source Context

Cosmos-RL's docs are a strong match for this example:

- Cosmos-RL async mode separates policy training from rollout generation and supports independently scaling policy and rollout workers.
- The async docs explicitly describe policy trainers and rollout actors as separate worker pools that can run on different hardware, such as H100s for policy and cheaper L40S-class GPUs for rollout.
- The manual multi-node docs launch a controller first, then join policy and rollout replicas by setting `COSMOS_CONTROLLER_HOST`.
- The Slurm docs show a Qwen3 GRPO launch using `configs/qwen3/qwen3-32b-p-fsdp1-tp8-r-tp4-pp1-grpo.toml` with `--n-policy-replicas` and `--n-rollout-replicas`.
- The single-node docs show the smaller `configs/qwen3/qwen3-8b-p-tp4-r-tp2-pp1-grpo.toml` profile, where one policy replica and two rollout replicas use 8 total GPUs.

References:

- [Cosmos-RL async overview](https://nvidia-cosmos.github.io/cosmos-rl/async/overview.html)
- [Cosmos-RL multi-node example](https://nvidia-cosmos.github.io/cosmos-rl/multinodes/overview.html)
- [Cosmos-RL Slurm job docs](https://nvidia-cosmos.github.io/cosmos-rl/multinodes/slurm.html)
- [Cosmos-RL single-node Qwen3 example](https://nvidia-cosmos.github.io/cosmos-rl/quickstart/single_node_example.html)

## Non-Goals

- Do not reimplement Cosmos-RL's controller, trainers, rollout actors, reward logic, or weight synchronization.
- Do not teach the SDK to parse every Cosmos-RL TOML option in the first version.
- Do not require a successful full Qwen3-32B training run for unit tests.
- Do not depend on the Cosmos-RL Slurm dispatcher.
- Do not eliminate Cosmos-RL's process/rank launcher. Replacing `torchrun` or Cosmos role launch internals would make the example more fragile, not more native.
- Do not make users pass the Cosmos-RL repository path or a `.sqsh` image path to the showcase CLI.
- Do not make the example the only path for Cosmos-RL users. It is a showcase and adapter, not a replacement for Cosmos-RL's launchers.

## Example Surface

Add one example module:

```text
src/slurm/examples/cosmos_rl_qwen3_async.py
```

Optional follow-up docs page:

```text
docs/how-to/cosmos_rl_qwen3_async.md
```

The example should be executable as:

```bash
uv run python -m slurm.examples.cosmos_rl_qwen3_async \
  --backend ssh \
  --hostname login.example.edu \
  --username alice \
  --job-base-dir /shared/alice/slurm-jobs \
  --account research \
  --time 24:00:00 \
  --profile qwen3-32b-grpo-async \
  --output-root /shared/alice/cosmos-outputs \
  --policy-replicas 1 \
  --policy-gpus-per-replica 8 \
  --policy-pool-nodes 1 \
  --policy-partition h100 \
  --policy-gpu-type h100 \
  --rollout-replicas 2 \
  --rollout-gpus-per-replica 4 \
  --rollout-pool-nodes 1 \
  --rollout-partition a100 \
  --rollout-gpu-type a100
```

Also support a small smoke-test profile:

```bash
uv run python -m slurm.examples.cosmos_rl_qwen3_async \
  --profile qwen3-8b-smoke \
  --policy-replicas 1 \
  --policy-gpus-per-replica 4 \
  --rollout-replicas 2 \
  --rollout-gpus-per-replica 2
```

The smoke profile is useful because it matches Cosmos-RL's documented single-node Qwen3-8B shape while still letting us place policy and rollout on different pools for the showcase.

### Runtime Assumption

The example should start from a native Python surface:

```python
from slurm.examples.cosmos_rl_qwen3_async import CosmosQwen3AsyncConfig, submit_showcase

config = CosmosQwen3AsyncConfig(
    profile="qwen3-32b-grpo-async",
    output_root="/shared/alice/cosmos-outputs",
    policy_replicas=1,
    rollout_replicas=2,
)

job = submit_showcase(cluster, config)
```

Cosmos-RL must be importable inside every peer runtime. The recommended setup is one of:

- The cluster environment already has `cosmos-rl` installed and the example uses `packaging="none"`.
- The caller configures `Cluster(default_packaging="container:<image>")` or task-level container packaging outside the showcase.
- A site-specific wrapper script loads modules before running the Python example.

The showcase itself should not accept `--cosmos-repo` or `--cosmos-container` as primary arguments. A later escape hatch can allow extra environment variables or role launcher overrides, but the main path should be profile-driven and SDK-native.

## Topology

The default Qwen3-32B showcase topology should be:

```python
Topology(
    pools={
        "control": Pool(
            nodes=1,
            cpus_per_node=32,
            mem_per_node="128G",
            partition=args.control_partition,
            node_labels=["ctrl"],
        ),
        "policy": Pool(
            nodes=args.policy_pool_nodes,
            gpus_per_node=args.policy_gpus_per_node,
            cpus_per_node=128,
            mem_per_node="1T",
            partition=args.policy_partition,
            gpu_type=args.policy_gpu_type,
            node_labels=[f"policy-{i}" for i in range(args.policy_pool_nodes)],
            exclusive=True,
        ),
        "rollout": Pool(
            nodes=args.rollout_pool_nodes,
            gpus_per_node=args.rollout_gpus_per_node,
            cpus_per_node=96,
            mem_per_node="512G",
            partition=args.rollout_partition,
            gpu_type=args.rollout_gpu_type,
            node_labels=[f"rollout-{i}" for i in range(args.rollout_pool_nodes)],
            exclusive=True,
        ),
    },
    network=args.slurm_network,
)
```

This demonstrates:

- Multi-pool heterogeneous Slurm jobs
- Different partitions and GPU types for policy and rollout
- Stable node labels
- Replica placement inside role-specific pools
- Optional network constraints for IB/EFA-style clusters

## Peer Model

The SDK allocation should contain these peers:

| Peer         | Pool      | Shape                                      | Failure policy          | Purpose                         |
| ------------ | --------- | ------------------------------------------ | ----------------------- | ------------------------------- |
| `controller` | `control` | 1 CPU-ish task, one auto-reserved port     | leader, kill allocation | Starts Cosmos-RL controller     |
| `policy`     | `policy`  | `policy_replicas`, N GPUs per replica      | kill allocation         | Runs policy trainer replicas    |
| `rollout`    | `rollout` | `rollout_replicas`, M GPUs per replica     | restart, then kill      | Runs rollout actor replicas     |
| `telemetry`  | any       | optional CPU sidecar, usually per GPU node | continue                | Logs environment and GPU health |

The controller peer should be the unique `leader=True` peer. When it exits, the SDK supervisor should shut down policy, rollout, and telemetry peers.

Rollout peers should start with `on_failure="restart", max_restarts=2` because rollout workers are the most likely role to hit transient data, engine, or model-serving failures. Policy failures should kill the allocation because they usually invalidate the run.

## Runtime Coordination

Use the SDK registry for the controller endpoint:

1. `controller` reserves a port with `@task(ports={"controller": "auto"})`.
1. `controller` announces `endpoint="<hostname>:<port>"`.
1. `policy` and `rollout` peers wait on `ctx.peers["controller"].wait_all(keys=["endpoint"])`.
1. Each worker sets `COSMOS_CONTROLLER_HOST` from the announced endpoint.
1. Each worker invokes the configured Cosmos role entrypoint through a private `run_cosmos_role(...)` helper.

This mirrors Cosmos-RL's manual multi-node flow while replacing hand-managed hostnames with SDK service discovery.

The distinction is important: the SDK manages Slurm and cross-role discovery; Cosmos-RL still manages distributed rank launch and role-specific runtime behavior.

## Task Sketch

The implementation should use small Python tasks around a shared Cosmos launch helper. The task functions are SDK-native: they receive typed config, use `JobContext`, announce/read peer metadata, and return structured results. The only subprocess logic should live inside `run_cosmos_role(...)`.

```python
@task(cpus_per_task=8, mem="32G", ports={"controller": "auto"})
def cosmos_controller(spec: CosmosSpec, ctx: JobContext) -> dict:
    port = ctx.my_ports["controller"]
    endpoint = f"{ctx.node.hostname}:{port}"
    ctx.announce(endpoint=endpoint, ready=True, role="controller")

    return run_cosmos_role(
        role="controller",
        spec=spec,
        ctx=ctx,
        env={"COSMOS_CONTROLLER_PORT": str(port)},
    )
```

```python
@task(gpus_per_task=8, cpus_per_task=32, mem="512G")
def cosmos_policy_replica(replica_id: int, spec: CosmosSpec, ctx: JobContext) -> dict:
    ctx.peers["controller"].wait_all(keys=["endpoint"], timeout=600)
    controller = ctx.peers["controller"].first

    env = base_cosmos_env(spec, ctx)
    env["COSMOS_CONTROLLER_HOST"] = controller.metadata["endpoint"]

    ctx.announce(role="policy", replica_id=replica_id, launching=True)
    return run_cosmos_role(role="policy", spec=spec, ctx=ctx, env=env)
```

```python
@task(gpus_per_task=4, cpus_per_task=16, mem="256G")
def cosmos_rollout_replica(replica_id: int, spec: CosmosSpec, ctx: JobContext) -> dict:
    ctx.peers["controller"].wait_all(keys=["endpoint"], timeout=600)
    controller = ctx.peers["controller"].first

    env = base_cosmos_env(spec, ctx)
    env["COSMOS_CONTROLLER_HOST"] = controller.metadata["endpoint"]

    ctx.announce(role="rollout", replica_id=replica_id, launching=True)
    return run_cosmos_role(role="rollout", spec=spec, ctx=ctx, env=env)
```

`run_cosmos_role(...)` should:

- Validate that Cosmos-RL is importable or that a configured role launcher is available.
- Resolve the profile to a Cosmos-RL config path or package resource.
- Build the environment expected by Cosmos-RL, including `COSMOS_CONTROLLER_HOST`.
- Spawn the Cosmos role launcher with `subprocess.Popen(..., start_new_session=True)`.
- Stream stdout/stderr to the peer log.
- On `ctx.shutdown_requested`, send `SIGTERM`, wait a grace period, then send `SIGKILL`.
- Return the command, exit code, hostname, peer name, and replica index.
- Raise on non-zero exit unless the caller intentionally tolerates that peer's failure policy.

The helper is intentionally process-based. Trying to import and directly call Cosmos-RL's policy or rollout internals would make the showcase depend on non-public APIs and would bypass the distributed launcher behavior needed for multi-GPU replicas.

## Submission Sketch

```python
with Cluster(
    backend_type=args.backend,
    hostname=args.hostname,
    username=args.username,
    job_base_dir=args.job_base_dir,
    default_account=args.account,
) as cluster:
    job = parallel(
        Peer(
            cosmos_controller.partial(spec=spec),
            name="controller",
            pool="control",
            on_node="ctrl",
            leader=True,
        ),
        Peer.replicas(
            cosmos_policy_replica.partial(spec=spec),
            name="policy",
            count=args.policy_replicas,
            pool="policy",
            args=lambda i: {"replica_id": i},
            tasks_per_node=max(1, args.policy_gpus_per_node // args.policy_gpus_per_replica),
            srun_args=args.policy_srun_args,
        ),
        Peer.replicas(
            cosmos_rollout_replica.partial(spec=spec),
            name="rollout",
            count=args.rollout_replicas,
            pool="rollout",
            args=lambda i: {"replica_id": i},
            tasks_per_node=max(1, args.rollout_gpus_per_node // args.rollout_gpus_per_replica),
            on_failure="restart",
            max_restarts=2,
            srun_args=args.rollout_srun_args,
        ),
        Peer.replicas(
            cosmos_telemetry.partial(spec=spec),
            name="telemetry",
            count=args.telemetry_replicas,
            pool="rollout",
            args=lambda i: {"replica_id": i},
            on_failure="continue",
        ),
        topology=topology,
        time=args.time,
        account=args.account,
    )

    job.wait()
    print(job.peer_outcomes())
```

The example should assume the cluster runtime already contains Cosmos-RL. Users who need containers should configure `Cluster(default_packaging=...)` in Python or through their site wrapper, outside the showcase's own CLI. That keeps the example focused on heterogeneous orchestration rather than container provisioning.

## CLI Parameters

Required:

- `--profile`, with built-in values such as `qwen3-8b-smoke` and `qwen3-32b-grpo-async`
- `--output-root`
- backend connection settings

Role sizing:

- `--policy-replicas`
- `--policy-gpus-per-replica`
- `--policy-pool-nodes`
- `--policy-gpus-per-node`
- `--policy-partition`
- `--policy-gpu-type`
- `--rollout-replicas`
- `--rollout-gpus-per-replica`
- `--rollout-pool-nodes`
- `--rollout-gpus-per-node`
- `--rollout-partition`
- `--rollout-gpu-type`

Operational:

- `--account`
- `--time`
- `--slurm-network`
- `--dry-run`
- `--extra-env KEY=VALUE`, repeatable
- `--config-path`, optional escape hatch for a custom Cosmos-RL TOML file already visible inside the peer runtime
- `--role-launcher`, optional escape hatch for site-specific Cosmos launch wrappers
- `--policy-srun-arg`, repeatable
- `--rollout-srun-arg`, repeatable

The first version should require explicit GPU-per-replica values. A follow-up can infer them from Cosmos-RL TOML. Explicit values keep the showcase robust while the SDK is demonstrating scheduling rather than Cosmos config parsing.

Avoid adding user-facing `--cosmos-repo` or `--cosmos-container` arguments. They make the example look like a shell launcher wrapper rather than a Slurm SDK-native orchestration example.

## Dry-Run Mode

Add `--dry-run` to replace Cosmos commands with fake processes:

- Controller announces an endpoint and sleeps.
- Policy replicas wait for the controller endpoint, announce themselves, print role metadata, and sleep.
- Rollout replicas do the same.
- Telemetry writes one heartbeat per second.

This enables:

- Local backend testing without Cosmos-RL installed
- Service discovery tests
- Replica result collection tests
- `peer_outcomes()` validation
- Documentation screenshots/log snippets without burning GPU time

Dry-run mode should still use the same `Topology`, `Peer`, `Peer.replicas`, ports, and registry calls. Only the Cosmos role helper changes from launching real Cosmos-RL roles to running a short fake role loop.

## Testing Plan

Unit tests:

- Topology sizing for Qwen3-8B and Qwen3-32B profiles.
- `tasks_per_node` calculation for policy and rollout pools.
- Profile-to-role-launch metadata resolution.
- Environment construction includes `COSMOS_CONTROLLER_HOST` only after discovery.
- `run_cosmos_role(...)` validates missing Cosmos-RL cleanly when not in dry-run.
- Invalid sizing fails before submission, for example rollout replicas requiring more GPUs than the rollout pool provides.

Local integration test:

- Run `--dry-run --backend local` with 1 controller, 1 policy replica, 2 rollout replicas, and 1 telemetry peer.
- Assert `job.peer_outcomes()` contains every peer and all non-telemetry peers complete successfully.
- Assert policy and rollout logs include the discovered controller endpoint.

Slurm smoke test:

- Use the Qwen3-8B config with 1 policy replica and 2 rollout replicas.
- Keep runtime short if Cosmos-RL supports a small training-step override.
- Verify `squeue` shows heterogeneous job components.
- Verify policy and rollout peers land in the intended partitions.

Full showcase test:

- Use the documented Qwen3-32B GRPO config.
- Place policy on H100 and rollout on A100/L40S.
- Verify Cosmos-RL logs show policy progress, rollout activity, and weight synchronization.

## Documentation Plan

The public write-up should be a how-to guide, not a tutorial:

- Problem: Cosmos-RL async training has distinct controller, policy, and rollout roles with different hardware needs.
- Solution: map those roles to SDK pools and peers.
- Code: trimmed but complete script.
- Verification: `squeue`, `job.peer_outcomes()`, registry inspection, and Cosmos-RL log lines.

Troubleshooting topics:

- Controller endpoint not discovered
- Cosmos-RL is not importable in the peer runtime
- Custom config path is not visible inside the peer runtime
- Rollout replicas restart repeatedly
- GPU count in CLI does not match Cosmos config
- Cross-node replica needs `--rdzv-endpoint`

## Risks and Open Questions

- Cosmos-RL role launchers may expect additional environment variables that `dispatch_job.py` currently provides. Implementation should inspect `tools/slurm/dispatch_job.py` and mirror only the role-launch environment, not the Slurm submission logic.
- Replicas that require more than one node need `--rdzv-endpoint` according to Cosmos-RL docs. The first version should avoid cross-node single replicas and document that limitation.
- Cosmos-RL may not expose a stable Python API for launching controller, policy, and rollout roles. The design should keep `run_cosmos_role(...)` narrow and treat the process boundary as intentional.
- Readiness is currently approximate. The wrapper can announce "launching" before the role launcher starts; a stronger version should watch logs or use a Cosmos health endpoint before announcing `ready=True`.
- If Cosmos-RL expects all roles in one homogeneous partition, its internals may expose assumptions. The showcase should start with the manual deployment scripts because they already support separate nodes.

## Success Criteria

- One SDK `parallel(...)` call reserves controller, policy, and rollout pools atomically.
- The generated Slurm job has multiple heterogeneous components.
- Policy and rollout workers discover the controller through `ctx.peers`, not hardcoded hostnames.
- Policy and rollout can use different Slurm partitions and GPU types.
- Rollout replicas are launched with `Peer.replicas`.
- Rollout failure policy demonstrates restart behavior without custom Slurm scripting.
- Optional telemetry failure does not fail training.
- The example has a no-GPU dry-run mode for CI and documentation.
- The design remains compatible with a future simplified SDK where direct `Cluster(...)` construction and prebuilt container images are the recommended path.
