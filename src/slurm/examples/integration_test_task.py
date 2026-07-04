"""Integration test tasks.

This module contains simple task definitions used by integration tests.
It is included in the slurm package wheel so tasks can be imported when
running remotely on Slurm clusters.

Note: This is NOT a user-facing example. It exists here (rather than in
tests/) because it must be importable on remote clusters.
"""

from pathlib import Path
from slurm.decorators import task
from slurm.runtime import JobContext


@task(time="00:01:00", mem="100M")
def simple_integration_task() -> str:
    """A simple task that returns a success message for integration testing."""
    return "integration-ok"


@task(time="00:01:00", mem="100M")
def write_output_file_task(message: str, job: JobContext) -> Path:
    """Write a message to a file in the output directory and return its path.

    Args:
        message: The message to write to the file
        job: JobContext with output_dir set

    Returns:
        Path to the written file
    """
    if job.output_dir is None:
        raise RuntimeError("output_dir is not set in JobContext")

    output_file = job.output_dir / "test_output.txt"
    output_file.write_text(f"Message: {message}\nJob ID: {job.job_id}\n")

    import json

    json_file = job.output_dir / "metadata.json"
    metadata = {
        "message": message,
        "job_id": job.job_id,
        "output_dir": str(job.output_dir),
        "rank": job.rank,
        "world_size": job.world_size,
    }
    json_file.write_text(json.dumps(metadata, indent=2))

    return output_file


@task(time="00:01:00", mem="100M")
def get_output_dir_task(job: JobContext) -> dict:
    """Return information about the output directory.

    Args:
        job: JobContext with environment and output_dir

    Returns:
        Dictionary with output_dir information
    """
    return {
        "output_dir": str(job.output_dir) if job.output_dir else None,
        "output_dir_exists": job.output_dir.exists() if job.output_dir else False,
        "job_id": job.job_id,
        "job_dir_env": job.environment.get("SLURM_JOB_DIR"),
    }


# Array job test tasks
@task(time="00:01:00", mem="100M")
def process_string_item(item: str) -> str:
    """Process a single string item and return uppercase."""
    return item.upper()


@task(time="00:01:00", mem="100M")
def add_two_numbers(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@task(time="00:01:00", mem="100M")
def multiply_with_default(x: int, y: int = 2) -> int:
    """Multiply x by y (default 2)."""
    return x * y


@task(time="00:01:00", mem="100M")
def prepare_data() -> str:
    """Prepare initial data."""
    return "prepared"


@task(time="00:01:00", mem="100M")
def process_item_simple(item: int) -> str:
    """Process an item."""
    return f"processed_{item}"


@task(time="00:01:00", mem="100M")
def slow_multiply_task(x: int) -> int:
    """Slow task that returns x * 2."""
    import time

    time.sleep(1)
    return x * 2


# ---------------------------------------------------------------------------
# Multi-node integration tasks (used by tests/integration/test_multi_node.py)
# ---------------------------------------------------------------------------


@task(time="00:01:00", mem="100M")
def multi_node_identity_task() -> dict:
    """Return ``socket.gethostname()`` plus ``SLURM_JOB_NODELIST``.

    Used by the multi-node integration tests (which apply
    ``.with_options(nodelist=...)`` or ``.with_options(nodes=2)`` per
    test case) to verify that the existing single-task ``@task`` path
    honours node-pinning and multi-node-allocation directives against
    a real Slurm cluster with two compute nodes.
    """
    import os
    import socket

    return {
        "hostname": socket.gethostname(),
        "nodelist": os.environ.get("SLURM_JOB_NODELIST"),
        "nnodes": os.environ.get("SLURM_JOB_NUM_NODES"),
    }


# ---------------------------------------------------------------------------
# parallel(...) integration tasks
#
# Tasks used by tests/integration/test_parallel_end_to_end.py. They live here
# (not in tests/) because remote peer processes import them by module name.
# ---------------------------------------------------------------------------


@task(time="00:01:00", mem="100M")
def parallel_named_peer_task(name: str) -> str:
    """Return ``"peer:<name>"`` — minimal round-trip identity."""
    return f"peer:{name}"


@task(time="00:01:00", mem="100M")
def parallel_leader_task() -> int:
    """Deterministic leader result for leader+sidecar integration tests."""
    return 42


@task(time="00:03:00", mem="100M")
def parallel_long_lived_sidecar_task() -> str:
    """Sleep long enough that a leader-exit cascade is observable.

    Paired with ``parallel_leader_task`` in the cascade test: if the
    supervisor's ``leader=True`` shutdown signal is broken, the sidecar
    runs to natural completion and the allocation's elapsed time pokes
    past this sleep. The ``on_failure="continue"`` policy on the peer
    means a SIGTERM-driven exit still registers as a
    ``shutdown_by_leader`` outcome rather than aborting the group.
    """
    import time

    time.sleep(120)
    return "side-ran-to-completion"


@task(time="00:01:00", mem="100M")
def parallel_replica_identity_task(ctx: JobContext) -> dict:
    """Return per-replica scheduler-populated identity — SLURM_PROCID etc.

    Used to prove the runner sees a real Slurm-populated environment for
    ``:by-taskid`` dispatch (not a synthesized one) and that the
    per-replica result file picks up PROCID correctly.
    """
    import os
    import socket

    return {
        "replica_index": ctx.replica_index,
        "replica_count": ctx.replica_count,
        "procid": os.environ.get("SLURM_PROCID"),
        "ntasks": os.environ.get("SLURM_NTASKS"),
        "hostname": socket.gethostname(),
    }


@task(time="00:02:00", mem="100M")
def parallel_coordinator_task(ctx: JobContext) -> dict:
    """Coordinator — announces its endpoint, then exits.

    Mirrors the service-discovery how-to recipe. For the integration test
    we don't need to actually serve traffic; the ``announce()`` itself is
    the scheduler-agnostic surface we care about.
    """
    import socket

    port = 29555
    endpoint = f"tcp://{socket.gethostname()}:{port}"
    ctx.announce(ready=True, endpoint=endpoint, role="coordinator")
    return {"endpoint": endpoint, "port": port}


@task(time="00:02:00", mem="100M")
def parallel_worker_task(ctx: JobContext) -> dict:
    """Worker — blocks on the coordinator's ``endpoint`` via ``wait_all``.

    Proves bootstrap has written the registry, the coordinator's runner has
    published its hostname, and ``wait_all(keys=["endpoint"])`` can pick
    up an announced metadata key from a sibling peer running under real
    Slurm.
    """
    ctx.peers["coordinator"].wait_all(keys=["endpoint"], timeout=60)
    coord = ctx.peers["coordinator"].first
    return {
        "observed_endpoint": coord.metadata.get("endpoint"),
        "observed_role": coord.metadata.get("role"),
        "coordinator_state": coord.state,
        "coordinator_hostname": coord.hostname,
    }


@task(time="00:01:00", mem="100M")
def parallel_upstream_token() -> str:
    """Produce a token used as proof of an upstream dependency running first."""
    return "upstream-token"


@task(time="00:02:00", mem="100M")
def parallel_slow_upstream_task() -> str:
    """Upstream that runs long enough to observe downstream in a dep-gated state.

    The dependency integration test submits this task, then submits a
    downstream ``parallel(...).after(upstream)`` and queries
    ``scontrol show job`` while this is still running. The sleep gives
    headroom for scheduler latency between the resubmit and the query.
    """
    import time

    time.sleep(20)
    return "slow-upstream-token"


@task(time="00:01:00", mem="100M")
def parallel_downstream_peer_task() -> str:
    """Downstream peer — just proves ``parallel(...).after(upstream)`` ran."""
    return "downstream-ok"
