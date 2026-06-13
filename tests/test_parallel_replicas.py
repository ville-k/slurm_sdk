"""Unit tests for Phase 5 replica sets.

Covers:

- :class:`Peer.replicas` validation hooks through construction.
- The submission-time args resolver (list / callable / range / None).
- :attr:`JobContext.replica_index` / :attr:`replica_count` population.
- :class:`ReplicaGroup` surface + ``ParallelJob`` tuple indexing.

These tests do not submit real jobs — they exercise the pure-Python pieces
of the replica pipeline plus the ``_RecordingBackend`` fake used elsewhere in
the parallel test suite.
"""

from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, List, Optional

import pytest

from slurm import task
from slurm._parallel_submission import _resolve_replica_items
from slurm.parallel import Peer, _build_spec, parallel
from slurm.parallel.plan import Plan
from slurm.parallel.rendering import PEER_REPLICA_ARGS_BASENAME
from slurm.parallel.topology_bootstrap import build_registry_skeleton
from slurm.parallel.validation import validate_spec
from slurm.parallel_job import ReplicaGroup
from slurm.runtime import build_job_context
from cluster_factory import make_test_cluster  # type: ignore


# ---------------------------------------------------------------------------
# Test-only tasks & cluster fixture (mirrors test_parallel_e2e_local.py)
# ---------------------------------------------------------------------------


@task(cpus_per_task=1, mem="512M")
def _inference(env_id: int = 0) -> dict:
    return {"env_id": env_id}


@task(cpus_per_task=1, mem="512M")
def _shared() -> None:
    pass


class _RecordingBackend:
    def __init__(self, job_base_dir: str):
        self.job_base_dir = job_base_dir
        self.submitted_script: Optional[str] = None
        self._counter = 0

    def is_remote(self) -> bool:
        return False

    def submit_job(
        self,
        script: str,
        *,
        target_job_dir: str,
        pre_submission_id: str,
        account: Optional[str] = None,
        partition: Optional[str] = None,
        array_spec: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        self.submitted_script = script
        os.makedirs(target_job_dir, exist_ok=True)
        with open(
            os.path.join(target_job_dir, f"slurm_job_{pre_submission_id}_script.sh"),
            "w",
        ) as fh:
            fh.write(script)
        self._counter += 1
        return str(8000 + self._counter)

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        return {"JobState": "PENDING"}

    def get_cluster_info(self) -> Dict[str, Any]:
        return {"partitions": []}

    def cancel_job(self, job_id: str) -> bool:
        return True

    def get_queue(self) -> List[Dict[str, Any]]:
        return []

    def close(self) -> None:
        return None


@pytest.fixture
def cluster(tmp_path):
    backend = _RecordingBackend(str(tmp_path))
    return make_test_cluster(
        backend=backend,
        backend_type="local",
        job_base_dir=str(tmp_path),
        default_packaging="none",
    )


# ---------------------------------------------------------------------------
# _resolve_replica_items — args generator shapes
# ---------------------------------------------------------------------------


def _replica_peer(**kwargs) -> Peer:
    return Peer.replicas(_inference, **kwargs)


def test_resolve_replica_items_none_yields_empty_dicts():
    peer = _replica_peer(count=3, args=None)
    items = _resolve_replica_items(peer)
    assert items == [{}, {}, {}]


def test_resolve_replica_items_range_yields_indices():
    peer = _replica_peer(count=4, args=range(4))
    items = _resolve_replica_items(peer)
    assert items == [0, 1, 2, 3]


def test_resolve_replica_items_list_of_dicts():
    peer = _replica_peer(
        count=3,
        args=[{"env_id": 0}, {"env_id": 10}, {"env_id": 20}],
    )
    items = _resolve_replica_items(peer)
    assert items == [{"env_id": 0}, {"env_id": 10}, {"env_id": 20}]


def test_resolve_replica_items_callable_invoked_per_index():
    seen: List[int] = []

    def _builder(i: int) -> dict:
        seen.append(i)
        return {"env_id": i * 100}

    peer = _replica_peer(count=3, args=_builder)
    items = _resolve_replica_items(peer)
    assert items == [{"env_id": 0}, {"env_id": 100}, {"env_id": 200}]
    assert seen == [0, 1, 2]


def test_resolve_replica_items_callable_returning_tuple():
    peer = _replica_peer(count=2, args=lambda i: (f"host-{i}", i))
    items = _resolve_replica_items(peer)
    assert items == [("host-0", 0), ("host-1", 1)]


def test_resolve_replica_items_rejects_singleton():
    peer = Peer(_inference)  # count == 1
    with pytest.raises(RuntimeError, match="singleton peer"):
        _resolve_replica_items(peer)


# ---------------------------------------------------------------------------
# Validation — length vs count (should already be caught by Phase 1)
# ---------------------------------------------------------------------------


def test_validation_rejects_arg_list_count_mismatch():
    # Lists with wrong length are flagged by validation.
    peer = _replica_peer(count=3, args=[{"env_id": 0}, {"env_id": 1}])
    spec = _build_spec(
        positional=(peer,),
        named={},
        topology=None,
        time=None,
        account=None,
        qos=None,
        reservation=None,
        network=None,
        grace_period_seconds=10,
    )
    with pytest.raises(Exception, match="len 2"):
        validate_spec(spec)


# ---------------------------------------------------------------------------
# JobContext.replica_index / replica_count
# ---------------------------------------------------------------------------


def test_build_job_context_populates_replica_fields():
    env = {
        "SLURM_JOB_ID": "111",
        "SLURM_PROCID": "2",
        "SLURM_SDK_PEER_NAME": "worker",
        "SLURM_SDK_REPLICA_COUNT": "4",
    }
    ctx = build_job_context(env=env)
    assert ctx.replica_index == 2
    assert ctx.replica_count == 4
    assert ctx.peer_name == "worker"


def test_build_job_context_no_replica_fields_for_singleton():
    env = {
        "SLURM_JOB_ID": "111",
        "SLURM_PROCID": "0",
        "SLURM_SDK_PEER_NAME": "solo",
    }
    ctx = build_job_context(env=env)
    assert ctx.replica_index is None
    assert ctx.replica_count is None


# ---------------------------------------------------------------------------
# End-to-end submission via fake backend
# ---------------------------------------------------------------------------


def test_replica_peer_submits_and_returns_replica_group(cluster):
    with cluster:
        job = parallel(
            Peer.replicas(
                _inference,
                count=3,
                args=lambda i: {"env_id": i},
            ),
        )

    # Job dict has one entry at the peer name; tuple indexing selects a single
    # replica.
    group = job["_inference"]
    assert isinstance(group, ReplicaGroup)
    assert len(group) == 3
    # Per-replica Jobs share the base job id but have distinct pre-submission ids.
    ids = [job["_inference", i].pre_submission_id for i in range(3)]
    assert len(set(ids)) == 3
    assert all("_peer__inference_" in (pid or "") for pid in ids)
    # ReplicaGroup iteration matches tuple indexing.
    for i, replica_job in enumerate(group):
        assert replica_job is job["_inference", i]


def test_replica_peer_rejects_tuple_index_on_singleton(cluster):
    with cluster:
        job = parallel(_shared)
    with pytest.raises(TypeError, match="not a replica set"):
        _ = job["_shared", 0]


def test_replica_peer_rejects_out_of_range_index(cluster):
    with cluster:
        job = parallel(Peer.replicas(_inference, count=2, args=range(2)))
    with pytest.raises(KeyError, match="out of range"):
        _ = job["_inference", 5]


def test_replica_script_renders_per_index_heredocs(cluster):
    backend = cluster.backend
    with cluster:
        parallel(
            Peer.replicas(
                _inference,
                count=3,
                args=[{"env_id": 10}, {"env_id": 20}, {"env_id": 30}],
            ),
        )
    script = backend.submitted_script
    assert script is not None

    # Each replica index has its own heredoc with a unique filename.
    for i in range(3):
        filename = PEER_REPLICA_ARGS_BASENAME.format(name="_inference", index=i)
        assert filename in script
        label = f"BASE64_PEER_REPLICA__INFERENCE_{i}"
        # Label appears at heredoc open and close.
        assert script.count(label) == 2

    # The srun command uses --ntasks=3 and the :by-taskid step selector.
    m = re.search(
        r'base64 -d > "plan\.json" << "BASE64_PARALLEL_PLAN"\n(.*?)\nBASE64_PARALLEL_PLAN',
        script,
        re.DOTALL,
    )
    assert m is not None
    plan = Plan.from_json(base64.b64decode(m.group(1).strip()).decode("utf-8"))
    assert plan.peers[0].replica_count == 3
    assert "--ntasks=3" in plan.peers[0].srun_command_line
    assert "--step peer:_inference:by-taskid" in plan.peers[0].srun_command_line
    # Replica peers export SLURM_SDK_REPLICA_COUNT so the runtime sees the
    # replica context.
    assert "SLURM_SDK_REPLICA_COUNT=3" in plan.peers[0].srun_command_line


def test_replica_script_decodes_heredoc_payload(cluster, tmp_path):
    """Decode a replica heredoc back to its dict payload end-to-end."""
    from slurm._serialization import loads_pickled

    backend = cluster.backend
    with cluster:
        parallel(
            Peer.replicas(
                _inference,
                count=2,
                args=[{"env_id": 7}, {"env_id": 42}],
            ),
        )
    script = backend.submitted_script
    assert script is not None

    for index, expected in enumerate([{"env_id": 7}, {"env_id": 42}]):
        label = f"BASE64_PEER_REPLICA__INFERENCE_{index}"
        m = re.search(
            rf'base64 -d > "peer__inference_{index}_args\.pkl" << "{label}"\n'
            rf"(.*?)\n{label}",
            script,
            re.DOTALL,
        )
        assert m is not None, f"missing heredoc for replica {index}"
        payload = loads_pickled(base64.b64decode(m.group(1).strip()))
        assert payload == expected


# ---------------------------------------------------------------------------
# Bootstrap skeleton — one registry entry per replica
# ---------------------------------------------------------------------------


def test_bootstrap_emits_count_entries_per_replica_peer():
    from slurm.parallel.plan import PlanPeer

    plan = Plan(
        peers=[
            PlanPeer(
                name="worker",
                pool="default",
                leader=False,
                on_failure="kill",
                srun_command_line="true",
                replica_count=4,
            ),
            PlanPeer(
                name="solo",
                pool="default",
                leader=False,
                on_failure="kill",
                srun_command_line="true",
                replica_count=1,
            ),
        ],
        grace_period_seconds=5,
        pool_names=["default"],
    )
    skeleton = build_registry_skeleton(plan, hostnames=("node-0",))
    assert len(skeleton["peers"]["worker"]) == 4
    assert [e["replica_index"] for e in skeleton["peers"]["worker"]] == [
        0,
        1,
        2,
        3,
    ]
    assert all(e["replica_count"] == 4 for e in skeleton["peers"]["worker"])
    assert len(skeleton["peers"]["solo"]) == 1
