"""JSON-serializable plan consumed by the parallel supervisor.

The submission pipeline writes one :class:`Plan` to ``$JOB_DIR/plan.json``
(via a base64 heredoc in the rendered batch script). The bootstrap and the
supervisor both read it:

- :mod:`slurm.parallel.topology_bootstrap` uses it for peer→pool mapping
  when seeding the registry skeleton.
- :mod:`slurm.parallel.topology_supervisor` uses it to launch each peer's
  ``srun`` and apply failure policies.

The schema is intentionally small and stable — phases 4+ add fields
(``max_restarts``, ``callback_module``, replica info, hetjob component
index), but the existing fields stay put so older bootstraps can read newer
plans in lockstep with the SDK version that wrote them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class PlanPeer:
    """One peer's entry in the plan.

    Attributes:
        name: Resolved peer name (matches ``PeerRegistryEntry.name``).
        pool: Pool the peer targets — one of :attr:`Plan.pool_names`.
        leader: Whether this peer is a leader. Leader exit (any outcome)
            triggers cascading shutdown of siblings.
        on_failure: ``"kill"`` / ``"continue"`` / ``"restart"`` / ``"callback"``.
        max_restarts: Maximum restart count for ``on_failure="restart"``.
            Ignored for other policies.
        callback: Fully-qualified name of the user's failure callback in
            ``"module:qualname"`` form, or ``None`` when ``on_failure`` is
            not ``"callback"``. The supervisor imports and resolves the
            callable at startup. We use a string because live callables
            cannot be pickled reliably across the process boundary between
            the submission host and the batch allocation.
        srun_command_line: The shell command that, when executed under
            ``bash -c``, launches this peer's ``srun`` step. Using a
            pre-rendered string (rather than an argv list) lets the shell
            expand variables like ``$PY_EXEC_RESOLVED`` / ``$JOB_DIR`` set
            by the surrounding batch script, which is how the runner's
            PYTHONPATH / python executable are resolved.
    """

    name: str
    pool: str
    leader: bool
    on_failure: str
    max_restarts: int
    srun_command_line: str
    callback: "str | None" = None
    # Replica count — ``1`` for singleton peers, ``> 1`` for replica sets
    # produced by :meth:`Peer.replicas`. The bootstrap uses this to size
    # the registry skeleton (``count`` entries per replica peer).
    replica_count: int = 1
    # Hetjob component index — 0-based position of the peer's pool in the
    # hetjob header sequence. Single-pool submissions leave this at ``0``.
    # Drives ``srun --het-group=<N>`` at render time and the bootstrap's
    # ``SLURM_JOB_NODELIST_HET_GROUP_<N>`` lookup.
    component_index: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pool": self.pool,
            "leader": self.leader,
            "on_failure": self.on_failure,
            "max_restarts": self.max_restarts,
            "srun_command_line": self.srun_command_line,
            "callback": self.callback,
            "replica_count": self.replica_count,
            "component_index": self.component_index,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PlanPeer":
        return cls(
            name=data["name"],
            pool=data["pool"],
            leader=bool(data["leader"]),
            on_failure=data["on_failure"],
            max_restarts=int(data.get("max_restarts", 0)),
            srun_command_line=data["srun_command_line"],
            callback=data.get("callback"),
            replica_count=int(data.get("replica_count", 1)),
            component_index=int(data.get("component_index", 0)),
        )


@dataclass
class PlanComponent:
    """Per-hetjob-component metadata.

    Attributes:
        index: 0-based component index. Component ``0`` is the first ``#SBATCH``
            block in the rendered script.
        pool: The pool name this component materialises.
        nodes: Node count reserved for the component. Used by the bootstrap to
            size the hostname slice it assigns to this component.
    """

    index: int
    pool: str
    nodes: int

    def to_dict(self) -> dict:
        return {"index": self.index, "pool": self.pool, "nodes": self.nodes}

    @classmethod
    def from_dict(cls, data: dict) -> "PlanComponent":
        return cls(
            index=int(data["index"]),
            pool=str(data["pool"]),
            nodes=int(data.get("nodes", 1)),
        )


@dataclass
class Plan:
    """The full supervisor plan for one ``parallel(...)`` allocation.

    Attributes:
        peers: Peer declarations in declaration order.
        grace_period_seconds: SIGTERM-to-SIGKILL window during cascading
            shutdown.
        pool_names: Pool names in hetjob component order. Length ``1`` for
            single-pool submissions; equal to the number of components for
            hetjob submissions.
        pre_submission_id: SDK base id threaded through for logging.
        components: Per-component metadata. Length ``1`` for single-pool
            submissions; matches ``pool_names`` for hetjob submissions. Added
            in Phase 6 so the supervisor and bootstrap can drive per-component
            scancel + nodelist resolution without having to reconstruct the
            mapping from ``peers``.
    """

    peers: List[PlanPeer]
    grace_period_seconds: int
    pool_names: List[str]
    pre_submission_id: str = ""
    schema_version: int = 1
    components: Optional[List[PlanComponent]] = None

    def to_json(self) -> str:
        components = self.components
        if components is None:
            # Back-compat synthesis: one component per pool_name, same index.
            components = [
                PlanComponent(index=i, pool=name, nodes=1)
                for i, name in enumerate(self.pool_names)
            ]
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "peers": [p.to_dict() for p in self.peers],
                "grace_period_seconds": self.grace_period_seconds,
                "pool_names": list(self.pool_names),
                "pre_submission_id": self.pre_submission_id,
                "components": [c.to_dict() for c in components],
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, text: str) -> "Plan":
        data = json.loads(text)
        peers = [PlanPeer.from_dict(p) for p in data.get("peers", [])]
        pool_names = list(data.get("pool_names", []))
        raw_components = data.get("components")
        if raw_components is None:
            components = [
                PlanComponent(index=i, pool=name, nodes=1)
                for i, name in enumerate(pool_names)
            ]
        else:
            components = [PlanComponent.from_dict(c) for c in raw_components]
        return cls(
            peers=peers,
            grace_period_seconds=int(data.get("grace_period_seconds", 10)),
            pool_names=pool_names,
            pre_submission_id=str(data.get("pre_submission_id", "")),
            schema_version=int(data.get("schema_version", 1)),
            components=components,
        )

    def peer_by_name(self, name: str) -> PlanPeer:
        for peer in self.peers:
            if peer.name == name:
                return peer
        raise KeyError(name)

    def effective_components(self) -> List[PlanComponent]:
        """Return ``components`` with a single-pool fallback synthesised."""
        if self.components is not None:
            return list(self.components)
        return [
            PlanComponent(index=i, pool=name, nodes=1)
            for i, name in enumerate(self.pool_names)
        ]


def write_plan(path: "str | Path", plan: Plan) -> None:
    """Write ``plan`` to ``path`` with pretty-printed JSON.

    No atomic tmp+rename here — the plan is written once at submission time
    and is not expected to race with readers. The registry is where
    concurrency matters (see :mod:`slurm.parallel.registry`).
    """
    Path(path).write_text(plan.to_json())


def read_plan(path: "str | Path") -> Plan:
    """Read and parse a :class:`Plan` from ``path``."""
    return Plan.from_json(Path(path).read_text())


__all__ = ["Plan", "PlanComponent", "PlanPeer", "read_plan", "write_plan"]


# Keep default_factory import so ruff doesn't flag field as unused; reserved
# for phases that add optional list/dict fields.
_ = field
