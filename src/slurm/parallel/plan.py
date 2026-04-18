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
from typing import List


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

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pool": self.pool,
            "leader": self.leader,
            "on_failure": self.on_failure,
            "max_restarts": self.max_restarts,
            "srun_command_line": self.srun_command_line,
            "callback": self.callback,
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
        )


@dataclass
class Plan:
    """The full supervisor plan for one ``parallel(...)`` allocation.

    Attributes:
        peers: Peer declarations in declaration order.
        grace_period_seconds: SIGTERM-to-SIGKILL window during cascading
            shutdown.
        pool_names: Pool names in hetjob component order. Always length 1 in
            Phase 3; Phase 6 grows it.
        pre_submission_id: SDK base id threaded through for logging.
    """

    peers: List[PlanPeer]
    grace_period_seconds: int
    pool_names: List[str]
    pre_submission_id: str = ""
    schema_version: int = 1

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "peers": [p.to_dict() for p in self.peers],
                "grace_period_seconds": self.grace_period_seconds,
                "pool_names": list(self.pool_names),
                "pre_submission_id": self.pre_submission_id,
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, text: str) -> "Plan":
        data = json.loads(text)
        peers = [PlanPeer.from_dict(p) for p in data.get("peers", [])]
        return cls(
            peers=peers,
            grace_period_seconds=int(data.get("grace_period_seconds", 10)),
            pool_names=list(data.get("pool_names", [])),
            pre_submission_id=str(data.get("pre_submission_id", "")),
            schema_version=int(data.get("schema_version", 1)),
        )

    def peer_by_name(self, name: str) -> PlanPeer:
        for peer in self.peers:
            if peer.name == name:
                return peer
        raise KeyError(name)


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


__all__ = ["Plan", "PlanPeer", "read_plan", "write_plan"]


# Keep default_factory import so ruff doesn't flag field as unused; reserved
# for phases that add optional list/dict fields.
_ = field
