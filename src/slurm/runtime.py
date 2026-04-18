"""Utilities for injecting runtime metadata into Slurm tasks."""

from __future__ import annotations

import inspect
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    get_args,
    get_origin,
)

if TYPE_CHECKING:
    from .parallel.peer_info import PeerGroup

_DEFAULT_MASTER_PORT = 29500

logger = logging.getLogger("slurm.runtime")

# Environment variable carrying the registry path through to user peer
# code. Set by the supervisor when launching peer ``Popen``s; absent for
# local-mode / non-parallel runs, in which case ``ctx.peers`` is an
# empty read-only mapping and ``ctx.announce()`` is a no-op.
_REGISTRY_PATH_ENV = "SLURM_SDK_REGISTRY_PATH"


@dataclass(frozen=True)
class JobContext:
    """Runtime metadata exposed to task functions.

    The context captures enough information to bootstrap distributed launchers
    such as ``torchrun`` without requiring callers to parse the raw ``SLURM_*``
    environment. Values are populated directly from the job environment so the
    object can be reconstructed inside containers that do not ship ``scontrol``.

    Examples:
        Type annotation injection (recommended):

            >>> @task(time="01:00:00")
            ... def train(data: str, ctx: JobContext) -> dict:
            ...     print(f"Job {ctx.job_id}, rank {ctx.rank}/{ctx.world_size}")
            ...     env = ctx.torch_distributed_env()
            ...     return {"rank": ctx.rank}

        Parameter name injection (alternative):

            >>> @task(time="01:00:00")
            ... def train(data: str, job=None):
            ...     # 'job' parameter is auto-populated with JobContext
            ...     print(f"Running on node {job.node_rank}")
    """

    job_id: Optional[str]
    step_id: Optional[str]
    node_rank: Optional[int]
    rank: Optional[int]
    local_rank: Optional[int]
    world_size: Optional[int]
    num_nodes: Optional[int]
    local_world_size: Optional[int]
    gpus_per_node: Optional[int]
    hostnames: Tuple[str, ...] = field(default_factory=tuple)
    master_addr: Optional[str] = None
    master_port: int = _DEFAULT_MASTER_PORT
    environment: Dict[str, str] = field(default_factory=dict)
    created_at: str = field(
        default_factory=lambda: (
            datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"
        )
    )
    output_dir: Optional[Path] = None

    # Topology identity — populated for peers launched via ``parallel(...)``.
    # Further discovery surfaces (``nodes``, ``shared_dir``,
    # ``shutdown_requested``) arrive in later phases; ``peers`` and
    # ``announce()`` are wired in Phase 7.
    peer_name: Optional[str] = None
    peer_pool: Optional[str] = None
    # Replica identity — populated only when the peer is a replica set
    # (``Peer.replicas(...)``). For singleton peers both fields are ``None``.
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    # Path to the parallel allocation's ``registry.json``. Populated from
    # ``SLURM_SDK_REGISTRY_PATH`` when the peer runs under the supervisor;
    # ``None`` for non-parallel runs.
    _registry_path: Optional[Path] = None

    @property
    def peers(self) -> Mapping[str, "PeerGroup"]:
        """Read-only view of every peer in the parallel allocation.

        Returns a mapping keyed by peer name. Each value is a
        :class:`~slurm.parallel.peer_info.PeerGroup` exposing the peer's
        replicas, their hostnames, declared ports, announced metadata,
        and lifecycle state.

        Always returns a real :class:`Mapping` — outside a parallel
        allocation (no registry path set) the mapping is empty instead of
        ``None`` so downstream code doesn't need to branch.

        The mapping is re-read from disk on every access; callers wanting
        a stable snapshot across multiple reads should hold the result of
        a single ``ctx.peers`` call.
        """
        if self._registry_path is None:
            return MappingProxyType({})
        from .parallel.registry import load_peer_groups

        return load_peer_groups(self._registry_path)

    def announce(self, *, ready: bool = False, **fields: Any) -> None:
        """Publish runtime metadata to the peer registry.

        Merges ``fields`` into this peer's ``metadata`` dict so every other
        peer in the allocation can read them via
        ``ctx.peers["<name>"][i].metadata``. Writes are atomic
        (``tmp + rename``); concurrent writers never produce torn files.

        The dedicated ``ready=True`` signal flips the peer's ``state`` to
        ``"ready"`` — the only way user code is allowed to influence state,
        since all other state transitions are the supervisor's province.

        Args:
            ready: When ``True``, set ``state="ready"`` on this replica's
                registry entry in addition to merging ``fields``. Peers
                typically call ``ctx.announce(ready=True, ...)`` once the
                service they run is actually accepting connections.
            **fields: Arbitrary user-defined key/value pairs. Reserved
                keys (those owned by the registry schema) are rejected —
                see :data:`slurm.parallel.registry._RESERVED_ANNOUNCE_KEYS`.

        Raises:
            ValueError: If any key in ``fields`` is reserved.
            RuntimeError: If this context has no peer identity
                (``ctx.peer_name`` is ``None``). Announce is a parallel-
                allocation operation; calling it from a non-parallel task
                is a programmer error rather than a silent no-op.

        Note:
            If the supervisor isn't managing this process (no registry
            path in the environment), the call is a no-op with a DEBUG
            log. This keeps ``ctx.announce()`` safe to sprinkle through
            code that also runs under ``cluster.run_local()``.
        """
        if self.peer_name is None:
            raise RuntimeError(
                "ctx.announce() requires a peer identity; this JobContext "
                "has peer_name=None. announce() is only meaningful for "
                "tasks launched via parallel(...)."
            )
        if self._registry_path is None:
            logger.debug(
                "announce(): no registry path — skipping (peer=%s, fields=%s, ready=%s)",
                self.peer_name,
                sorted(fields.keys()),
                ready,
            )
            return

        from .parallel.registry import announce_peer_metadata

        replica = self.replica_index or 0
        announce_peer_metadata(
            self._registry_path,
            self.peer_name,
            replica,
            fields=fields,
            ready=ready,
        )

    def torch_distributed_env(
        self, *, master_port: Optional[int] = None
    ) -> Dict[str, str]:
        """Return environment variables suitable for ``torchrun``/DDP workers."""

        port = master_port or self.master_port or _DEFAULT_MASTER_PORT
        env: Dict[str, str] = {
            "MASTER_ADDR": self.master_addr or "",
            "MASTER_PORT": str(port),
        }
        if self.world_size is not None:
            env["WORLD_SIZE"] = str(self.world_size)
        if self.rank is not None:
            env["RANK"] = str(self.rank)
        if self.local_rank is not None:
            env["LOCAL_RANK"] = str(self.local_rank)
        if self.local_world_size is not None:
            env["LOCAL_WORLD_SIZE"] = str(self.local_world_size)
        if self.node_rank is not None:
            env["NODE_RANK"] = str(self.node_rank)
        # Remove empty values to avoid overriding upstream configuration with blanks
        return {key: value for key, value in env.items() if value}


_CONTEXT: Optional[JobContext] = None
_CONTEXT_LOCK = threading.Lock()


def current_job_context() -> JobContext:
    """Return the cached :class:`JobContext`, building it from the environment."""

    global _CONTEXT
    with _CONTEXT_LOCK:
        if _CONTEXT is None:
            _CONTEXT = build_job_context()
        return _CONTEXT


def build_job_context(env: Optional[Dict[str, str]] = None) -> JobContext:
    """Create a :class:`JobContext` from the provided (or current) environment."""

    env_map = env or os.environ

    nodelist = (
        env_map.get("SLURM_STEP_NODELIST")
        or env_map.get("SLURM_NODELIST")
        or env_map.get("SLURM_JOB_NODELIST")
        or ""
    )
    hostnames = _expand_nodelist(nodelist)

    job_id = env_map.get("SLURM_JOB_ID")
    step_id = env_map.get("SLURM_STEP_ID")
    node_rank = _parse_int(env_map.get("SLURM_NODEID"))
    rank = _parse_int(env_map.get("SLURM_PROCID"))
    local_rank = _parse_int(env_map.get("SLURM_LOCALID"))
    world_size = _parse_int(env_map.get("SLURM_NTASKS")) or _parse_int(
        env_map.get("WORLD_SIZE")
    )
    num_nodes = _parse_int(env_map.get("SLURM_NNODES")) or _parse_int(
        env_map.get("SLURM_JOB_NUM_NODES")
    )
    if num_nodes is None and hostnames:
        num_nodes = len(hostnames)

    local_world_size = _parse_local_world_size(env_map.get("SLURM_NTASKS_PER_NODE"))
    gpus_per_node = _parse_int(env_map.get("SLURM_GPUS_PER_NODE")) or _parse_int(
        env_map.get("SLURM_GPUS_ON_NODE")
    )

    master_addr = (
        env_map.get("MASTER_ADDR")
        or (hostnames[0] if hostnames else None)
        or env_map.get("SLURM_LAUNCH_NODE_IPADDR")
    )
    master_port = _parse_int(env_map.get("MASTER_PORT")) or _parse_int(
        env_map.get("SLURM_SRUN_COMM_PORT")
    )
    if master_port is None:
        master_port = _DEFAULT_MASTER_PORT

    slurm_environment = {k: env_map[k] for k in env_map if k.startswith("SLURM_")}

    # Extract output directory from JOB_DIR environment variable
    output_dir = None
    job_dir_str = env_map.get("JOB_DIR")
    if job_dir_str:
        output_dir = Path(job_dir_str)

    peer_name = env_map.get("SLURM_SDK_PEER_NAME") or None
    peer_pool = env_map.get("SLURM_SDK_PEER_POOL") or None

    # Replica identity: the parallel renderer exports ``SLURM_SDK_REPLICA_COUNT``
    # for replica peers (count > 1). When present, ``SLURM_PROCID`` is the
    # replica index within the step. Singleton peers leave both at ``None`` so
    # user code can branch cleanly between replica and non-replica contexts.
    replica_count_env = _parse_int(env_map.get("SLURM_SDK_REPLICA_COUNT"))
    if replica_count_env is not None and replica_count_env > 0:
        replica_count_val: Optional[int] = replica_count_env
        replica_index_val: Optional[int] = _parse_int(env_map.get("SLURM_PROCID")) or 0
    else:
        replica_count_val = None
        replica_index_val = None

    registry_path_raw = env_map.get(_REGISTRY_PATH_ENV)
    registry_path = Path(registry_path_raw) if registry_path_raw else None

    return JobContext(
        job_id=job_id,
        step_id=step_id,
        node_rank=node_rank,
        rank=rank,
        local_rank=local_rank,
        world_size=world_size,
        num_nodes=num_nodes,
        local_world_size=local_world_size,
        gpus_per_node=gpus_per_node,
        hostnames=hostnames,
        master_addr=master_addr,
        master_port=master_port,
        environment=slurm_environment,
        output_dir=output_dir,
        peer_name=peer_name,
        peer_pool=peer_pool,
        replica_index=replica_index_val,
        replica_count=replica_count_val,
        _registry_path=registry_path,
    )


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_local_world_size(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    raw = str(raw).strip()
    # Handles forms like "4", "4(x2)", "4*2" (seen in some configs)
    match = re.match(r"^(\d+)", raw)
    if match:
        return _parse_int(match.group(1))
    return _parse_int(raw)


def _expand_nodelist(spec: str) -> Tuple[str, ...]:
    if not spec:
        return tuple()

    # Split top-level comma separated segments while respecting brackets
    segments: list[str] = []
    depth = 0
    current: list[str] = []
    for char in spec:
        if char == "," and depth == 0:
            segment = "".join(current).strip()
            if segment:
                segments.append(segment)
            current = []
            continue
        if char == "[":
            depth += 1
        elif char == "]" and depth > 0:
            depth -= 1
        current.append(char)
    if current:
        segment = "".join(current).strip()
        if segment:
            segments.append(segment)

    expanded: list[str] = []
    for segment in segments:
        expanded.extend(_expand_bracket_expression(segment))
    return tuple(expanded)


_RANGE_RE = re.compile(r"^(\d+)-(\d+)(?::(\d+))?$")
_BRACKET_RE = re.compile(r"^(.*)\[([^\[\]]+)\](.*)$")


def _expand_bracket_expression(segment: str) -> Iterable[str]:
    match = _BRACKET_RE.match(segment)
    if not match:
        return [segment]

    prefix, body, suffix = match.groups()
    options: list[str] = []
    for token in body.split(","):
        token = token.strip()
        if not token:
            continue
        range_match = _RANGE_RE.match(token)
        if range_match:
            start_raw, end_raw, step_raw = range_match.groups()
            start = int(start_raw)
            end = int(end_raw)
            step = int(step_raw) if step_raw else 1
            width = max(len(start_raw), len(end_raw))
            for value in range(start, end + 1, step):
                options.append(f"{value:0{width}d}")
        else:
            options.append(token)

    if not options:
        return [segment]

    expanded: list[str] = []
    for option in options:
        expanded.extend(_expand_bracket_expression(f"{prefix}{option}{suffix}"))
    return expanded


def _function_wants_job_context(func: Callable[..., Any]) -> bool:
    """Return ``True`` if ``func`` expects a :class:`JobContext` parameter."""

    candidate = _unwrap_callable(func)
    signature = inspect.signature(candidate)
    for parameter in signature.parameters.values():
        if _parameter_accepts_job_context(parameter):
            return True
    return False


def _bind_job_context(
    func: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    context: JobContext,
) -> Tuple[Tuple[Any, ...], Dict[str, Any], bool]:
    """Inject ``context`` into ``func`` if the signature requests it.

    Returns modified ``(args, kwargs, injected)``.
    """

    candidate = _unwrap_callable(func)
    signature = inspect.signature(candidate)
    parameters = list(signature.parameters.values())
    target_param = None
    for parameter in parameters:
        if _parameter_accepts_job_context(parameter):
            target_param = parameter
            break
    if target_param is None:
        return args, kwargs, False

    name = target_param.name
    if target_param.kind == inspect.Parameter.KEYWORD_ONLY:
        if name in kwargs:
            return args, kwargs, False
        new_kwargs = dict(kwargs)
        new_kwargs[name] = context
        return args, new_kwargs, True

    positional_params = [
        p
        for p in parameters
        if p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if target_param.kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        if name in kwargs:
            return args, kwargs, False
        position = positional_params.index(target_param)
        if position < len(args):
            return args, kwargs, False
        # If there are args provided but they don't reach the context parameter position,
        # we should inject via kwargs to avoid conflicts with keyword arguments
        # that might fill the gap between len(args) and position
        if len(args) > 0 and position > len(args):
            new_kwargs = dict(kwargs)
            new_kwargs[name] = context
            return args, new_kwargs, True
        # Only use positional injection if we're extending args sequentially
        new_args = list(args)
        new_args.append(context)
        return tuple(new_args), kwargs, True

    # We deliberately avoid injecting through *args/**kwargs as it is ambiguous.
    return args, kwargs, False


def _parameter_accepts_job_context(parameter: inspect.Parameter) -> bool:
    if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
        return False
    if parameter.name == "job":
        return True
    annotation = parameter.annotation
    if _annotation_is_job_context(annotation):
        return True
    return False


def _annotation_is_job_context(annotation: Any) -> bool:
    if annotation is inspect._empty:
        return False
    if annotation is JobContext:
        return True
    if isinstance(annotation, str):
        return "JobContext" in annotation

    origin = get_origin(annotation)
    if origin is not None:
        return any(_annotation_is_job_context(arg) for arg in get_args(annotation))

    return (
        getattr(annotation, "__name__", None) == "JobContext"
        or getattr(annotation, "__qualname__", None) == "JobContext"
    )


def _unwrap_callable(func: Callable[..., Any]) -> Callable[..., Any]:
    """Peel back common wrappers (functools, SlurmTask, partial)."""

    seen: set[int] = set()
    candidate: Callable[..., Any] = func

    while True:
        ident = id(candidate)
        if ident in seen:
            break
        seen.add(ident)

        wrapped = getattr(candidate, "__wrapped__", None)
        if callable(wrapped):
            candidate = wrapped  # type: ignore[assignment]
            continue

        if isinstance(candidate, partial):
            candidate = candidate.func  # type: ignore[assignment]
            continue

        inner = getattr(candidate, "func", None)
        if callable(inner):
            candidate = inner  # type: ignore[assignment]
            continue

        break

    return candidate
