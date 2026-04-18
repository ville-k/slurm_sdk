"""Defensive cycle-detection tests for the placement resolver.

Phase 1's validator rejects ``colocate_with`` cycles at spec-build time.
These tests make sure the resolver's runtime fallback fires rather than
infinite-looping if a malformed spec ever reaches :func:`resolve_placement`
(e.g. via a direct ``_ParallelSpec`` construction that skipped validation).
"""

from __future__ import annotations

import pytest

from slurm import task
from slurm.errors import TopologyError
from slurm.parallel.placement import resolve_placement
from slurm.parallel.types import Peer, Pool, Topology, _ParallelSpec


@task(time="00:05:00")
def _noop() -> None:
    return None


@task(time="00:05:00")
def _noop2() -> None:
    return None


def test_resolver_detects_direct_cycle():
    pool = Pool(nodes=2)
    # A → B, B → A. Validator would catch this, but bypass it so the
    # resolver's defensive check fires.
    a = Peer(task=_noop, name="a", colocate_with="b")
    b = Peer(task=_noop2, name="b", colocate_with="a")
    spec = _ParallelSpec(
        peers=(a, b),
        topology=Topology(pools={"p": pool}, default_pool="p"),
    )
    # We still need to set peer.pool on each — _ParallelSpec doesn't do
    # that for us. Use _replace-style rebuild.
    a2 = Peer(task=_noop, name="a", pool="p", colocate_with="b")
    b2 = Peer(task=_noop2, name="b", pool="p", colocate_with="a")
    spec = _ParallelSpec(
        peers=(a2, b2),
        topology=Topology(pools={"p": pool}, default_pool="p"),
    )
    with pytest.raises(TopologyError, match="cycle"):
        resolve_placement(spec, {"p": ("h1", "h2")})


def test_resolver_does_not_hang_on_self_cycle():
    # Peer referring to itself is the degenerate cycle case.
    pool = Pool(nodes=1)
    a = Peer(task=_noop, name="a", pool="p", colocate_with="a")
    spec = _ParallelSpec(
        peers=(a,),
        topology=Topology(pools={"p": pool}, default_pool="p"),
    )
    with pytest.raises(TopologyError, match="cycle"):
        resolve_placement(spec, {"p": ("h1",)})
