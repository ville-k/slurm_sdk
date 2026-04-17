"""Tests for slurm._serialization round-trips and error handling."""

import pickle

import pytest

from slurm._serialization import (
    _HEADER_PREFIX,
    dumps_pickled,
    loads_pickled,
)


def test_roundtrip_simple_object():
    data = dumps_pickled({"answer": 42, "items": [1, 2, 3]})
    assert loads_pickled(data) == {"answer": 42, "items": [1, 2, 3]}


def test_loads_legacy_unheadered_pickle():
    raw = pickle.dumps({"legacy": True})
    assert loads_pickled(raw) == {"legacy": True}


def test_loads_truncated_header_raises_value_error():
    truncated = _HEADER_PREFIX + b'{"python":"3.12"'  # no newline, no payload
    with pytest.raises(ValueError, match="truncated"):
        loads_pickled(truncated)
