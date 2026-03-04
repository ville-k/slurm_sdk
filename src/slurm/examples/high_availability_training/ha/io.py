from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, MutableMapping


def now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def unix_time() -> float:
    """Return the current UNIX time (seconds since epoch)."""
    return time.time()


def read_json(path: Path) -> MutableMapping[str, Any]:
    """Read a JSON file as a mutable mapping."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Write JSON to `path` using atomic replace (temp file + os.replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp_path, path)


def append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    """Append a single JSON record to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, sort_keys=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(line + "\n")


__all__ = [
    "append_jsonl",
    "now_iso",
    "read_json",
    "unix_time",
    "write_json_atomic",
]
