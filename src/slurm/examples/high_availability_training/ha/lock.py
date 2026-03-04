from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from slurm.examples.high_availability_training.ha.io import (
    now_iso,
    unix_time,
    write_json_atomic,
)


class LockHeldError(RuntimeError):
    """Raised when another supervisor is actively holding the run lock."""


@dataclass
class SupervisorLockPaths:
    lock_dir: Path

    @property
    def owner_path(self) -> Path:
        return self.lock_dir / "owner.json"

    @property
    def heartbeat_path(self) -> Path:
        return self.lock_dir / "heartbeat.json"


class SupervisorLeaseLock:
    """Lease-based directory lock to enforce a single active supervisor.

    The lock is acquired via atomic mkdir of `lock_dir`. The owner refreshes a
    heartbeat periodically. Another supervisor may "steal" the lock if the
    heartbeat is stale (and/or a cluster check indicates the owner is gone).
    """

    def __init__(
        self,
        lock_dir: Path,
        *,
        ttl_s: float = 120.0,
        heartbeat_interval_s: float = 5.0,
        is_owner_active: Optional[Callable[[Mapping[str, Any]], bool]] = None,
    ) -> None:
        self._paths = SupervisorLockPaths(lock_dir=lock_dir)
        self._ttl_s = float(ttl_s)
        self._heartbeat_interval_s = float(heartbeat_interval_s)
        self._is_owner_active = is_owner_active
        self._last_heartbeat_unix: float = 0.0
        self._acquired = False

    @property
    def acquired(self) -> bool:
        return self._acquired

    def acquire(self, owner: Mapping[str, Any]) -> None:
        """Acquire the lock or raise LockHeldError if an active owner exists."""
        attempts = 0
        while True:
            attempts += 1
            try:
                self._paths.lock_dir.mkdir(parents=True, exist_ok=False)
                self._write_owner(owner)
                self.heartbeat()
                self._acquired = True
                return
            except FileExistsError:
                if self._lock_is_active():
                    raise LockHeldError(
                        f"Another supervisor is active (lock: {self._paths.lock_dir})"
                    )
                self._steal_lock()
                if attempts > 3:
                    raise LockHeldError(
                        "Failed to acquire lock after steal attempts "
                        f"(lock: {self._paths.lock_dir})"
                    )

    def _read_json_if_exists(self, path: Path) -> Optional[Mapping[str, Any]]:
        try:
            if not path.exists():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _lock_is_active(self) -> bool:
        owner = self._read_json_if_exists(self._paths.owner_path) or {}
        heartbeat = self._read_json_if_exists(self._paths.heartbeat_path) or {}

        if self._is_owner_active is not None:
            try:
                if self._is_owner_active(owner):
                    return True
            except Exception:
                # Fall back to heartbeat TTL if cluster checks fail.
                pass

        heartbeat_unix = heartbeat.get("time_unix")
        if isinstance(heartbeat_unix, (int, float)):
            return (unix_time() - float(heartbeat_unix)) < self._ttl_s
        return False

    def _steal_lock(self) -> None:
        suffix = int(time.time())
        stale_dir = self._paths.lock_dir.with_name(
            self._paths.lock_dir.name + f".stale.{suffix}"
        )
        try:
            os.rename(self._paths.lock_dir, stale_dir)
        except FileNotFoundError:
            return
        except OSError:
            # If another process wins the race, we'll retry acquire.
            return

    def _write_owner(self, owner: Mapping[str, Any]) -> None:
        payload = dict(owner)
        payload.setdefault("acquired_at", now_iso())
        write_json_atomic(self._paths.owner_path, payload)

    def heartbeat(self) -> None:
        """Refresh the heartbeat file (rate-limited)."""
        if not self._paths.lock_dir.exists():
            return
        now_unix = unix_time()
        if (now_unix - self._last_heartbeat_unix) < self._heartbeat_interval_s:
            return
        self._last_heartbeat_unix = now_unix
        write_json_atomic(
            self._paths.heartbeat_path,
            {"time_unix": now_unix, "updated_at": now_iso()},
        )

    def release(self) -> None:
        """Release the lock (best-effort)."""
        if not self._acquired:
            return
        try:
            for child in (self._paths.owner_path, self._paths.heartbeat_path):
                try:
                    child.unlink()
                except FileNotFoundError:
                    pass
            self._paths.lock_dir.rmdir()
        except Exception:
            # Leave the lock in place; TTL-based stealing will recover.
            pass
        finally:
            self._acquired = False

    def __enter__(self) -> "SupervisorLeaseLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


__all__ = [
    "LockHeldError",
    "SupervisorLeaseLock",
]
