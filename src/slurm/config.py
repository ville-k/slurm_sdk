"""Utilities for loading and resolving project Slurmfile configuration.

Note: This module is an internal/private API. The Slurmfile configuration
mechanism is considered implementation detail and may be removed or changed
in future releases. It is not part of the public API and should not be
imported directly by users.

See slurmfile_remaining_usage.md for documentation of Slurmfile usage.
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:  # pragma: no cover - import guard
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - python <3.11 fallback
    try:
        import tomli as tomllib  # type: ignore[assignment]
    except ModuleNotFoundError as exc:  # pragma: no cover - surface helpful error
        raise ImportError(
            "Parsing Slurmfile requires 'tomllib' (Python 3.11+) or 'tomli'."
        ) from exc

TOMLDecodeError = getattr(tomllib, "TOMLDecodeError", ValueError)

from .callbacks.callbacks import BaseCallback
from .errors import (
    SlurmfileEnvironmentNotFoundError,
    SlurmfileInvalidError,
    SlurmfileNotFoundError,
)


SLURM_ENV_VAR = "SLURM_ENV"
SLURMFILE_ENV_VAR = "SLURMFILE"
DEFAULT_SLURMFILE_NAMES = (
    "Slurmfile",
    "Slurmfile.toml",
    "slurmfile",
    "slurmfile.toml",
)


@dataclass
class SlurmfileEnvironment:
    """Resolved configuration for a specific Slurmfile environment."""

    name: str
    path: Path
    config: Dict[str, Any]
    callbacks: List[BaseCallback]


PathLike = Union[str, os.PathLike[str]]


def load_environment(
    slurmfile: Optional[PathLike] = None,
    *,
    env: Optional[str] = None,
    start_dir: Optional[PathLike] = None,
) -> SlurmfileEnvironment:
    """Load a Slurmfile environment, merging defaults and instantiating callbacks."""

    resolved_path = resolve_slurmfile_path(slurmfile, start_dir=start_dir)
    raw_data = _read_toml(resolved_path)
    root_table = _extract_root_table(raw_data)
    env_table = _extract_environment_table(root_table)

    env_name = (env or os.getenv(SLURM_ENV_VAR) or "default").strip() or "default"
    resolved_config = _resolve_environment_config(root_table, env_table, env_name)

    callbacks_config = resolved_config.pop("callbacks", None)
    callbacks = _build_callbacks(callbacks_config)

    return SlurmfileEnvironment(
        name=env_name,
        path=resolved_path,
        config=resolved_config,
        callbacks=callbacks,
    )


def resolve_slurmfile_path(
    slurmfile: Optional[PathLike] = None,
    *,
    start_dir: Optional[PathLike] = None,
) -> Path:
    """Determine which Slurmfile to use, respecting explicit hints and discovery."""

    if slurmfile is not None:
        return _normalize_slurmfile_path(Path(slurmfile))

    env_path = os.getenv(SLURMFILE_ENV_VAR)
    if env_path:
        return _normalize_slurmfile_path(Path(env_path))

    return discover_slurmfile(start_dir=start_dir)


def discover_slurmfile(start_dir: Optional[PathLike] = None) -> Path:
    """Search upwards from ``start_dir`` (or ``cwd``) for a Slurmfile."""

    start_candidate = Path(start_dir) if start_dir is not None else Path.cwd()
    start_candidate = start_candidate.expanduser()
    try:
        start_candidate = start_candidate.resolve()
    except FileNotFoundError:
        start_candidate = start_candidate.absolute()

    for directory in (start_candidate,) + tuple(start_candidate.parents):
        for name in DEFAULT_SLURMFILE_NAMES:
            candidate = directory / name
            if candidate.is_file():
                return candidate

    raise SlurmfileNotFoundError(
        f"No Slurmfile found starting from '{start_candidate}'. Checked {DEFAULT_SLURMFILE_NAMES}."
    )


def _normalize_slurmfile_path(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_dir():
        for name in DEFAULT_SLURMFILE_NAMES:
            candidate = expanded / name
            if candidate.is_file():
                return candidate
        raise SlurmfileNotFoundError(
            f"Slurmfile not found inside directory '{expanded}'. Checked {DEFAULT_SLURMFILE_NAMES}."
        )
    if expanded.is_file():
        return expanded
    raise SlurmfileNotFoundError(f"Slurmfile path '{expanded}' does not exist.")


def _read_toml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except TOMLDecodeError as exc:  # pragma: no cover - toml parser variations
        raise SlurmfileInvalidError(f"Invalid TOML in Slurmfile '{path}'.") from exc

    if not isinstance(data, dict):
        raise SlurmfileInvalidError(
            f"Slurmfile '{path}' must contain a top-level table."
        )
    return data


def _extract_root_table(data: Dict[str, Any]) -> Dict[str, Any]:
    tool_section = data.get("tool")
    if isinstance(tool_section, dict):
        slurm_section = tool_section.get("slurm")
        if isinstance(slurm_section, dict):
            return slurm_section
    return data


def _extract_environment_table(root: Dict[str, Any]) -> Dict[str, Any]:
    environments = root.get("environments")
    if isinstance(environments, dict):
        return environments
    return root


def _resolve_environment_config(
    root_table: Dict[str, Any],
    env_table: Dict[str, Any],
    env_name: str,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    root_default = root_table.get("default")
    if root_default:
        if not isinstance(root_default, dict):
            raise SlurmfileInvalidError("[default] section must be a table.")
        result = _deep_merge(result, root_default)

    env_default = env_table.get("default")
    if env_default and env_default is not root_default:
        if not isinstance(env_default, dict):
            raise SlurmfileInvalidError("[default] environment must be a table.")
        result = _deep_merge(result, env_default)

    if env_name != "default":
        env_config = env_table.get(env_name)
        if env_config is None:
            raise SlurmfileEnvironmentNotFoundError(
                f"Environment '{env_name}' not defined in Slurmfile."
            )
        if not isinstance(env_config, dict):
            raise SlurmfileInvalidError(
                f"Environment '{env_name}' section must be a table."
            )
        result = _deep_merge(result, env_config)

    return result


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build_callbacks(config: Optional[Any]) -> List[BaseCallback]:
    if config is None:
        return []
    if not isinstance(config, list):
        raise SlurmfileInvalidError("callbacks section must be a list.")

    callbacks: List[BaseCallback] = []
    for entry in config:
        callbacks.append(_instantiate_callback(entry))
    return callbacks


def _instantiate_callback(entry: Any) -> BaseCallback:
    if isinstance(entry, str):
        target = entry
        args: Tuple[Any, ...] = ()
        kwargs: Dict[str, Any] = {}
    elif isinstance(entry, dict):
        target = entry.get("target")
        if not isinstance(target, str) or not target:
            raise SlurmfileInvalidError(
                "Callback dictionary must include non-empty 'target'."
            )
        args = entry.get("args", ())
        kwargs = entry.get("kwargs", {})
        if not isinstance(args, (list, tuple)):
            raise SlurmfileInvalidError("Callback 'args' must be a list.")
        if not isinstance(kwargs, dict):
            raise SlurmfileInvalidError("Callback 'kwargs' must be a dictionary.")
    else:
        raise SlurmfileInvalidError("Callback entries must be strings or dictionaries.")

    module_name, attr_name = _split_target(target)
    try:
        module = importlib.import_module(module_name)
        factory = getattr(module, attr_name)
    except (ModuleNotFoundError, AttributeError) as exc:
        raise SlurmfileInvalidError(
            f"Cannot import callback '{target}': {exc}."
        ) from exc

    instance = factory(*args, **kwargs) if args or kwargs else factory()
    if not isinstance(instance, BaseCallback):
        raise SlurmfileInvalidError(
            f"Callback '{target}' did not produce a BaseCallback instance."
        )
    return instance


def _split_target(target: str) -> Tuple[str, str]:
    if ":" in target:
        module_name, attr_name = target.split(":", 1)
    else:
        module_name, _, attr_name = target.rpartition(".")
    if not module_name or not attr_name:
        raise SlurmfileInvalidError(
            f"Invalid callback target '{target}'. Expected 'module:Class'."
        )
    return module_name, attr_name
