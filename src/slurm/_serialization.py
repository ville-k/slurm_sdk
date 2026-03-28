"""Pickle wrappers with version metadata for cross-version detection.

Every pickle write embeds a one-line JSON header containing the Python
version, pickle protocol, and SDK version.  On read the header is validated
and a clear warning is logged if the writing environment differs from the
reading environment.  Files written by older SDK versions (no header) are
read transparently for backward compatibility.
"""

import json
import logging
import os
import pickle  # nosec B403 - pickle usage is intentional and controlled by SDK
import platform

logger = logging.getLogger(__name__)

_HEADER_PREFIX = b"# slurm-sdk-pickle "
_HEADER_SUFFIX = b"\n"


def _get_sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("slurm-sdk")
    except Exception:
        return "unknown"


def _make_header() -> bytes:
    meta = {
        "python": platform.python_version(),
        "protocol": pickle.HIGHEST_PROTOCOL,
        "sdk": _get_sdk_version(),
    }
    return (
        _HEADER_PREFIX
        + json.dumps(meta, separators=(",", ":")).encode()
        + _HEADER_SUFFIX
    )


def _validate_header(header_line: bytes, source: str = "<unknown>") -> None:
    """Log a warning if the header indicates a different Python version."""
    try:
        payload = header_line[len(_HEADER_PREFIX) :].rstrip()
        meta = json.loads(payload)
        writer_python = meta.get("python", "unknown")
        reader_python = platform.python_version()
        if writer_python != reader_python:
            logger.warning(
                "Pickle version mismatch in %s: written with Python %s (SDK %s), "
                "reading with Python %s (SDK %s). "
                "This may cause deserialization failures.",
                source,
                writer_python,
                meta.get("sdk", "unknown"),
                reader_python,
                _get_sdk_version(),
            )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# File-based API
# ---------------------------------------------------------------------------


def write_pickled(path: str, obj: object) -> None:
    """Write *obj* to *path* as pickle with a version header."""
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    with os.fdopen(fd, "wb") as f:
        f.write(_make_header())
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)


def read_pickled(path: str) -> object:
    """Read a pickled object from *path*, validating the version header if present."""
    with open(path, "rb") as f:
        first_line = f.readline()
        if first_line.startswith(_HEADER_PREFIX):
            _validate_header(first_line, source=path)
            return pickle.load(f)  # nosec B301 - pickle data created by SDK
        # Legacy file without header — rewind and load directly
        f.seek(0)
        return pickle.load(f)  # nosec B301 - pickle data created by SDK


# ---------------------------------------------------------------------------
# Bytes-based API (for base64-encoded transport in rendered scripts)
# ---------------------------------------------------------------------------


def dumps_pickled(obj: object) -> bytes:
    """Pickle *obj* to bytes with a version header prefix."""
    return _make_header() + pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def loads_pickled(data: bytes) -> object:
    """Unpickle from bytes, validating the version header if present."""
    if data.startswith(_HEADER_PREFIX):
        # Find end of header line
        newline_idx = data.index(b"\n", len(_HEADER_PREFIX))
        _validate_header(data[: newline_idx + 1], source="<bytes>")
        return pickle.loads(data[newline_idx + 1 :])  # nosec B301
    return pickle.loads(data)  # nosec B301 - legacy data without header
