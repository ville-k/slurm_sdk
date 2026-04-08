"""Container image digest resolution via registry v2 HTTP API.

Resolves image tags to immutable SHA256 digests without pulling images.
Used by ContainerPackagingStrategy when use_digest=True (the default).
"""

import json
import logging
from pathlib import Path
from typing import Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# Docker Hub defaults
_DOCKER_HUB_REGISTRY = "registry-1.docker.io"
_DOCKER_HUB_AUTH_URL = "https://auth.docker.io/token"
_DOCKER_HUB_SERVICE = "registry.docker.io"

# Manifest content types for HEAD request
_MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
    ]
)


def resolve_digest(image_ref: str, *, tls_verify: bool = True) -> Optional[str]:
    """Resolve an image tag to its content-addressable SHA256 digest.

    Uses the registry v2 API (a single HTTP HEAD request) instead of
    pulling the entire image. Returns a digest reference like
    ``registry/repo@sha256:abc123...`` or ``None`` if resolution fails.

    Args:
        image_ref: Image reference like "python:3.11", "nvcr.io/nvidia/pytorch:24.01",
            or "registry.example.com:5000/app:v1".
        tls_verify: Whether to verify TLS certificates for the registry.

    Returns:
        Digest reference string, or None if resolution fails for any reason.
    """
    try:
        registry, repository, tag = _parse_image_ref(image_ref)
        auth_header = _get_auth_header(registry, repository)
        digest = _head_manifest(registry, repository, tag, auth_header, tls_verify)
        if digest:
            return f"{registry}/{repository}@{digest}"
        return None
    except Exception as exc:
        logger.debug("Registry digest resolution failed for %s: %s", image_ref, exc)
        return None


def _parse_image_ref(image_ref: str) -> Tuple[str, str, str]:
    """Parse an image reference into (registry, repository, tag/digest).

    Handles Docker Hub shorthand, custom registries, and digest references.
    """
    # Already a digest reference — extract registry/repo and return as-is
    if "@sha256:" in image_ref:
        ref_without_digest, digest = image_ref.split("@", 1)
        parts = ref_without_digest.split("/")
        if len(parts) >= 2 and ("." in parts[0] or ":" in parts[0]):
            registry = parts[0]
            repository = "/".join(parts[1:])
        elif len(parts) == 1:
            registry = _DOCKER_HUB_REGISTRY
            repository = f"library/{parts[0]}"
        else:
            registry = _DOCKER_HUB_REGISTRY
            repository = "/".join(parts)
        return registry, repository, digest

    parts = image_ref.split("/")

    if len(parts) >= 2 and ("." in parts[0] or ":" in parts[0]):
        # First component looks like a registry (contains '.' or ':')
        registry = parts[0]
        repo_and_tag = "/".join(parts[1:])
    else:
        # Docker Hub
        registry = _DOCKER_HUB_REGISTRY
        repo_and_tag = "/".join(parts)

    # Split repo:tag on last ':'
    if ":" in repo_and_tag:
        repository, tag = repo_and_tag.rsplit(":", 1)
    else:
        repository = repo_and_tag
        tag = "latest"

    # Docker Hub single-name images get library/ prefix
    if registry == _DOCKER_HUB_REGISTRY and "/" not in repository:
        repository = f"library/{repository}"

    return registry, repository, tag


def _get_auth_header(registry: str, repository: str) -> Optional[str]:
    """Get an authorization header for the given registry.

    Checks ~/.docker/config.json first, then falls back to anonymous
    token auth for Docker Hub.
    """
    stored_auth = _read_docker_config_auth(registry)
    if stored_auth:
        return stored_auth

    # Docker Hub anonymous token auth
    if registry == _DOCKER_HUB_REGISTRY:
        try:
            resp = requests.get(
                _DOCKER_HUB_AUTH_URL,
                params={
                    "service": _DOCKER_HUB_SERVICE,
                    "scope": f"repository:{repository}:pull",
                },
                timeout=10,
            )
            if resp.status_code == 200:
                token = resp.json().get("token")
                if token:
                    return f"Bearer {token}"
        except Exception as exc:
            logger.debug("Docker Hub token auth failed: %s", exc)

    return None


def _read_docker_config_auth(registry: str) -> Optional[str]:
    """Read credentials from ~/.docker/config.json for the given registry.

    Returns a 'Basic ...' authorization header value, or None.
    Does not handle credsStore or credHelpers (out of scope for v1).
    """
    config_path = Path.home() / ".docker" / "config.json"  # nosec B108 - standard Docker config location
    if not config_path.exists():
        return None

    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.debug("Could not read Docker config: %s", exc)
        return None

    auths = config.get("auths", {})

    # Determine which keys to try for this registry
    keys_to_try = [registry]
    if registry == _DOCKER_HUB_REGISTRY:
        keys_to_try.extend(["https://index.docker.io/v1/", "registry-1.docker.io"])

    for key in keys_to_try:
        entry = auths.get(key)
        if entry and "auth" in entry:
            return f"Basic {entry['auth']}"

    return None


def _head_manifest(
    registry: str,
    repository: str,
    tag: str,
    auth_header: Optional[str],
    tls_verify: bool,
) -> Optional[str]:
    """Send a HEAD request for the manifest and return the digest."""
    url = f"https://{registry}/v2/{repository}/manifests/{tag}"
    headers: dict[str, str] = {"Accept": _MANIFEST_ACCEPT}
    if auth_header:
        headers["Authorization"] = auth_header

    resp = requests.head(url, headers=headers, timeout=10, verify=tls_verify)

    # If 401 on Docker Hub and we didn't already have auth, try token flow
    if resp.status_code == 401 and registry == _DOCKER_HUB_REGISTRY and not auth_header:
        token_header = _get_auth_header(registry, repository)
        if token_header:
            headers["Authorization"] = token_header
            resp = requests.head(url, headers=headers, timeout=10, verify=tls_verify)

    if resp.status_code == 200:
        digest = resp.headers.get("Docker-Content-Digest")
        if digest:
            return digest

    logger.debug("Registry HEAD %s returned status %s", url, resp.status_code)
    return None
