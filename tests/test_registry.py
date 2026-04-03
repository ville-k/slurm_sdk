"""Tests for container registry digest resolution."""

import json

from unittest.mock import MagicMock, patch

from slurm.packaging._registry import (
    resolve_digest,
    _parse_image_ref,
    _read_docker_config_auth,
)


class TestParseImageRef:
    def test_dockerhub_official(self):
        registry, repo, tag = _parse_image_ref("python:3.11")
        assert registry == "registry-1.docker.io"
        assert repo == "library/python"
        assert tag == "3.11"

    def test_dockerhub_with_org(self):
        registry, repo, tag = _parse_image_ref("nvidia/cuda:12.0")
        assert registry == "registry-1.docker.io"
        assert repo == "nvidia/cuda"
        assert tag == "12.0"

    def test_custom_registry(self):
        registry, repo, tag = _parse_image_ref("registry.example.com/team/app:v1")
        assert registry == "registry.example.com"
        assert repo == "team/app"
        assert tag == "v1"

    def test_registry_with_port(self):
        registry, repo, tag = _parse_image_ref("registry.example.com:5000/app:v1")
        assert registry == "registry.example.com:5000"
        assert repo == "app"
        assert tag == "v1"

    def test_no_tag_defaults_to_latest(self):
        registry, repo, tag = _parse_image_ref("python")
        assert tag == "latest"

    def test_nested_repo_path(self):
        registry, repo, tag = _parse_image_ref("ghcr.io/org/repo/sub:v2")
        assert registry == "ghcr.io"
        assert repo == "org/repo/sub"
        assert tag == "v2"

    def test_digest_reference(self):
        ref = "registry.example.com/team/app@sha256:abc123"
        registry, repo, tag = _parse_image_ref(ref)
        assert registry == "registry.example.com"
        assert repo == "team/app"
        assert tag == "sha256:abc123"


class TestResolveDigest:
    def test_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Docker-Content-Digest": "sha256:abc123def456"}

        with patch("slurm.packaging._registry.requests") as mock_requests:
            mock_requests.head.return_value = mock_response
            # Also mock the auth token request for Docker Hub
            token_response = MagicMock()
            token_response.status_code = 200
            token_response.json.return_value = {"token": "test-token"}
            mock_requests.get.return_value = token_response

            result = resolve_digest("python:3.11")

        assert result is not None
        assert "sha256:abc123def456" in result

    def test_returns_none_on_connection_error(self):
        with patch("slurm.packaging._registry.requests") as mock_requests:
            mock_requests.head.side_effect = Exception("Connection refused")
            mock_requests.get.side_effect = Exception("Connection refused")

            result = resolve_digest("python:3.11")

        assert result is None

    def test_returns_none_on_404(self):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.headers = {}

        with patch("slurm.packaging._registry.requests") as mock_requests:
            mock_requests.head.return_value = mock_response
            token_response = MagicMock()
            token_response.status_code = 200
            token_response.json.return_value = {"token": "test-token"}
            mock_requests.get.return_value = token_response

            result = resolve_digest("nonexistent:latest")

        assert result is None

    def test_tls_verify_false(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Docker-Content-Digest": "sha256:abc123"}

        with patch("slurm.packaging._registry.requests") as mock_requests:
            mock_requests.head.return_value = mock_response
            token_response = MagicMock()
            token_response.status_code = 200
            token_response.json.return_value = {"token": "t"}
            mock_requests.get.return_value = token_response

            resolve_digest("python:3.11", tls_verify=False)

        head_call = mock_requests.head.call_args
        assert head_call.kwargs.get("verify") is False


class TestReadDockerConfigAuth:
    def test_reads_auth_from_config(self, tmp_path):
        config = {
            "auths": {
                "registry.example.com": {
                    "auth": "dXNlcjpwYXNz"  # base64("user:pass")
                }
            }
        }
        config_dir = tmp_path / ".docker"
        config_dir.mkdir()
        (config_dir / "config.json").write_text(json.dumps(config))

        with patch("slurm.packaging._registry.Path.home", return_value=tmp_path):
            result = _read_docker_config_auth("registry.example.com")

        assert result is not None
        assert "dXNlcjpwYXNz" in result

    def test_returns_none_when_no_config(self, tmp_path):
        with patch("slurm.packaging._registry.Path.home", return_value=tmp_path):
            result = _read_docker_config_auth("registry.example.com")

        assert result is None

    def test_reads_docker_hub_with_index_key(self, tmp_path):
        config = {
            "auths": {
                "https://index.docker.io/v1/": {
                    "auth": "aHViOnBhc3M="  # base64("hub:pass")
                }
            }
        }
        config_dir = tmp_path / ".docker"
        config_dir.mkdir()
        (config_dir / "config.json").write_text(json.dumps(config))

        with patch("slurm.packaging._registry.Path.home", return_value=tmp_path):
            result = _read_docker_config_auth("registry-1.docker.io")

        assert result is not None
        assert "aHViOnBhc3M=" in result
