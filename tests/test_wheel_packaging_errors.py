import os
from pathlib import Path
from unittest.mock import patch

import pytest

from slurm.packaging.wheel import WheelPackagingStrategy
from slurm.errors import PackagingError


class DummyCluster:
    def __init__(self, backend):
        self.backend = backend


def test_prepare_missing_pyproject_raises_packaging_error(tmp_path):
    # No pyproject here
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        strat = WheelPackagingStrategy({"build_tool": "uv"})
        with pytest.raises(PackagingError):
            strat.prepare(task=lambda: None, cluster=DummyCluster(backend=object()))
    finally:
        os.chdir(cwd)


@patch("slurm.packaging.wheel.subprocess.run")
def test_prepare_missing_tools_raises_packaging_error(mock_run, tmp_path):
    # Both uv and pip unavailable
    def side_effect(cmd, check=True, capture_output=True, text=True):
        raise FileNotFoundError("tool not found")

    mock_run.side_effect = side_effect

    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "pyproject.toml").write_text("[build-system]\nrequires=['setuptools']\n")

    cwd = os.getcwd()
    os.chdir(proj)
    try:
        strat = WheelPackagingStrategy({"build_tool": "uv"})
        with pytest.raises(PackagingError):
            strat.prepare(task=lambda: None, cluster=DummyCluster(backend=object()))
    finally:
        os.chdir(cwd)


def test_prepare_upload_failure_raises_packaging_error(tmp_path, monkeypatch):
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "pyproject.toml").write_text("[build-system]\nrequires=['setuptools']\n")

    # Create a fake wheel file path to return from build
    dist = tmp_path / "dist"
    dist.mkdir()
    fake_wheel = dist / "pkg-0.0.1-py3-none-any.whl"
    fake_wheel.write_text("wheel")

    # Patch build to return our fake wheel path
    strat = WheelPackagingStrategy({"build_tool": "uv"})
    monkeypatch.setattr(strat, "_find_project_root", lambda: Path(proj))
    monkeypatch.setattr(
        strat, "_build_with_uv", lambda project_root, output_dir: str(fake_wheel)
    )

    # Patch SSHCommandBackend reference inside wheel module to our dummy
    class DummySSH:
        def get_remote_upload_base_path(self):
            return str(tmp_path / "remote")

        def upload_file(self, local_path: str, remote_path: str):
            raise RuntimeError("upload failed")

    from slurm.packaging import wheel as wheel_mod

    # Use dummy backend instance and ensure isinstance check passes by monkeypatching type
    monkeypatch.setattr(wheel_mod, "SSHCommandBackend", DummySSH)
    backend = DummySSH()

    with pytest.raises(PackagingError):
        strat.prepare(task=lambda: None, cluster=DummyCluster(backend=backend))
