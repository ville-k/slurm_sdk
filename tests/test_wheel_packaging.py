from unittest.mock import patch, MagicMock
from pathlib import Path
from slurm.packaging.wheel import WheelPackagingStrategy


@patch("slurm.packaging.wheel.subprocess.run")
def test_wheel_build_uses_uv_and_falls_back_to_pip(mock_run, tmp_path):
    # Simulate uv present but returns no wheels -> triggers fallback
    def side_effect(cmd, check=True, capture_output=True, text=True):
        if cmd and cmd[0] == "uv" and cmd[1] == "--version":
            m = MagicMock()
            m.stdout = "uv 0.1"
            return m
        if cmd and cmd[0] == "uv" and cmd[1] == "build":
            raise Exception("uv build failed")
        # pip wheel succeeds
        m = MagicMock()
        return m

    mock_run.side_effect = side_effect

    # Create fake project root with pyproject
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "pyproject.toml").write_text("[build-system]\nrequires=['setuptools']\n")

    # Chdir so strategy can find project root
    import os

    cwd = os.getcwd()
    os.chdir(proj)
    try:
        strat = WheelPackagingStrategy({"build_tool": "uv"})
        # Patch internal pip wheel search by creating a fake wheel
        out_dir = tmp_path / "dist"
        out_dir.mkdir()
        fake_wheel = out_dir / "pkg-0.0.1-py3-none-any.whl"
        fake_wheel.write_text("wheel")

        # Monkeypatch glob lookup by calling private method directly
        # prepare() calls _find_project_root() and build function which we simulate
        # However, prepare() also requires a backend; skip and directly test build method
        # Here, we exercise _build_with_uv -> fallback to pip -> find wheel
        built = strat._build_with_uv(project_root=Path(proj), output_dir=str(out_dir))
        assert built.endswith(".whl")
    finally:
        os.chdir(cwd)
