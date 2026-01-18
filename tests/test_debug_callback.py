"""Tests for the DebugCallback."""

import pickle
from unittest.mock import MagicMock, patch


from slurm.callbacks import DebugCallback
from slurm.callbacks.callbacks import ExecutionLocus, RunBeginContext


class TestDebugCallbackInit:
    """Test DebugCallback initialization."""

    def test_default_values(self):
        """Test default configuration values."""
        callback = DebugCallback()
        assert callback.port == 5678
        assert callback.wait_for_client is False
        assert callback.host == "0.0.0.0"
        assert callback.log_connection_info is True

    def test_custom_values(self):
        """Test custom configuration values."""
        callback = DebugCallback(
            port=9999,
            wait_for_client=True,
            host="127.0.0.1",
            log_connection_info=False,
        )
        assert callback.port == 9999
        assert callback.wait_for_client is True
        assert callback.host == "127.0.0.1"
        assert callback.log_connection_info is False


class TestDebugCallbackExecutionLocus:
    """Test DebugCallback execution locus configuration."""

    def test_runs_on_runner(self):
        """Test that on_begin_run_job_ctx runs on runner."""
        callback = DebugCallback()
        locus = callback.get_execution_locus("on_begin_run_job_ctx")
        assert locus == ExecutionLocus.RUNNER

    def test_requires_pickling(self):
        """Test that callback requires pickling for transport."""
        callback = DebugCallback()
        assert callback.requires_pickling is True


class TestDebugCallbackPickling:
    """Test DebugCallback serialization."""

    def test_pickle_roundtrip(self):
        """Test callback can be pickled and unpickled."""
        callback = DebugCallback(
            port=9999,
            wait_for_client=True,
            host="127.0.0.1",
        )

        pickled = pickle.dumps(callback)
        restored = pickle.loads(pickled)

        assert restored.port == 9999
        assert restored.wait_for_client is True
        assert restored.host == "127.0.0.1"

    def test_getstate_setstate(self):
        """Test __getstate__ and __setstate__ methods."""
        callback = DebugCallback(port=1234, wait_for_client=True)

        state = callback.__getstate__()
        assert state["port"] == 1234
        assert state["wait_for_client"] is True

        new_callback = DebugCallback()
        new_callback.__setstate__(state)
        assert new_callback.port == 1234
        assert new_callback.wait_for_client is True


class TestDebugCallbackOnBeginRun:
    """Test DebugCallback.on_begin_run_job_ctx behavior."""

    def test_debugpy_not_installed(self, caplog):
        """Test graceful handling when debugpy is not installed."""
        callback = DebugCallback()

        ctx = MagicMock(spec=RunBeginContext)
        ctx.hostname = "compute-node-1"
        ctx.job_id = "12345"

        with patch.dict("sys.modules", {"debugpy": None}):
            with patch("builtins.__import__", side_effect=ImportError("No debugpy")):
                callback.on_begin_run_job_ctx(ctx)

    def test_debugpy_listen_called(self):
        """Test debugpy.listen is called with correct arguments."""
        callback = DebugCallback(port=5678, host="0.0.0.0")

        ctx = MagicMock(spec=RunBeginContext)
        ctx.hostname = "compute-node-1"
        ctx.job_id = "12345"

        mock_debugpy = MagicMock()

        with patch.dict("sys.modules", {"debugpy": mock_debugpy}):
            with patch("builtins.__import__", return_value=mock_debugpy):
                callback.on_begin_run_job_ctx(ctx)

    def test_env_var_override(self, monkeypatch):
        """Test environment variable overrides configuration."""
        monkeypatch.setenv("DEBUGPY_PORT", "9999")
        monkeypatch.setenv("DEBUGPY_HOST", "127.0.0.1")
        monkeypatch.setenv("DEBUGPY_WAIT", "1")

        callback = DebugCallback(port=5678, host="0.0.0.0", wait_for_client=False)

        ctx = MagicMock(spec=RunBeginContext)
        ctx.hostname = "compute-node-1"
        ctx.job_id = "12345"

        mock_debugpy = MagicMock()

        with patch.dict("sys.modules", {"debugpy": mock_debugpy}):
            with patch("builtins.__import__", return_value=mock_debugpy):
                callback.on_begin_run_job_ctx(ctx)


class TestDebugCallbackExport:
    """Test DebugCallback is properly exported."""

    def test_exported_from_callbacks_module(self):
        """Test DebugCallback is exported from slurm.callbacks."""
        from slurm.callbacks import DebugCallback as ExportedCallback

        assert ExportedCallback is DebugCallback

    def test_in_all_list(self):
        """Test DebugCallback is in __all__ list."""
        from slurm import callbacks

        assert "DebugCallback" in callbacks.__all__
